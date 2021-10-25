/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),
    Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <cassert>
#include <cstdint>
#include "aggregate.h"
#include "config.h"
#include "down.h"
#include "memmgr.h"
#include "ts.h"
#include "tsdb.h"
#include "meter.h"
#include "logger.h"
#include "utils.h"
#include "leak.h"


namespace tt
{


TimeSeries::TimeSeries() :
    m_metric(nullptr),
    m_key(nullptr),
    m_tsdb(nullptr),
    m_buff(nullptr),
    m_ooo_buff(nullptr),
    TagOwner(true)
{
}

void
TimeSeries::init(const char *metric, const char *key, Tag *tags, Tsdb *tsdb, bool read_only)
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
    }

    if (m_key != nullptr)
    {
        FREE(m_key);
    }

    m_metric = STRDUP(metric);
    m_key = STRDUP(key);
    m_tags = tags;
    m_tsdb = tsdb;
    m_pages.clear();
    m_buff = nullptr;
    m_ooo_buff = nullptr;

    ASSERT(m_tsdb != nullptr);
    Logger::debug("ts of %T is loaded in read-only mode", tsdb);
}

TimeSeries::~TimeSeries()
{
    if (m_key != nullptr)
    {
        FREE(m_key);
        m_key = nullptr;
    }

    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }
}

bool
TimeSeries::recycle()
{
    std::lock_guard<std::mutex> guard(m_lock);

    m_tsdb = nullptr;

    for (PageInfo *info: m_pages)
    {
        MemoryManager::free_recyclable(info);
    }
    m_pages.clear();
    m_pages.shrink_to_fit();

    for (PageInfo *info: m_ooo_pages)
    {
        MemoryManager::free_recyclable(info);
    }
    m_ooo_pages.clear();
    m_ooo_pages.shrink_to_fit();

    TagOwner::recycle();

    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }

    if (m_key != nullptr)
    {
        FREE(m_key);
        m_key = nullptr;
    }

    return true;
}

void
TimeSeries::flush(bool close)
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_buff != nullptr) m_buff->flush();
    if (m_ooo_buff != nullptr) m_ooo_buff->flush();
}

PageInfo *
TimeSeries::get_free_page_on_disk(bool is_out_of_order)
{
    PageInfo *info = m_tsdb->get_free_page_on_disk(is_out_of_order);

    if (is_out_of_order)
        m_ooo_pages.push_back(info);
    else
        m_pages.push_back(info);
    m_tsdb->append_meta(this, info);

    return info;
}

void
TimeSeries::add_page_info(PageInfo *page_info)
{
    ASSERT(page_info != nullptr);

    if (page_info->is_out_of_order())
        m_ooo_pages.push_back(page_info);
    else
        m_pages.push_back(page_info);
}

bool
TimeSeries::add_data_point(DataPoint& dp)
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_buff == nullptr)
    {
        if (m_pages.empty())
            m_buff = get_free_page_on_disk(false);
        else
        {
            m_buff = m_pages.back();
            m_buff->ensure_dp_available();
        }
    }

    Timestamp last_tstamp = m_buff->get_last_tstamp();

    if ((dp.get_timestamp() < last_tstamp) && (! m_buff->is_empty()))
    {
        return add_ooo_data_point(dp);
    }

    bool ok = m_buff->add_data_point(dp.get_timestamp(), dp.get_value());

    if (! ok)
    {
        ASSERT(m_buff->is_full());

        m_buff->flush();
        m_buff = get_free_page_on_disk(false);
        ASSERT(m_buff->is_empty());

        // try again
        ok = m_buff->add_data_point(dp.get_timestamp(), dp.get_value());
        ASSERT(ok);
    }

    return ok;
}

bool
TimeSeries::add_batch(DataPointSet& dps)
{
    std::lock_guard<std::mutex> guard(m_lock);
    bool success = true;

    if (m_buff == nullptr)
    {
        if (m_pages.empty())
            m_buff = get_free_page_on_disk(false);
        else
        {
            m_buff = m_pages.back();
            m_buff->ensure_dp_available();
        }
    }

    for (int i = 0; i < dps.get_dp_count(); i++)
    {
        Timestamp tstamp = dps.get_timestamp(i);
        double value = dps.get_value(i);
        Timestamp last_tstamp = m_buff->get_last_tstamp();

        if (tstamp < last_tstamp)
        {
            DataPoint dp(tstamp, value);
            success = add_ooo_data_point(dp) && success;
            continue;
        }

        bool ok = m_buff->add_data_point(tstamp, value);

        if (! ok)
        {
            ASSERT(m_buff->is_full());

            m_buff->flush();
            m_buff = get_free_page_on_disk(false);
            ASSERT(m_buff->is_empty());
            ASSERT(m_buff->get_last_tstamp() == m_tsdb->get_time_range().get_from());

            // try again
            ok = m_buff->add_data_point(tstamp, value);
            ASSERT(ok);
        }

        success = ok && success;
    }

    return success;
}

// Lock is acquired in add_data_point() already!
bool
TimeSeries::add_ooo_data_point(DataPoint& dp)
{
    if (m_ooo_buff == nullptr)
    {
        if (m_ooo_pages.empty())
            m_ooo_buff = get_free_page_on_disk(true);
        else
        {
            m_ooo_buff = m_ooo_pages.back();
            m_ooo_buff->ensure_dp_available();
        }
    }

    bool ok = m_ooo_buff->add_data_point(dp.get_timestamp(), dp.get_value());

    if (! ok)
    {
        ASSERT(m_ooo_buff->is_full());

        m_ooo_buff->flush();
        m_ooo_buff = get_free_page_on_disk(true);
        ASSERT(m_ooo_buff->is_empty());

        // try again
        ok = m_ooo_buff->add_data_point(dp.get_timestamp(), dp.get_value());
        ASSERT(ok);
    }

    return ok;
}

Tag *
TimeSeries::find_tag_by_name(const char *name) const
{
    if ((m_tags == nullptr) || (name == nullptr)) return nullptr;
    return Tag::get_key_value_pair(m_tags, name);
}

void
TimeSeries::query(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
    Meter meter(METRIC_TICKTOCK_QUERY_TS_LATENCY_MS);

    if ((m_ooo_buff != nullptr) || (! m_ooo_pages.empty()))
    {
        query_with_ooo(range, downsampler, dps);
    }
    else
    {
        query_without_ooo(range, downsampler, dps);
    }
}

void
TimeSeries::query_with_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
    using container_it = std::pair<DataPointContainer*,int>;
    auto container_cmp = [](const container_it &lhs, const container_it &rhs)
    {
        if (lhs.first->get_data_point(lhs.second).first > rhs.first->get_data_point(rhs.second).first)
        {
            return true;
        }
        else if (lhs.first->get_data_point(lhs.second).first == rhs.first->get_data_point(rhs.second).first)
        {
            if (lhs.first->is_out_of_order() && ! rhs.first->is_out_of_order())
                return true;
            else if (! lhs.first->is_out_of_order() && rhs.first->is_out_of_order())
                return false;

            if (lhs.first->get_page_index() == 0)
                return true;
            else if (rhs.first->get_page_index() == 0)
                return false;
            else
                return (lhs.first->get_page_index() > rhs.first->get_page_index());
        }
        else
        {
            return false;
        }
    };
    std::priority_queue<container_it, std::vector<container_it>, decltype(container_cmp)> pq(container_cmp);
    size_t size = m_pages.size() + m_ooo_pages.size();
    DataPointContainer containers[size];
    size_t count = 0;
    std::lock_guard<std::mutex> guard(m_lock);

    for (PageInfo *page_info : m_pages)
    {
        ASSERT(! page_info->is_empty());

        if (range.has_intersection(page_info->get_time_range()))
        {
            containers[count].init(page_info);
            pq.emplace(&containers[count], 0);
            count++;
        }
    }

    for (PageInfo *page_info : m_ooo_pages)
    {
        ASSERT(! page_info->is_empty());

        if (range.has_intersection(page_info->get_time_range()))
        {
            containers[count].init(page_info);
            pq.emplace(&containers[count], 0);
            count++;
        }
    }

    while (! pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        DataPointContainer *container = top.first;
        int i = top.second;
        DataPointPair& dp = container->get_data_point(i);

        // TODO: these logic does not belong here.
        if (range.in_range(dp.first))
        {
            // remove duplicates
            if ((!dps.empty()) && (dps.back().first == dp.first))
            {
                dps.back().second = dp.second;
            }
            else if (downsampler == nullptr)
            {
                dps.push_back(dp);
            }
            else
            {
                downsampler->add_data_point(dp, dps);
            }
        }
        else if (range.get_to() < dp.first)
        {
            continue;
        }

        if ((i+1) < container->size())
        {
            pq.emplace(container, i+1);
        }
    }
}

void
TimeSeries::query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
    std::lock_guard<std::mutex> guard(m_lock);

    for (PageInfo *page_info : m_pages)
    {
        this->query_without_ooo(range, downsampler, dps, page_info);
    }

    ASSERT(m_ooo_pages.empty());
    Logger::debug("Found %d data points in ts %T", dps.size(), this);
}

void
TimeSeries::query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps, PageInfo *page_info)
{
    ASSERT(page_info != nullptr);

    if (m_tsdb == nullptr) return;

    if (! range.has_intersection(page_info->get_time_range())) return;

    // make sure data points are loaded and uncompressed
    DataPointContainer container;
    container.init(page_info);

    for (int i = 0; i < container.size(); i++)
    {
        DataPointPair& dp = container.get_data_point(i);
        ASSERT(page_info->get_time_range().in_range(dp.first));

        // TODO: these logic does not belong here.
        if (range.in_range(dp.first))
        {
            if (downsampler == nullptr)
            {
                dps.push_back(dp);
            }
            else
            {
                downsampler->add_data_point(dp, dps);
            }
        }
        else if (range.get_to() < dp.first)
        {
            break;
        }
    }
}

void
TimeSeries::add_data_point_with_dedup(DataPointPair &dp, DataPointPair &prev, PageInfo* &info)
{
    bool success = (info != nullptr);

    if (success)
    {
        if ((prev.first != dp.first) && (prev.first != 0L))
        {
            success = info->add_data_point(prev.first, prev.second);
        }
    }

    if (! success)
    {
        if (info != nullptr) info->persist();
        info = get_free_page_on_disk(false);

        if (prev.first != 0L)
        {
            success = info->add_data_point(prev.first, prev.second);
            ASSERT(success);
        }
    }

    prev.first = dp.first;
    prev.second = dp.second;
}

// Compress all out-of-order pages in m_ooo_pages[], if any.
// Return true if we actually compacted something.
bool
TimeSeries::compact(MetaFile& meta_file)
{
    DataPointVector dps;
    PageInfo *info = nullptr;
    TimeRange range = m_tsdb->get_time_range();
    int id_from, id_to, file_id;

    // get all data points
    query_with_ooo(range, nullptr, dps);

    Logger::trace("ts (%s, %s): Found %d dps to compact", m_metric, m_key, dps.size());

    for (auto& dp : dps)
    {
        ASSERT(range.in_range(dp.first));
        Logger::trace("dp.first=%ld, dp.second=%f", dp.first, dp.second);

        if (info == nullptr)
        {
            info = m_tsdb->get_free_page_for_compaction();
            //meta_file.append(this, info);
            file_id = info->get_file_id();
            id_from = id_to = info->get_id();
        }

        bool ok = info->add_data_point(dp.first, dp.second);

        if (! ok)
        {
            ASSERT(info->is_full());
            info->flush();
            MemoryManager::free_recyclable(info);

            info = m_tsdb->get_free_page_for_compaction();
            //meta_file.append(this, info);
            ok = info->add_data_point(dp.first, dp.second);
            ASSERT(ok);

            if (info->get_file_id() == file_id)
            {
                id_to = info->get_id();
            }
            else
            {
                meta_file.append(this, file_id, id_from, id_to);

                file_id = info->get_file_id();
                id_from = id_to = info->get_id();
            }
        }
    }

    if (info != nullptr)
    {
        info->shrink_to_fit();
        MemoryManager::free_recyclable(info);
        meta_file.append(this, file_id, id_from, id_to);
    }

    return (! dps.empty());
}

void
TimeSeries::get_all_pages(std::vector<PageInfo*>& pages)
{
    for (PageInfo *info: m_ooo_pages)
    {
        pages.push_back(info);
    }

    for (PageInfo *info: m_pages)
    {
        pages.push_back(info);
    }
}

void
TimeSeries::append_meta_all(MetaFile& meta)
{
    for (PageInfo *info: m_pages)
    {
        meta.append(this, info);
    }

    for (PageInfo *info: m_ooo_pages)
    {
        meta.append(this, info);
    }
}

void
TimeSeries::set_check_point()
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_buff != nullptr) m_buff->persist(true);
    if (m_ooo_buff != nullptr) m_ooo_buff->persist(true);
}

const char*
TimeSeries::c_str(char* buff) const
{
    ASSERT(m_key != nullptr);
    ASSERT(m_metric != nullptr);

    std::snprintf(buff, c_size(), "%s %s", m_metric, m_key);
    return buff;
}

int
TimeSeries::get_dp_count()
{
    int count = 0;
    std::lock_guard<std::mutex> guard(m_lock);

    for (PageInfo *info: m_pages)
    {
        count += info->get_dp_count();
    }

    for (PageInfo *info: m_ooo_pages)
    {
        count += info->get_dp_count();
    }

    return count;
}

int
TimeSeries::get_page_count(bool ooo)
{
    int count = 0;
    std::lock_guard<std::mutex> guard(m_lock);

    if (ooo)
    {
        // count out-of-order pages
        count += m_ooo_pages.size();
    }
    else
    {
        // count regular (non-out-of-order) pages
        count += m_pages.size();
    }

    return count;
}


}
