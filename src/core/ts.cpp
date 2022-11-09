/*
    TickTock is an open-source Time Series Database, maintained by
    Yongtao You (yongtao.you@gmail.com) and Yi Lin (ylin30@gmail.com).

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
#include "meter.h"
#include "leak.h"
#include "logger.h"
#include "query.h"
#include "ts.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


std::atomic<TimeSeriesId> TimeSeries::m_next_id{0};


TimeSeries::TimeSeries(const char *metric, const char *key, Tag *tags) :
    TagOwner(true)
{
    TimeSeriesId id = m_next_id.fetch_add(1);
    init(id, metric, key, tags);

    // WARN: If there are derived classes from TimeSeries,
    //       this might be a problem...
    MetaFile::instance()->add_ts(this);
}

// called during restart/restore
TimeSeries::TimeSeries(TimeSeriesId id, const char *metric, const char *key, Tag *tags) :
    TagOwner(true)
{
    init(id, metric, key, tags);

    if (m_next_id.load() <= id)
        m_next_id = id + 1;
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

void
TimeSeries::init(TimeSeriesId id, const char *metric, const char *key, Tag *tags)
{
    m_id = id;
    m_buff = nullptr;
    m_ooo_buff = nullptr;

    m_metric = STRDUP(metric);
    m_key = STRDUP(key);
    m_tags = tags;
}

void
TimeSeries::restore(Tsdb *tsdb, PageSize offset, uint8_t start, char *buff, bool is_ooo)
{
    ASSERT(tsdb != nullptr);

    if (is_ooo)
    {
        ASSERT(m_ooo_buff == nullptr);
        m_ooo_buff = new PageInMemory(m_id, tsdb, true);
        m_ooo_buff->init(m_id, tsdb, true);
    }
    else
    {
        ASSERT(m_buff == nullptr);
        m_buff = new PageInMemory(m_id, tsdb, false);
        m_buff->init(m_id, tsdb, false);
    }
}

void
TimeSeries::flush(bool accessed)
{
    std::lock_guard<std::mutex> guard(m_lock);
    flush_no_lock(accessed);
}

void
TimeSeries::flush_no_lock(bool accessed)
{
    if (m_buff != nullptr)
        m_buff->flush(m_id);

    if (m_ooo_buff != nullptr)
        m_ooo_buff->flush(m_id);
}

void
TimeSeries::append(FILE *file)
{
    ASSERT(file != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);
    if (m_buff != nullptr) m_buff->append(m_id, file);
    if (m_ooo_buff != nullptr) m_ooo_buff->append(m_id, file);
}

bool
TimeSeries::add_data_point(DataPoint& dp)
{
    const Timestamp tstamp = dp.get_timestamp();
    std::lock_guard<std::mutex> guard(m_lock);

    // Make sure we have a valid m_buff (PageInMemory)
    if (UNLIKELY(m_buff == nullptr))
    {
        ASSERT(m_ooo_buff == nullptr);
        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_buff = new PageInMemory(m_id, tsdb, false);
    }
    else if (m_buff->in_range(tstamp) != 0)
    {
        m_buff->flush(m_id);

        // reset the m_buff
        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_buff->init(m_id, tsdb, false);
    }

    ASSERT(m_buff != nullptr);
    ASSERT(m_buff->in_range(tstamp) == 0);

    Timestamp last_tstamp = m_buff->get_last_tstamp();

    if ((tstamp <= last_tstamp) && (! m_buff->is_empty()))
    {
        return add_ooo_data_point(dp);
    }

    bool ok = m_buff->add_data_point(tstamp, dp.get_value());

    if (! ok)
    {
        ASSERT(m_buff->is_full());

        m_buff->flush(m_id);
        m_buff->init(m_id, nullptr, false);

        ASSERT(m_buff->is_empty());

        // try again
        ok = m_buff->add_data_point(tstamp, dp.get_value());
        ASSERT(ok);
    }

    return ok;
}

// Lock is acquired in add_data_point() already!
bool
TimeSeries::add_ooo_data_point(DataPoint& dp)
{
    const Timestamp tstamp = dp.get_timestamp();

    // Make sure we have a valid m_ooo_buff (PageInMemory)
    if (m_ooo_buff == nullptr)
    {
        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_ooo_buff = new PageInMemory(m_id, tsdb, true);
    }
    else if (m_ooo_buff->in_range(tstamp) != 0)
    {
        m_ooo_buff->flush(m_id);

        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_ooo_buff->init(m_id, tsdb, true);
    }

    bool ok = m_ooo_buff->add_data_point(tstamp, dp.get_value());

    if (! ok)
    {
        ASSERT(m_ooo_buff->is_full());

        m_ooo_buff->flush(m_id);
        m_ooo_buff->init(m_id, nullptr, true);

        ASSERT(m_ooo_buff->is_empty());
        ASSERT(m_ooo_buff->is_out_of_order());

        // try again
        ok = m_ooo_buff->add_data_point(tstamp, dp.get_value());
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

bool
TimeSeries::query_for_data(Tsdb *tsdb, TimeRange& range, std::vector<DataPointContainer*>& data)
{
    bool has_ooo = false;
    std::lock_guard<std::mutex> guard(m_lock);

    if ((m_buff != nullptr) && (! m_buff->is_empty()))
    {
        TimeRange r = m_buff->get_time_range();

        if (range.has_intersection(r))
        {
            PageIndex page_idx = 0;

            if (! data.empty())
            {
                DataPointContainer *last = data.back();
                page_idx = last->get_page_index() + 1;
            }

            DataPointContainer *container = (DataPointContainer*)
                MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
            container->collect_data(m_buff);
            container->set_page_index(page_idx);
            data.push_back(container);
        }
    }

    if ((m_ooo_buff != nullptr) && (! m_ooo_buff->is_empty()))
    {
        TimeRange r = m_ooo_buff->get_time_range();

        if (range.has_intersection(r))
        {
            PageIndex page_idx = 0;

            if (! data.empty())
            {
                DataPointContainer *last = data.back();
                page_idx = last->get_page_index() + 1;
            }

            DataPointContainer *container = (DataPointContainer*)
                MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
            container->collect_data(m_ooo_buff);
            container->set_page_index(page_idx);
            data.push_back(container);
            has_ooo = true;
        }
    }

    return has_ooo;
}

void
TimeSeries::query(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
/*
    Meter meter(METRIC_TICKTOCK_QUERY_TS_LATENCY_MS);

    if ((m_ooo_buff != nullptr) || (! m_ooo_pages.empty()))
    {
        query_with_ooo(range, downsampler, dps);
    }
    else
    {
        query_without_ooo(range, downsampler, dps);
    }
*/
}

void
TimeSeries::query_with_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
/*
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
            break;
        }

        if ((i+1) < container->size())
        {
            pq.emplace(container, i+1);
        }
    }
*/
}

void
TimeSeries::query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps)
{
/*
    std::lock_guard<std::mutex> guard(m_lock);

    for (PageInfo *page_info : m_pages)
    {
        this->query_without_ooo(range, downsampler, dps, page_info);
    }

    ASSERT(m_ooo_pages.empty());
    Logger::debug("Found %d data points in ts %T", dps.size(), this);
*/
}

void
TimeSeries::query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps, PageInfo *page_info)
{
#if 0
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
#endif
}

// Compress all out-of-order pages in m_ooo_pages[], if any.
// Return true if we actually compacted something.
bool
TimeSeries::compact(MetaFile& meta_file)
{
    return true;
/*
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
        Logger::trace("dp.first=%" PRIu64 ", dp.second=%f", dp.first, dp.second);

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
            info->flush(false, m_tsdb);
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
                //meta_file.append(this, file_id, id_from, id_to);

                file_id = info->get_file_id();
                id_from = id_to = info->get_id();
            }
        }
    }

    if (info != nullptr)
    {
        info->shrink_to_fit();
        MemoryManager::free_recyclable(info);
        //meta_file.append(this, file_id, id_from, id_to);
    }

    return (! dps.empty());
*/
}


}
