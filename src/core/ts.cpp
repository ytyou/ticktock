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

#include <algorithm>
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


#define TT_TIME_SERIES_INCREMENT    65536


std::atomic<TimeSeriesId> TimeSeries::m_next_id{0};
uint32_t TimeSeries::m_lock_count;
std::mutex *TimeSeries::m_locks;
default_contention_free_shared_mutex TimeSeries::m_ts_lock;
TimeSeries **TimeSeries::m_time_series;
std::size_t TimeSeries::m_ts_size;


/*
TimeSeries::TimeSeries(const char *metric, const char *key, Tag *tags) :
    TagOwner(true),
    m_next(nullptr)
{
    //ASSERT(false);
    //TimeSeriesId id = m_next_id.fetch_add(1);
    //TimeSeriesId id = TT_INVALID_TIME_SERIES_ID;
    //while (TT_INVALID_META_POSITION == MetaFile::instance()->put(key, TT_INVALID_META_POSITION, id))
        //MetaFile::instance()->expand();
    init(id, metric, key, tags);
}
*/

// called during restart/restore
TimeSeries::TimeSeries(TimeSeriesId id, Tag *tags) :
    TagOwner(true),
    m_next(nullptr)
{
    init(id, tags);

    //if (m_next_id.load() <= id)
        //m_next_id = id + 1;
}

TimeSeries::~TimeSeries()
{
    if (m_buff != nullptr)
    {
        delete m_buff;
        m_buff = nullptr;
    }

    if (m_ooo_buff != nullptr)
    {
        delete m_ooo_buff;
        m_ooo_buff = nullptr;
    }

/*
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
*/
}

TimeSeries *
TimeSeries::create(Tag *tags)
{
    TimeSeries *ts = new TimeSeries(get_next_id(), tags);
    set_ts(ts);
    return ts;
}

void
TimeSeries::init()
{
    // Birthday Paradox - Square approximation method
    int tcp_responders =
        Config::get_int(CFG_TCP_LISTENER_COUNT, CFG_TCP_LISTENER_COUNT_DEF) *
        Config::get_int(CFG_TCP_RESPONDERS_PER_LISTENER, CFG_TCP_RESPONDERS_PER_LISTENER_DEF);
    int http_responders =
        Config::get_int(CFG_HTTP_LISTENER_COUNT, CFG_HTTP_LISTENER_COUNT_DEF) *
        Config::get_int(CFG_HTTP_RESPONDERS_PER_LISTENER, CFG_HTTP_RESPONDERS_PER_LISTENER_DEF);
    float probability =
        Config::get_float(CFG_TS_LOCK_PROBABILITY, CFG_TS_LOCK_PROBABILITY_DEF);
    m_lock_count = std::max(tcp_responders, http_responders);
    m_lock_count = (uint32_t)((float)(m_lock_count * m_lock_count) / (2.0 * probability));
    m_locks = new std::mutex[m_lock_count];

    m_ts_size = 65536;
    m_time_series = (TimeSeries**) calloc(m_ts_size, sizeof(TimeSeries*));
    std::memset(m_time_series, 0, m_ts_size * sizeof(TimeSeries*));
    ASSERT(m_time_series[0] == nullptr);

    Logger::info("number of ts locks: %u", m_lock_count);
}

void
TimeSeries::init(TimeSeriesId id, Tag *tags)
{
    m_id = id;
    m_buff = nullptr;
    m_ooo_buff = nullptr;

    //m_metric = STRDUP(metric);
    //m_key = STRDUP(key);
    m_tags = tags;
}

TimeSeries *
TimeSeries::get_ts(TimeSeriesId id)
{
    ReadLock guard(m_ts_lock);
    if (m_ts_size <= id)
        return nullptr;
    return m_time_series[id];
}

void
TimeSeries::set_ts(TimeSeries *ts)
{
    TimeSeriesId id = ts->m_id;
    WriteLock guard(m_ts_lock);

    if (m_ts_size > id)
    {
        ASSERT(m_time_series[id] == nullptr || m_time_series[id] == ts);
        m_time_series[id] = ts;
        return;
    }

    // expand m_time_series[]
    std::size_t new_cap = id + TT_TIME_SERIES_INCREMENT;
    TimeSeries **tmp = static_cast<TimeSeries**>(aligned_alloc(g_sys_page_size, new_cap*sizeof(TimeSeries*)));
    std::memcpy(tmp, m_time_series, m_ts_size * sizeof(TimeSeries*));
    std::memset(&tmp[m_ts_size], 0, TT_TIME_SERIES_INCREMENT * sizeof(TimeSeries*));
    std::free(m_time_series);
    Logger::info("Expanded TimeSeries::m_time_series[] from %" PRIu64 " to %" PRIu64, m_ts_size, new_cap);
    m_time_series = tmp;
    m_ts_size = new_cap;

    m_time_series[id] = ts;
}

void
TimeSeries::restore(Tsdb *tsdb, PageSize offset, uint8_t start, char *buff, bool is_ooo)
{
    ASSERT(tsdb != nullptr);

    if (is_ooo)
    {
        ASSERT(m_ooo_buff == nullptr);
        m_ooo_buff = new PageInMemory(m_id, tsdb, true);
    }
    else
    {
        ASSERT(m_buff == nullptr);
        m_buff = new PageInMemory(m_id, tsdb, false);
    }
}

void
TimeSeries::flush(bool close)
{
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);
    flush_no_lock(close);
}

void
TimeSeries::flush_no_lock(bool close)
{
    if (m_buff != nullptr)
    {
        m_buff->flush(m_id);

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

        if (close)
        {
            delete m_buff;
            m_buff = nullptr;
        }
        else
        {
            m_buff->init(m_id, nullptr, false);
        }
    }

    if (m_ooo_buff != nullptr)
    {
        m_ooo_buff->flush(m_id);

        if (close)
        {
            delete m_ooo_buff;
            m_ooo_buff = nullptr;
        }
        else if (m_buff != nullptr)
        {
            m_buff->update_indices(m_ooo_buff);
            m_buff->init(m_id, nullptr, true);
        }
    }
}

void
TimeSeries::append(FILE *file)
{
    ASSERT(file != nullptr);
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);
    if (m_buff != nullptr) m_buff->append(m_id, file);
    if (m_ooo_buff != nullptr) m_ooo_buff->append(m_id, file);
}

bool
TimeSeries::add_data_point(DataPoint& dp)
{
    const Timestamp tstamp = dp.get_timestamp();
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

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

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

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

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

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

        if (m_buff != nullptr)
            m_buff->update_indices(m_ooo_buff);

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

        if (m_buff != nullptr)
            m_buff->update_indices(m_ooo_buff);

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
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

    if ((m_buff != nullptr) && (! m_buff->is_empty()) && (m_buff->get_tsdb() == tsdb))
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
            ASSERT(container->size() > 0);
            data.push_back(container);
        }
    }

    if ((m_ooo_buff != nullptr) && (! m_ooo_buff->is_empty()) && (m_ooo_buff->get_tsdb() == tsdb))
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
            ASSERT(container->size() > 0);
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
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

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


}
