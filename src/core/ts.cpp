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
#include "limit.h"
#include "logger.h"
#include "query.h"
#include "ts.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


std::atomic<TimeSeriesId> TimeSeries::m_next_id{0};
uint32_t TimeSeries::m_lock_count;
std::mutex *TimeSeries::m_locks;


TimeSeries::TimeSeries(const char *metric, const char *key, Tag *tags) :
    m_next(nullptr),
    m_tags(tags),
    m_buff(nullptr),
    m_ooo_buff(nullptr)
{
    ASSERT(metric != nullptr);
    ASSERT(key != nullptr);

    m_id = m_next_id.fetch_add(1);
    MetaFile::instance()->add_ts(metric, key, m_id);
}

TimeSeries::TimeSeries(const char *metric, TagBuilder& builder) :
    m_next(nullptr),
    m_tags(builder),
    m_buff(nullptr),
    m_ooo_buff(nullptr)
{
    m_id = m_next_id.fetch_add(1);
    MetaFile::instance()->add_ts(metric, m_tags, m_id);
}

TimeSeries::TimeSeries(TagBuilder& builder) :
    m_next(nullptr),
    m_tags(builder),
    m_buff(nullptr),
    m_ooo_buff(nullptr)
{
    m_id = m_next_id.fetch_add(1);
}

// called during restart/restore
TimeSeries::TimeSeries(TagBuilder& builder, TimeSeriesId id) :
    m_id(id),
    m_next(nullptr),
    m_tags(builder),
    m_buff(nullptr),
    m_ooo_buff(nullptr)
{
    if (m_next_id.load() <= id)
        m_next_id = id + 1;
}

// called during restart/restore
TimeSeries::TimeSeries(TimeSeriesId id, const char *metric, const char *key, Tag *tags) :
    m_next(nullptr),
    m_tags(tags)
{
    init(id, metric, key, tags);

    if (m_next_id.load() <= id)
        m_next_id = id + 1;
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

    //if (m_tags != nullptr)
    //{
        //std::free(m_tags);
        //m_tags = nullptr;
    //}

    //if (m_key != nullptr)
    //{
        //FREE(m_key);
        //m_key = nullptr;
    //}

    //if (m_metric != nullptr)
    //{
        //FREE(m_metric);
        //m_metric = nullptr;
    //}
}

void
TimeSeries::init()
{
    // Birthday Paradox - Square approximation method
    int tcp_responders = 0;
    int http_responders = 0;

    for (int i = 0; i < LISTENER0_COUNT; i++)
    {
        tcp_responders += Config::inst()->get_tcp_listener_count(i)
            * Config::inst()->get_tcp_responders_per_listener(i);
        http_responders += Config::inst()->get_http_listener_count(i)
            * Config::inst()->get_http_responders_per_listener(i);
    }

    float probability =
        Config::inst()->get_float(CFG_TS_LOCK_PROBABILITY, CFG_TS_LOCK_PROBABILITY_DEF);
    m_lock_count = std::max(tcp_responders, http_responders);
    m_lock_count = (uint32_t)((float)(m_lock_count * m_lock_count) / (2.0 * probability));
    m_locks = new std::mutex[m_lock_count];
    Logger::info("number of ts locks: %u", m_lock_count);
}

void
TimeSeries::init(TimeSeriesId id, const char *metric, const char *key, Tag *tags)
{
    m_id = id;
    m_buff = nullptr;
    m_ooo_buff = nullptr;
}

void
TimeSeries::cleanup()
{
#ifdef _DEBUG
    if (m_locks != nullptr)
    {
        delete[] m_locks;
        m_locks = nullptr;
    }
#endif
}

// restore from WAL
void
TimeSeries::restore(Tsdb *tsdb, MetricId mid, Timestamp tstamp, PageSize offset, uint8_t start, uint8_t *buff, int size, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx)
{
    ASSERT(tsdb != nullptr);
    bool out_of_date;

    if (is_ooo)
    {
        ASSERT(m_ooo_buff == nullptr);
        m_ooo_buff = new PageInMemory(mid, m_id, tsdb, true, file_idx, header_idx);
        out_of_date = m_ooo_buff->restore(tstamp, buff, offset, start, mid, m_id, true);
        ASSERT(! out_of_date);
    }
    else
    {
        ASSERT(m_buff == nullptr);
        m_buff = new PageInMemory(mid, m_id, tsdb, false, file_idx, header_idx);
        out_of_date = m_buff->restore(tstamp, buff, offset, start, mid, m_id, false);

        // See if data on disk is newer than what we've just restored?
        // If so, the data we just restored is already on disk, ignore them.
        if (out_of_date)
        {
            delete m_buff;
            m_buff = nullptr;
        }
    }
}

void
TimeSeries::close(MetricId mid)
{
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

    if (m_buff != nullptr)
    {
        m_buff->flush(mid, m_id);

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

        delete m_buff;
        m_buff = nullptr;
    }

    if (m_ooo_buff != nullptr)
    {
        m_ooo_buff->flush(mid, m_id);
        delete m_ooo_buff;
        m_ooo_buff = nullptr;
    }

    m_rollup.close(m_id);
}

void
TimeSeries::flush(MetricId mid)
{
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);
    flush_no_lock(mid);
}

void
TimeSeries::flush_no_lock(MetricId mid, bool close)
{
    if (m_buff != nullptr)
    {
        m_buff->flush(mid, m_id);

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

        if (close)
        {
            delete m_buff;
            m_buff = nullptr;
        }
        else
        {
            m_buff->init(mid, m_id, nullptr, false);
        }
    }

    if (m_ooo_buff != nullptr)
    {
        m_ooo_buff->flush(mid, m_id);

        if (close)
        {
            delete m_ooo_buff;
            m_ooo_buff = nullptr;
        }
        else if (m_buff != nullptr)
        {
            m_buff->update_indices(m_ooo_buff);
            m_buff->init(mid, m_id, nullptr, true);
        }
    }

    //m_rollup.flush(mid, m_id);
}

// write to WAL
void
TimeSeries::append(MetricId mid, FILE *file)
{
    ASSERT(file != nullptr);
    //std::lock_guard<std::mutex> guard(m_lock);
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);
    if (m_buff != nullptr) m_buff->append(mid, m_id, file);
    if (m_ooo_buff != nullptr) m_ooo_buff->append(mid, m_id, file);
}

/* The special values of 'NaN' and 'Inf' should be treated as out-of-order,
 * because our compression algorithms (except v0) can't handle them.
 */
bool
TimeSeries::add_data_point(MetricId mid, DataPoint& dp)
{
    int in_range;
    const double value = dp.get_value();
    const Timestamp tstamp = dp.get_timestamp();
    bool is_ooo = isnan(value) || isinf(value);
    bool update_rollup = false;
    //std::lock_guard<std::mutex> guard(m_lock);

    // timestamp can't be 14 digits or more
    if (MAX_MS_SINCE_EPOCH <= tstamp)
        return false;

    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

    // Make sure we have a valid m_buff (PageInMemory)
    if (UNLIKELY(m_buff == nullptr))
    {
        //ASSERT(m_ooo_buff == nullptr);
        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        ASSERT(tsdb != nullptr);
        Timestamp last_tstamp = tsdb->get_last_tstamp(mid, m_id);
        is_ooo = is_ooo || (tstamp <= last_tstamp);
        if (! is_ooo)
        {
            update_rollup = true;
            m_buff = new PageInMemory(mid, m_id, tsdb, false);
        }
    }
    else if ((in_range = m_buff->in_range_strictly(tstamp)) != 0)
    {
        is_ooo = is_ooo || (in_range <= 0);

        if (! is_ooo)
        {
            m_buff->flush(mid, m_id);

            if (m_ooo_buff != nullptr)
                m_ooo_buff->update_indices(m_buff);

            // reset the m_buff
            Tsdb *tsdb = Tsdb::inst(tstamp, true);
            m_buff->init(mid, m_id, tsdb, false);
            update_rollup = true;
        }
    }
    else
    {
        //Timestamp last_tstamp = m_buff->get_last_tstamp(mid, m_id);
        //is_ooo = (tstamp <= last_tstamp);
        is_ooo = is_ooo || m_buff->is_out_of_order(mid, m_id, tstamp);
    }

    if (is_ooo)
        return add_ooo_data_point(mid, dp);

    ASSERT(m_buff != nullptr);
    //ASSERT(m_buff->in_range(tstamp) == 0);

    bool ok = m_buff->add_data_point(tstamp, value);

    if (! ok)
    {
        ASSERT(m_buff->is_full());

        m_buff->flush(mid, m_id);
        m_buff->init(mid, m_id, nullptr, false);

        if (m_ooo_buff != nullptr)
            m_ooo_buff->update_indices(m_buff);

        ASSERT(m_buff->is_empty());

        // try again
        ok = m_buff->add_data_point(tstamp, value);
        ASSERT(ok);
    }

    ASSERT(! m_buff->is_empty());

    // rollup
    if (g_rollup_enabled)
    {
        if (update_rollup)
            m_rollup.update_data_file(mid, dp);
        m_rollup.add_data_point(m_buff->get_tsdb(), mid, m_id, dp);
    }

    return ok;
}

// Lock is acquired in add_data_point() already!
bool
TimeSeries::add_ooo_data_point(MetricId mid, DataPoint& dp)
{
    bool update_rollup = false;
    const Timestamp tstamp = dp.get_timestamp();

    // Make sure we have a valid m_ooo_buff (PageInMemory)
    if (m_ooo_buff == nullptr)
    {
        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_ooo_buff = new PageInMemory(mid, m_id, tsdb, true);
        tsdb->set_out_of_order(m_id, true);
        update_rollup = true;
    }
    else if (m_ooo_buff->in_range_strictly(tstamp) != 0)
    {
        m_ooo_buff->flush(mid, m_id);

        if (m_buff != nullptr)
            m_buff->update_indices(m_ooo_buff);

        Tsdb *tsdb = Tsdb::inst(tstamp, true);
        m_ooo_buff->init(mid, m_id, tsdb, true);
        tsdb->set_out_of_order(m_id, true);
        update_rollup = true;
    }

    bool ok = m_ooo_buff->add_data_point(tstamp, dp.get_value());

    if (! ok)
    {
        ASSERT(m_ooo_buff->is_full());

        m_ooo_buff->flush(mid, m_id);
        m_ooo_buff->init(mid, m_id, nullptr, true);

        ASSERT(m_ooo_buff->is_empty());
        ASSERT(m_ooo_buff->is_out_of_order());

        if (m_buff != nullptr)
            m_buff->update_indices(m_ooo_buff);

        // try again
        ok = m_ooo_buff->add_data_point(tstamp, dp.get_value());
        ASSERT(ok);
    }

    // rollup
    if (g_rollup_enabled)
    {
        if (update_rollup)
            m_rollup.update_data_file(mid, dp);
        m_rollup.add_data_point(m_ooo_buff->get_tsdb(), mid, m_id, dp);
    }

    return ok;
}

bool
TimeSeries::query_for_data(Tsdb *tsdb, const TimeRange& range, std::vector<DataPointContainer*>& data)
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
TimeSeries::query_for_rollup(const TimeRange& range, QueryTask *qt, RollupType rollup, bool ms)
{
    ASSERT(rollup != RollupType::RU_NONE);

    Timestamp ts = m_rollup.get_tstamp();   // in seconds

    if (is_ms(range.get_from()))
        ts *= 1000;

    if (range.in_range(ts) == 0)
    {
        struct rollup_entry_ext entry;

        if (m_rollup.get(entry))
        {
            if (ms) entry.tstamp *= 1000;
            entry.tid = m_id;
            qt->add_data_point(&entry, rollup);
        }
    }
}

void
TimeSeries::archive(MetricId mid, Timestamp now_sec, Timestamp threshold_sec)
{
    std::lock_guard<std::mutex> guard(m_locks[m_id % m_lock_count]);

    if (m_buff != nullptr)
    {
        if (m_buff->is_empty())
        {
            delete m_buff;
            m_buff = nullptr;
        }
        else if (((int64_t)now_sec - (int64_t)to_sec(m_buff->get_last_tstamp(mid, m_id))) > (int64_t)threshold_sec)
        {
            flush_no_lock(mid, true);
        }
    }

    if ((m_ooo_buff != nullptr) && (m_buff == nullptr))
        flush_no_lock(mid, true);
}

void
TimeSeries::restore_rollup_mgr(const struct rollup_entry_ext& entry)
{
    ASSERT(entry.tid == m_id);
    m_rollup.copy_from(entry);
}


}
