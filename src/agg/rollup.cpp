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

#include <limits.h>
#include <unordered_map>
#include "config.h"
#include "limit.h"
#include "logger.h"
#include "query.h"
#include "rollup.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


std::mutex RollupManager::m_lock;
std::unordered_map<uint64_t, RollupDataFile*> RollupManager::m_data_files;
RollupDataFile *RollupManager::m_wal_data_file = nullptr;
std::queue<off_t> RollupManager::m_sizes;
off_t RollupManager::m_size_hint;


RollupManager::RollupManager() :
    m_cnt(0),
    m_min(std::numeric_limits<double>::max()),
    m_max(std::numeric_limits<double>::min()),
    m_sum(0.0),
    m_tstamp(TT_INVALID_TIMESTAMP)
{
}

// Copy Constructor
RollupManager::RollupManager(const RollupManager& copy)
{
    copy_from(copy);
}

RollupManager::RollupManager(Timestamp tstamp, uint32_t cnt, double min, double max, double sum) :
    m_cnt(cnt),
    m_min(min),
    m_max(max),
    m_sum(sum),
    m_tstamp(tstamp)
{
    ASSERT(cnt != 0);
    ASSERT(tstamp != TT_INVALID_TIMESTAMP);
}

RollupManager::~RollupManager()
{
}

void
RollupManager::copy_from(const RollupManager& other)
{
    m_cnt = other.m_cnt;
    m_min = other.m_min;
    m_max = other.m_max;
    m_sum = other.m_sum;
    m_tstamp = other.m_tstamp;
}

RollupManager&
RollupManager::operator=(const RollupManager& other)
{
    if (this == &other)
        return *this;

    copy_from(other);
    return *this;
}

void
RollupManager::init()
{
    std::string wal_dir = Config::get_wal_dir();
    create_dir(wal_dir);
    m_wal_data_file = new RollupDataFile(wal_dir + "/rollup.data");

    // restore if necessary
    if (m_wal_data_file->exists())
    {
        std::unordered_map<TimeSeriesId,RollupManager> map;

/******
        m_backup_data_file->open();

        for (auto h : *m_backup_header_tmp_file)
        {
            TimeSeriesId tid = h.tid;
            RollupIndex idx = h.data_idx;

            Timestamp tstamp;
            uint32_t cnt;
            double min, max, sum;

            m_backup_data_file->query(idx, tstamp, cnt, min, max, sum);
            map.emplace(tid, RollupManager(tstamp, cnt, min, max, sum));
        }

        Tsdb::restore_rollup_mgr(map);

        m_backup_data_file->close();
        m_backup_header_tmp_file->close();
        m_backup_data_file->remove();
        m_backup_header_tmp_file->remove();
******/
    }
}

void
RollupManager::shutdown()
{
    std::lock_guard<std::mutex> guard(m_lock);

    for (const auto& entry: m_data_files)
    {
        RollupDataFile *file = entry.second;
        delete file;
    }

    m_data_files.clear();

    if (m_wal_data_file != nullptr)
    {
        delete m_wal_data_file;
        m_wal_data_file = nullptr;
    }
}

// Here we only handle in-order dps!
void
RollupManager::add_data_point(MetricId mid, TimeSeriesId tid, DataPoint& dp)
{
    Timestamp tstamp = to_sec(dp.get_timestamp());
    Timestamp interval = g_rollup_interval;
    double value = dp.get_value();

    if (m_tstamp == TT_INVALID_TIMESTAMP)
        m_tstamp = begin_month(tstamp);

    // step-down
    ASSERT(interval > 0);
    Timestamp tstamp1 = tstamp - (tstamp % interval);
    ASSERT(tstamp1 >= m_tstamp);

    if (tstamp1 > m_tstamp)
    {
        std::lock_guard<std::mutex> guard(m_lock);

        flush(mid, tid);

        Timestamp end = end_month(m_tstamp);

        for (m_tstamp += interval; (m_tstamp < end) && (m_tstamp < tstamp1); m_tstamp += interval)
            flush(mid, tid);

        if (m_tstamp >= end)
        {
            for (m_tstamp = begin_month(tstamp); m_tstamp < tstamp1; m_tstamp += interval)
                flush(mid, tid);
        }
    }
    else if (tstamp1 < m_tstamp)
    {
        // out-of-order!!!
        Logger::warn("Out-of-order rollup dp ignored!");
    }

    m_cnt++;
    m_min = std::min(m_min, value);
    m_max = std::max(m_min, value);
    m_sum += value;
}

void
RollupManager::flush(MetricId mid, TimeSeriesId tid)
{
    ASSERT(m_tstamp != TT_INVALID_TIMESTAMP);

    if (m_tstamp == TT_INVALID_TIMESTAMP)
        return;

    // write to rollup files
    RollupDataFile *file = get_data_file(mid, m_tstamp);
    file->add_data_point(tid, m_cnt, m_min, m_max, m_sum);

    // reset
    m_cnt = 0;
    m_min = std::numeric_limits<double>::max();
    m_max = std::numeric_limits<double>::min();
    m_sum = 0.0;
}

void
RollupManager::close(TimeSeriesId tid)
{
    if (m_tstamp == TT_INVALID_TIMESTAMP || m_cnt == 0)
        return;

    if (m_wal_data_file != nullptr)
        m_wal_data_file->add_data_point(tid, m_tstamp, m_cnt, m_min, m_max, m_sum);
}

double
RollupManager::query(struct rollup_entry *entry, RollupType type)
{
    ASSERT(entry != nullptr);
    ASSERT(entry->cnt != 0);
    ASSERT(type != RollupType::RU_NONE);

    double val = 0;

    switch (type)
    {
        case RollupType::RU_AVG:
            val = entry->sum / (double)entry->cnt;
            break;

        case RollupType::RU_CNT:
            val = (double)entry->cnt;
            break;

        case RollupType::RU_MAX:
            val = entry->max;
            break;

        case RollupType::RU_MIN:
            val = entry->min;
            break;

        case RollupType::RU_SUM:
            val = entry->sum;
            break;

        default:
            ASSERT(false);
    }

    return val;
}

// return false if no data will be returned;
bool
RollupManager::query(RollupType type, DataPointPair& dp)
{
    if (m_cnt == 0) return false;

    switch (type)
    {
        case RollupType::RU_AVG:
            dp.second = m_sum / (double)m_cnt;
            break;

        case RollupType::RU_CNT:
            dp.second = (double)m_cnt;
            break;

        case RollupType::RU_MAX:
            dp.second = m_max;
            break;

        case RollupType::RU_MIN:
            dp.second = m_min;
            break;

        case RollupType::RU_SUM:
            dp.second = m_sum;
            break;

        default:
            return false;
    }

    dp.first = m_tstamp;
    return true;
}

void
RollupManager::query(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, RollupType rollup)
{
    std::lock_guard<std::mutex> guard(m_lock);
    query_no_lock(mid, range, tasks, rollup);
}

void
RollupManager::query_no_lock(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, RollupType rollup)
{
    ASSERT(! tasks.empty());
    ASSERT(rollup != RollupType::RU_NONE);

    std::vector<RollupDataFile*> data_files;
    std::unordered_map<TimeSeriesId,QueryTask*> map;

    for (auto task: tasks)
    {
        ASSERT(map.find(task->get_ts_id()) == map.end());
        map[task->get_ts_id()] = task;
    }

    get_data_files(mid, range, data_files);

    for (auto file: data_files)
    {
        file->query(map, rollup);
        file->dec_ref_count();
    }
}

/* @return Stepped down timestamp of the input, in seconds.
 */
Timestamp
RollupManager::step_down(Timestamp tstamp)
{
    tstamp = to_sec(tstamp);
    return tstamp - (tstamp % g_rollup_interval);
}

int
RollupManager::get_rollup_bucket(MetricId mid)
{
    // TODO: CFG_TSDB_ROLLUP_BUCKETS shouldn't be changed
    return mid % Config::inst()->get_int(CFG_TSDB_ROLLUP_BUCKETS,CFG_TSDB_ROLLUP_BUCKETS_DEF);
}

RollupDataFile *
RollupManager::get_data_file(MetricId mid, Timestamp tstamp)
{
    std::time_t begin = begin_month(tstamp);
    uint64_t bucket = begin * MAX_ROLLUP_BUCKET_COUNT + get_rollup_bucket(mid);
    auto search = m_data_files.find(bucket);
    RollupDataFile *data_file = nullptr;

    if (search == m_data_files.end())
    {
        data_file = new RollupDataFile(mid, begin);
        m_data_files.insert(std::make_pair(bucket, data_file));
    }
    else
        data_file = search->second;

    ASSERT(data_file != nullptr);
    data_file->inc_ref_count();

    return data_file;
}

void
RollupManager::get_data_files(MetricId mid, TimeRange& range, std::vector<RollupDataFile*>& files)
{
    int year, month;
    Timestamp end = range.get_to_sec();

    get_year_month(range.get_from_sec(), year, month);
    month--;
    year -= 1900;

    for ( ; ; )
    {
        std::time_t ts = begin_month(year, month);
        if (end <= ts) break;

        RollupDataFile *data_file = get_data_file(mid, ts);

        if (! data_file->exists())
            delete data_file;
        else
            files.push_back(data_file);

        month++;
        if (month >= 12)
        {
            month = 0;
            year++;
        }
    }
}

void
RollupManager::rotate()
{
    uint64_t thrashing_threshold = g_rollup_interval;   // 1 hour
    Timestamp now = ts_now_sec();
    std::lock_guard<std::mutex> guard(m_lock);

    for (auto it = m_data_files.begin(); it != m_data_files.end(); )
    {
        RollupDataFile *file = it->second;

        if (file->close_if_idle(thrashing_threshold, now))
        {
            RollupManager::add_data_file_size(file->size());
            it = m_data_files.erase(it);
            delete file;
        }
        else
            it++;
    }
}

// Remember sizes of recent rollup data files
void
RollupManager::add_data_file_size(off_t size)
{
    if (m_sizes.size() >= 10)   // keep 10 most recent sizes
    {
        m_size_hint -= m_sizes.front();
        m_sizes.pop();
    }

    m_sizes.push(size);
    m_size_hint += size;
}

off_t
RollupManager::get_rollup_data_file_size()
{
    //std::lock_guard<std::mutex> guard(m_lock);
    return m_sizes.empty() ? 409600 : m_size_hint/m_sizes.size();
}


}
