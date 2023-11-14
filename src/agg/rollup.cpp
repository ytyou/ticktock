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
#include "rollup.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


std::mutex RollupManager::m_lock;
std::unordered_map<uint64_t, RollupDataFile*> RollupManager::m_data_files;
RollupDataFile *RollupManager::m_backup_data_file = nullptr;


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
    std::string backup_dir = Config::get_data_dir() + "/backup";
    create_dir(backup_dir);
    m_backup_data_file = new RollupDataFile(backup_dir + "/rollup.data");

    // restore if necessary
    if (m_backup_data_file->exists())
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

    if (m_backup_data_file != nullptr)
    {
        delete m_backup_data_file;
        m_backup_data_file = nullptr;
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
    RollupDataFile *file = get_rollup_data_file(mid, m_tstamp);
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

    if (m_backup_data_file != nullptr)
        m_backup_data_file->add_data_point(tid, m_tstamp, m_cnt, m_min, m_max, m_sum);
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
RollupManager::get_rollup_data_file(MetricId mid, Timestamp tstamp)
{
    std::time_t begin = begin_month(tstamp);
    uint64_t bucket = begin * MAX_ROLLUP_BUCKET_COUNT + get_rollup_bucket(mid);
    std::lock_guard<std::mutex> guard(m_lock);
    auto search = m_data_files.find(bucket);
    RollupDataFile *data_file = nullptr;

    if (search == m_data_files.end())
    {
        data_file = new RollupDataFile(mid, begin);
        m_data_files.insert(std::make_pair(bucket, data_file));
    }
    else
        data_file = search->second;

    return data_file;
}

void
RollupManager::rotate()
{
    uint64_t thrashing_threshold =
        Config::inst()->get_time(CFG_TSDB_THRASHING_THRESHOLD, TimeUnit::SEC, CFG_TSDB_THRASHING_THRESHOLD_DEF);
    Timestamp now = ts_now_sec();
    std::lock_guard<std::mutex> guard(m_lock);

    for (const auto& entry: m_data_files)
    {
        RollupDataFile *file = entry.second;
        Timestamp last = file->get_last_access();

        if (thrashing_threshold < (now - last))
            file->close();
    }
}


}
