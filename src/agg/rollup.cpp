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
#include "logger.h"
#include "rollup.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


RollupDataFile *RollupManager::m_backup_data_file = nullptr;
RollupHeaderTmpFile *RollupManager::m_backup_header_tmp_file = nullptr;


RollupManager::RollupManager() :
    m_cnt(0),
    m_min(std::numeric_limits<double>::max()),
    m_max(std::numeric_limits<double>::min()),
    m_sum(0.0),
    m_tsdb(nullptr),
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
    m_tsdb = Tsdb::inst(validate_resolution(tstamp), false);
    ASSERT(m_tsdb != nullptr);
}

RollupManager::~RollupManager()
{
    if (m_tsdb != nullptr)
    {
        m_tsdb->dec_ref_count();
        m_tsdb = nullptr;
    }
}

void
RollupManager::copy_from(const RollupManager& other)
{
    m_cnt = other.m_cnt;
    m_min = other.m_min;
    m_max = other.m_max;
    m_sum = other.m_sum;
    m_tsdb = other.m_tsdb;
    m_tstamp = other.m_tstamp;

    if (m_tsdb != nullptr)
        m_tsdb->inc_ref_count();
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
    ASSERT(m_backup_data_file == nullptr);
    ASSERT(m_backup_header_tmp_file == nullptr);

    std::string backup_dir = Config::get_data_dir() + "/backup";
    create_dir(backup_dir);
    m_backup_data_file = new RollupDataFile(backup_dir + "/rollup.data");
    m_backup_header_tmp_file = new RollupHeaderTmpFile(backup_dir + "/rollup.header.tmp");

    // restore if necessary
    if (m_backup_data_file->exists() && m_backup_header_tmp_file->exists())
    {
        std::unordered_map<TimeSeriesId,RollupManager> map;

        m_backup_data_file->ensure_open(true);
        m_backup_header_tmp_file->ensure_open(true);

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
    }
}

void
RollupManager::shutdown()
{
    ASSERT(m_backup_data_file != nullptr);
    ASSERT(m_backup_header_tmp_file != nullptr);

    if (m_backup_data_file != nullptr)
    {
        delete m_backup_data_file;
        m_backup_data_file = nullptr;
    }

    if (m_backup_header_tmp_file != nullptr)
    {
        delete m_backup_header_tmp_file;
        m_backup_header_tmp_file = nullptr;
    }
}

// Here we only handle in-order dps!
void
RollupManager::add_data_point(Tsdb *tsdb, MetricId mid, TimeSeriesId tid, DataPoint& dp)
{
    ASSERT(tsdb != nullptr);

    if (m_tsdb == nullptr)
    {
        m_tsdb = tsdb;
        m_tsdb->inc_ref_count();
        m_tstamp = m_tsdb->get_time_range().get_from_sec();
    }

    Timestamp tstamp = to_sec(dp.get_timestamp());
    Timestamp interval = m_tsdb->get_rollup_interval();
    double value = dp.get_value();

    // step-down
    ASSERT(interval > 0);
    Timestamp tstamp1 = tstamp - (tstamp % interval);
    ASSERT(tstamp1 >= m_tstamp);
    ASSERT(m_tstamp != TT_INVALID_TIMESTAMP);

    if (tstamp1 > m_tstamp)
    {
        flush(mid, tid);

        Timestamp end = m_tsdb->get_time_range().get_to_sec();

        for (m_tstamp += interval; (m_tstamp < end) && (m_tstamp < tstamp1); m_tstamp += interval)
            flush(mid, tid);

        if (m_tstamp >= end)
        {
            m_tsdb->dec_ref_count();
            m_tsdb = tsdb;
            m_tsdb->inc_ref_count();
            interval = m_tsdb->get_rollup_interval();
            tstamp1 = tstamp - (tstamp % interval);

            for (m_tstamp = m_tsdb->get_time_range().get_from_sec(); m_tstamp < tstamp1; m_tstamp += interval)
                flush(mid, tid);
        }
    }

    m_cnt++;
    m_min = std::min(m_min, value);
    m_max = std::max(m_min, value);
    m_sum += value;
}

void
RollupManager::flush(MetricId mid, TimeSeriesId tid)
{
    if (m_tstamp == TT_INVALID_TIMESTAMP)
        return;

    // write to rollup files
    m_tsdb->add_rollup_point(mid, tid, m_cnt, m_min, m_max, m_sum);

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

    RollupEntry data_idx = TT_INVALID_ROLLUP_ENTRY;

    if (m_backup_data_file != nullptr)
    {
        m_backup_data_file->ensure_open(false);
        data_idx = m_backup_data_file->add_data_point(m_tstamp, m_cnt, m_min, m_max, m_sum);
        //m_backup_data_file = nullptr;
    }

    if (m_backup_header_tmp_file != nullptr)
    {
        m_backup_header_tmp_file->ensure_open(false);
        m_backup_header_tmp_file->add_index(tid, data_idx);
        //m_backup_header_tmp_file = nullptr;
    }
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
    ASSERT(m_tsdb != nullptr);

    Timestamp interval = m_tsdb->get_rollup_interval();
    ASSERT(interval > 0);
    tstamp = to_sec(tstamp);
    return tstamp - (tstamp % interval);
}


}
