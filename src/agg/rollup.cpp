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

#include "logger.h"
#include "rollup.h"
#include "tsdb.h"


namespace tt
{


RollupManager::RollupManager() :
    m_count(0),
    m_min(0),
    m_max(0),
    m_sum(0),
    m_tsdb(nullptr),
    m_tstamp(TT_INVALID_TIMESTAMP),
    m_prev_idx(TT_INVALID_ROLLUP_INDEX)
{
}

RollupManager::~RollupManager()
{
}

// Here we only handle in-order dps!
void
RollupManager::add_data_point(Tsdb *tsdb, MetricId mid, TimeSeriesId tid, DataPoint& dp)
{
    ASSERT(tsdb != nullptr);

    if (m_tsdb == nullptr)
        m_tsdb = tsdb;

    Timestamp tstamp = to_sec(dp.get_timestamp());
    Timestamp interval = m_tsdb->get_rollup_interval();
    double value = dp.get_value();

    // step-down
    ASSERT(interval > 0);
    tstamp = tstamp - (tstamp % interval);

    if (m_tstamp == TT_INVALID_TIMESTAMP)
        m_tstamp = tstamp;

    if (tstamp != m_tstamp)
    {
        flush(mid, tid);

        Timestamp end = m_tsdb->get_time_range().get_to_sec();

        for (m_tstamp += interval; (m_tstamp < end) && (m_tstamp < tstamp); m_tstamp += interval)
            flush(mid, tid);

        if (m_tstamp >= end)
        {
            ASSERT(tsdb != m_tsdb);
            m_tsdb = tsdb;
            m_prev_idx = TT_INVALID_ROLLUP_INDEX;
            interval = m_tsdb->get_rollup_interval();

            for (m_tstamp = m_tsdb->get_time_range().get_from_sec(); m_tstamp < tstamp; m_tstamp += interval)
                flush(mid, tid);
        }
    }

    m_count++;
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
    m_prev_idx = m_tsdb->add_rollup_point(mid, tid, m_prev_idx, m_count, m_min, m_max, m_sum);

    // reset
    m_count = 0;
    m_min = m_max = m_sum = 0.0;
}

// return false if no data will be returned;
bool
RollupManager::query(RollupType type, DataPointPair& dp)
{
    if (m_count == 0) return false;

    switch (type)
    {
        case RollupType::RU_AVG:
            dp.second = m_sum / (double)m_count;
            break;

        case RollupType::RU_CNT:
            dp.second = (double)m_count;
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


}
