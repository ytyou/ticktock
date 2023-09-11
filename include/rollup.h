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

#pragma once

#include "dp.h"
#include "type.h"


namespace tt
{


class Tsdb;


enum RollupType : unsigned char
{
    RU_NONE = 0,
    RU_AVG = 1,
    RU_CNT = 2,
    RU_MAX = 3,
    RU_MIN = 4,
    RU_SUM = 5
};


class __attribute__ ((__packed__)) RollupManager
{
public:
    RollupManager();
    ~RollupManager();

    // process in-order dps only
    void add_data_point(Tsdb *tsdb, MetricId mid, TimeSeriesId tid, DataPoint& dp);
    void flush(MetricId mid, TimeSeriesId tid);

    inline Tsdb *get_tsdb() { return m_tsdb; }
    inline Timestamp get_tstamp() const { return m_tstamp; }

    // return true if data-point found; false if no data
    bool query(RollupType type, DataPointPair& dp);

private:
    uint32_t m_count;
    double m_min;
    double m_max;
    double m_sum;
    Timestamp m_tstamp;
    RollupIndex m_prev_idx;
    Tsdb *m_tsdb;
};


}
