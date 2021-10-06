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

#pragma once


namespace tt
{


extern const char *METRIC_TICKTOCK_PAGE_RESTORE_TOTAL_MS;
extern const char *METRIC_TICKTOCK_QUERY_LATENCY_MS;
extern const char *METRIC_TICKTOCK_QUERY_AGGREGATE_LATENCY_MS;
extern const char *METRIC_TICKTOCK_QUERY_TS_LATENCY_MS;
extern const char *METRIC_TICKTOCK_TSDB_COMPACT_MS;
extern const char *METRIC_TICKTOCK_TSDB_LOAD_TOTAL_MS;
extern const char *METRIC_TICKTOCK_TSDB_ROTATE_MS;


enum MeterType : unsigned char
{
    COUNT = 0,
    GAUGE = 1
};


/* Used to collect TickTock's own metrics.
 */
class Meter
{
public:
    Meter(const char *metric, const MeterType type = MeterType::GAUGE);
    virtual ~Meter();

private:
    MeterType m_type;
    const char *m_metric;   // We don't own this memory so don't free it
    std::chrono::system_clock::time_point m_start;
};


}
