/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include "config.h"
#include "dp.h"
#include "tsdb.h"
#include "global.h"
#include "memmgr.h"
#include "logger.h"
#include "stats.h"
#include "meter.h"
#include "utils.h"


namespace tt
{


const char *METRIC_TICKTOCK_PAGE_RESTORE_TOTAL_MS = "ticktock.page.restore.total.ms";
const char *METRIC_TICKTOCK_QUERY_LATENCY_MS = "ticktock.query.latency.ms";
const char *METRIC_TICKTOCK_QUERY_AGGREGATE_LATENCY_MS = "ticktock.query.aggregate.latency.ms";
const char *METRIC_TICKTOCK_QUERY_TS_LATENCY_MS = "ticktock.query.ts.latency.ms";
const char *METRIC_TICKTOCK_TSDB_COMPACT_MS = "ticktock.tsdb.compact.ms";
const char *METRIC_TICKTOCK_TSDB_LOAD_TOTAL_MS = "ticktock.tsdb.load.total.ms";
const char *METRIC_TICKTOCK_TSDB_ROLLUP_MS = "ticktock.tsdb.rollup.ms";
const char *METRIC_TICKTOCK_TSDB_ROTATE_MS = "ticktock.tsdb.rotate.ms";


Meter::Meter(const char *metric, const MeterType type) :
    m_type(type),
    m_metric(metric),
    m_start(std::chrono::system_clock::now())
{
    ASSERT(m_metric != nullptr);
}

Meter::~Meter()
{
    // We do NOT own memory pointed to by m_metric, so don't try to free it!
    if (! g_self_meter_enabled) return;

    // Use current time as timestamp to minimize number of out-order data points.
    using namespace std::chrono;
    system_clock::time_point end = system_clock::now();
    Timestamp ts = duration_cast<milliseconds>(end.time_since_epoch()).count();
    DataPoint *dp = (DataPoint*) MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT);

    if (! g_tstamp_resolution_ms)
        ts /= 1000L;

    if (m_type == MeterType::COUNT)
    {
        dp->init(ts, (double)1.0);
    }
    else    // m_type == MeterType::GAUGE
    {
        auto ms = duration_cast<milliseconds>(end - m_start).count();
        dp->init(ts, (double)ms);
    }

    dp->set_metric(m_metric);
    dp->add_tag(THREAD_TAG_NAME, (char*)g_thread_id.c_str());

    Stats::add_data_point(dp);
}


}
