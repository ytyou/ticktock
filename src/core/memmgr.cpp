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
#include <cstring>
#include <stdlib.h>
#include <unistd.h>
#include "admin.h"
#include "bitset.h"
#include "compress.h"
#include "config.h"
#include "memmgr.h"
#include "ts.h"
#include "tsdb.h"
#include "logger.h"
#include "utils.h"
#include "aggregate.h"
#include "down.h"
#include "query.h"
#include "rate.h"
#include "stats.h"
#include "timer.h"


namespace tt
{


bool MemoryManager::m_initialized = false;
uint64_t MemoryManager::m_network_buffer_len = 0;
uint64_t MemoryManager::m_network_buffer_small_len = 0;

std::mutex MemoryManager::m_network_lock;
char *MemoryManager::m_network_buffer_free_list = nullptr;

std::mutex MemoryManager::m_network_small_lock;
char *MemoryManager::m_network_buffer_small_free_list = nullptr;

std::mutex MemoryManager::m_locks[RecyclableType::RT_COUNT];
Recyclable * MemoryManager::m_free_lists[RecyclableType::RT_COUNT];

std::atomic<int> MemoryManager::m_free[RecyclableType::RT_COUNT+2];
std::atomic<int> MemoryManager::m_total[RecyclableType::RT_COUNT+2];

std::mutex MemoryManager::m_garbage_lock;
int MemoryManager::m_max_usage[RecyclableType::RT_COUNT+2][MAX_USAGE_SIZE];
int MemoryManager::m_max_usage_idx;

#ifdef _DEBUG
std::unordered_map<Recyclable*,bool> MemoryManager::m_maps[RecyclableType::RT_COUNT];
#endif

#ifdef TT_STATS
std::atomic<uint64_t> g_query_count{0};
std::atomic<uint64_t> g_query_latency_ms{0};
#endif


MemoryManager::MemoryManager()
{
    for (int i = 0; i < RecyclableType::RT_COUNT; i++)
    {
        m_free_lists[i] = nullptr;
    }
}

char *
MemoryManager::alloc_network_buffer()
{
    char* buff = nullptr;

    {
        std::lock_guard<std::mutex> guard(m_network_lock);
        buff = m_network_buffer_free_list;
        if (buff != nullptr)
        {
            m_network_buffer_free_list = *((char**)buff);
            ASSERT(((long)m_network_buffer_free_list % g_page_size) == 0);
            m_free[RecyclableType::RT_COUNT]--;
        }
    }

    ASSERT(m_initialized);

    if (buff == nullptr)
    {
        // TODO: check if we have enough memory
        buff =
            static_cast<char*>(aligned_alloc(g_page_size, m_network_buffer_len));
        if (buff == nullptr)
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
        ASSERT(((long)buff % g_page_size) == 0);
        m_total[RecyclableType::RT_COUNT]++;
    }

    ASSERT(((long)buff % g_page_size) == 0);
    return buff;
}

void
MemoryManager::free_network_buffer(char* buff)
{
    if (UNLIKELY(buff == nullptr))
    {
        Logger::error("Passing nullptr to MemoryManager::free_network_buffer()");
        return;
    }

    ASSERT(((long)buff % g_page_size) == 0);
    ASSERT(((long)m_network_buffer_free_list % g_page_size) == 0);
    std::lock_guard<std::mutex> guard(m_network_lock);
    *((char**)buff) = m_network_buffer_free_list;
    m_network_buffer_free_list = buff;
    m_free[RecyclableType::RT_COUNT]++;
}

char *
MemoryManager::alloc_network_buffer_small()
{
    char* buff = nullptr;

    {
        std::lock_guard<std::mutex> guard(m_network_small_lock);
        buff = m_network_buffer_small_free_list;
        if (buff != nullptr)
        {
            m_network_buffer_small_free_list = *((char**)buff);
            m_free[RecyclableType::RT_COUNT+1]--;
        }
    }

    ASSERT(m_initialized);

    if (buff == nullptr)
    {
        // TODO: check if we have enough memory
        buff =
            static_cast<char*>(malloc(m_network_buffer_small_len));
        if (buff == nullptr)
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
        m_total[RecyclableType::RT_COUNT+1]++;
    }

    return buff;
}

void
MemoryManager::free_network_buffer_small(char* buff)
{
    if (UNLIKELY(buff == nullptr))
    {
        Logger::error("Passing nullptr to MemoryManager::free_network_buffer()");
        return;
    }

    std::lock_guard<std::mutex> guard(m_network_small_lock);
    *((char**)buff) = m_network_buffer_small_free_list;
    m_network_buffer_small_free_list = buff;
    m_free[RecyclableType::RT_COUNT+1]++;
}

void
MemoryManager::init()
{
    g_page_size = Config::inst()->get_bytes(CFG_TSDB_PAGE_SIZE, CFG_TSDB_PAGE_SIZE_DEF);
    if (g_page_size < 64)
        g_page_size = 64;   // min page size
    else if (g_page_size > UINT16_MAX)
        g_page_size = ((long)UINT16_MAX / 128) * 128;
    Logger::info("mm::page-size = %u", g_page_size);
    unsigned long page_cnt =
        Config::inst()->get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    if (page_cnt > UINT16_MAX)
        page_cnt = UINT16_MAX;
    g_page_count = page_cnt;

    m_network_buffer_small_len = MAX_HEADER_SIZE + MAX_SMALL_PAYLOAD;
    m_network_buffer_len = Config::inst()->get_bytes(CFG_TCP_BUFFER_SIZE, CFG_TCP_BUFFER_SIZE_DEF);
    if (m_network_buffer_len < g_sys_page_size) m_network_buffer_len = g_sys_page_size;
    // make sure it's multiple of g_sys_page_size
    m_network_buffer_len = ((long)m_network_buffer_len / g_sys_page_size) * g_sys_page_size;
    Logger::info("mm::m_network_buffer_len = %" PRIu64, m_network_buffer_len);
    ASSERT(m_network_buffer_len > 0);
    if (m_network_buffer_small_len > m_network_buffer_len)
        m_network_buffer_small_len = m_network_buffer_len;
    Logger::info("mm::m_network_buffer_small_len = %" PRIu64, m_network_buffer_small_len);
    for (int i = 0; i < RecyclableType::RT_COUNT; i++)
        m_free_lists[i] = nullptr;
    for (int i = 0; i < RecyclableType::RT_COUNT+2; i++)
        m_free[i] = m_total[i] = 0;
    m_initialized = true;

    m_max_usage_idx = 0;
    for (int i = 0; i < RecyclableType::RT_COUNT+2; i++)
    {
        for (int j = 0; j < MAX_USAGE_SIZE; j++)
            m_max_usage[i][j] = 0;
    }

    int freq = (int)Config::inst()->get_time(CFG_TSDB_GC_FREQUENCY, TimeUnit::SEC, CFG_TSDB_GC_FREQUENCY_DEF);
    if (freq > 0)
    {
        Task task;
        task.doit = &MemoryManager::collect_garbage;
        task.data.integer = 0;  // indicates this is from scheduled task (vs. interactive cmd)
        Timer::inst()->add_task(task, freq, "gc");
        Logger::info("GC Freq: %d secs", freq);
    }
}

void
MemoryManager::collect_stats(Timestamp ts, std::vector<DataPoint> &dps)
{
#define COLLECT_STATS_FOR(RTYPE, RNAME, SIZE_OF_RTYPE)     \
    {   \
        {   \
            dps.emplace_back(ts, (double)m_total[RTYPE] * (SIZE_OF_RTYPE));   \
            auto& dp = dps.back();  \
            dp.set_metric("ticktock.mem.reusable.total");   \
            dp.add_tag(TYPE_TAG_NAME, RNAME);    \
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str()); \
        }   \
        {   \
            dps.emplace_back(ts, (double)m_free[RTYPE] * (SIZE_OF_RTYPE));    \
            auto& dp = dps.back();  \
            dp.set_metric("ticktock.mem.reusable.free");    \
            dp.add_tag(TYPE_TAG_NAME, RNAME);    \
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str()); \
        }   \
    }

    COLLECT_STATS_FOR(RT_AGGREGATOR_AVG, "aggregator_avg", sizeof(AggregatorAvg))
    COLLECT_STATS_FOR(RT_AGGREGATOR_BOTTOM, "aggregator_bottom", sizeof(AggregatorBottom))
    COLLECT_STATS_FOR(RT_AGGREGATOR_COUNT, "aggregator_count", sizeof(AggregatorCount))
    COLLECT_STATS_FOR(RT_AGGREGATOR_DEV, "aggregator_dev", sizeof(AggregatorDev))
    COLLECT_STATS_FOR(RT_AGGREGATOR_MAX, "aggregator_max", sizeof(AggregatorMax))
    COLLECT_STATS_FOR(RT_AGGREGATOR_MIN, "aggregator_min", sizeof(AggregatorMin))
    COLLECT_STATS_FOR(RT_AGGREGATOR_NONE, "aggregator_none", sizeof(AggregatorNone))
    COLLECT_STATS_FOR(RT_AGGREGATOR_PT, "aggregator_pt", sizeof(AggregatorPercentile))
    COLLECT_STATS_FOR(RT_AGGREGATOR_SUM, "aggregator_sum", sizeof(AggregatorSum))
    COLLECT_STATS_FOR(RT_AGGREGATOR_TOP, "aggregator_top", sizeof(AggregatorTop))
    COLLECT_STATS_FOR(RT_BITSET_CURSOR, "bitset_cursor", sizeof(BitSetCursor))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V0, "compressor_v0", sizeof(Compressor_v0))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V1, "compressor_v1", sizeof(Compressor_v1))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V2, "compressor_v2", sizeof(Compressor_v2))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V3, "compressor_v3", sizeof(Compressor_v3))
    COLLECT_STATS_FOR(RT_DATA_POINT, "data_point", sizeof(DataPoint))
    COLLECT_STATS_FOR(RT_DATA_POINT_CONTAINER, "data_point_container", sizeof(DataPointContainer))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_AVG, "downsampler_avg", sizeof(DownsamplerAvg))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_COUNT, "downsampler_count", sizeof(DownsamplerCount))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_DEV, "downsampler_dev", sizeof(DownsamplerDev))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_FIRST, "downsampler_first", sizeof(DownsamplerFirst))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_LAST, "downsampler_last", sizeof(DownsamplerLast))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_MAX, "downsampler_max", sizeof(DownsamplerMax))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_MIN, "downsampler_min", sizeof(DownsamplerMin))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_PT, "downsampler_pt", sizeof(DownsamplerPercentile))
    COLLECT_STATS_FOR(RT_DOWNSAMPLER_SUM, "downsampler_sum", sizeof(DownsamplerSum))
    COLLECT_STATS_FOR(RT_HTTP_CONNECTION, "http_connection", sizeof(HttpConnection))
    COLLECT_STATS_FOR(RT_JSON_VALUE, "json_value", sizeof(JsonValue))
    COLLECT_STATS_FOR(RT_KEY_VALUE_PAIR, "key_value_pair", sizeof(KeyValuePair))
    COLLECT_STATS_FOR(RT_QUERY_RESULTS, "query_results", sizeof(QueryResults))
    COLLECT_STATS_FOR(RT_QUERY_TASK, "query_task", sizeof(QueryTask))
    COLLECT_STATS_FOR(RT_RATE_CALCULATOR, "rate_calculator", sizeof(RateCalculator))
    COLLECT_STATS_FOR(RT_TAG_MATCHER, "tag_matcher", sizeof(TagMatcher))
    COLLECT_STATS_FOR(RT_TCP_CONNECTION, "tcp_connection", sizeof(TcpConnection))
    COLLECT_STATS_FOR(RT_COUNT, "network_buffer", m_network_buffer_len)
    COLLECT_STATS_FOR(RT_COUNT+1, "network_buffer_small", m_network_buffer_small_len)

    float total = 0;
    total += m_total[RT_AGGREGATOR_AVG] * sizeof(AggregatorAvg);
    total += m_total[RT_AGGREGATOR_BOTTOM] * sizeof(AggregatorBottom);
    total += m_total[RT_AGGREGATOR_COUNT] * sizeof(AggregatorCount);
    total += m_total[RT_AGGREGATOR_DEV] * sizeof(AggregatorDev);
    total += m_total[RT_AGGREGATOR_MAX] * sizeof(AggregatorMax);
    total += m_total[RT_AGGREGATOR_MIN] * sizeof(AggregatorMin);
    total += m_total[RT_AGGREGATOR_NONE] * sizeof(AggregatorNone);
    total += m_total[RT_AGGREGATOR_PT] * sizeof(AggregatorPercentile);
    total += m_total[RT_AGGREGATOR_SUM] * sizeof(AggregatorSum);
    total += m_total[RT_AGGREGATOR_TOP] * sizeof(AggregatorTop);
    total += m_total[RT_BITSET_CURSOR] * sizeof(BitSetCursor);
    total += m_total[RT_COMPRESSOR_V0] * sizeof(Compressor_v0);
    total += m_total[RT_COMPRESSOR_V1] * sizeof(Compressor_v1);
    total += m_total[RT_COMPRESSOR_V2] * sizeof(Compressor_v2);
    total += m_total[RT_COMPRESSOR_V3] * sizeof(Compressor_v3);
    total += m_total[RT_DATA_POINT] * sizeof(DataPoint);
    total += m_total[RT_DATA_POINT_CONTAINER] * sizeof(DataPointContainer);
    total += m_total[RT_DOWNSAMPLER_AVG] * sizeof(DownsamplerAvg);
    total += m_total[RT_DOWNSAMPLER_COUNT] * sizeof(DownsamplerCount);
    total += m_total[RT_DOWNSAMPLER_DEV] * sizeof(DownsamplerDev);
    total += m_total[RT_DOWNSAMPLER_FIRST] * sizeof(DownsamplerFirst);
    total += m_total[RT_DOWNSAMPLER_LAST] * sizeof(DownsamplerLast);
    total += m_total[RT_DOWNSAMPLER_MAX] * sizeof(DownsamplerMax);
    total += m_total[RT_DOWNSAMPLER_MIN] * sizeof(DownsamplerMin);
    total += m_total[RT_DOWNSAMPLER_PT] * sizeof(DownsamplerPercentile);
    total += m_total[RT_DOWNSAMPLER_SUM] * sizeof(DownsamplerSum);
    total += m_total[RT_HTTP_CONNECTION] * sizeof(HttpConnection);
    total += m_total[RT_JSON_VALUE] * sizeof(JsonValue);
    total += m_total[RT_KEY_VALUE_PAIR] * sizeof(KeyValuePair);
    total += m_total[RT_QUERY_RESULTS] * sizeof(QueryResults);
    total += m_total[RT_QUERY_TASK] * sizeof(QueryTask);
    total += m_total[RT_RATE_CALCULATOR] * sizeof(RateCalculator);
    total += m_total[RT_TAG_MATCHER] * sizeof(TagMatcher);
    total += m_total[RT_TCP_CONNECTION] * sizeof(TcpConnection);
    total += m_total[RT_COUNT] * m_network_buffer_len;
    total += m_total[RT_COUNT+1] * m_network_buffer_small_len;

    dps.emplace_back(ts, total);
    auto& dp = dps.back();
    dp.set_metric("ticktock.mem.reusable.total");
    dp.add_tag(TYPE_TAG_NAME, "all");
    dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
}

void
MemoryManager::log_stats()
{
    std::vector<DataPoint> dps;
    Timestamp ts = ts_now_sec();
    MemoryManager::collect_stats(ts, dps);

    char name[PATH_MAX];
    sprintf(name, "/tmp/tt/log/stat.%" PRIu64 ".log", ts);
    FILE *file = std::fopen(name, "w");

    if (file != nullptr)
    {
        for (DataPoint& dp: dps)
        {
            char buff[dp.c_size()];
            fprintf(file, "%s\n", dp.c_str(buff));
        }

        long ts_cnt = Tsdb::get_ts_count();
        fprintf(file, "ticktock.time_series.count %" PRIu64 " %ld %s=%s\nticktock.time_series.memory %" PRIu64 " %ld %s=%s\n",
            ts, ts_cnt, HOST_TAG_NAME, g_host_name.c_str(),
            ts, ts_cnt*sizeof(TimeSeries), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.tsdb.count %" PRIu64 " %d mode=active %s=%s\n",
            ts, Tsdb::get_active_tsdb_count(), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.tsdb.count %" PRIu64 " %d mode=any %s=%s\n",
            ts, Tsdb::get_total_tsdb_count(), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.data_file.count %" PRIu64 " %d mode=read %s=%s\n",
            ts, Tsdb::get_open_data_file_count(true), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.data_file.count %" PRIu64 " %d mode=write %s=%s\n",
            ts, Tsdb::get_open_data_file_count(false), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.header_file.count %" PRIu64 " %d mode=read %s=%s\n",
            ts, Tsdb::get_open_header_file_count(true), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.header_file.count %" PRIu64 " %d mode=write %s=%s\n",
            ts, Tsdb::get_open_header_file_count(false), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.index_file.count %" PRIu64 " %d mode=read %s=%s\n",
            ts, Tsdb::get_open_index_file_count(true), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.open.index_file.count %" PRIu64 " %d mode=write %s=%s\n",
            ts, Tsdb::get_open_index_file_count(false), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.query.dp.count %" PRIu64 " %" PRIu64 " %s=%s\n",
            ts, Query::get_dp_count(), HOST_TAG_NAME, g_host_name.c_str());

        fprintf(file, "ticktock.connection.count %" PRIu64 " %d %s=%s\n",
            ts, TcpListener::get_active_conn_count(), HOST_TAG_NAME, g_host_name.c_str());

#ifdef TT_STATS
        if (g_query_count.load() > 0)
        {
            fprintf(file, "ticktock.query.latency.avg %" PRIu64 " %f %s=%s\n",
                ts, (double)g_query_latency_ms.load()/(double)g_query_count.load(),
                HOST_TAG_NAME, g_host_name.c_str());
        }
#endif

        size_t buff_size = get_network_buffer_size();
        char *buff = alloc_network_buffer();
        Stats::collect_stats(buff, buff_size);
        fprintf(file, "%s", buff);
        free_network_buffer(buff);

        fclose(file);
    }
}

void
MemoryManager::cleanup()
{
    while (m_free_lists[RecyclableType::RT_AGGREGATOR_AVG] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_AVG];
        m_free_lists[RecyclableType::RT_AGGREGATOR_AVG] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_AVG);
        delete static_cast<AggregatorAvg*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_BOTTOM] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_BOTTOM];
        m_free_lists[RecyclableType::RT_AGGREGATOR_BOTTOM] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_BOTTOM);
        delete static_cast<AggregatorBottom*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_COUNT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_COUNT];
        m_free_lists[RecyclableType::RT_AGGREGATOR_COUNT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_COUNT);
        delete static_cast<AggregatorCount*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_DEV] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_DEV];
        m_free_lists[RecyclableType::RT_AGGREGATOR_DEV] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_DEV);
        delete static_cast<AggregatorDev*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_MAX] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_MAX];
        m_free_lists[RecyclableType::RT_AGGREGATOR_MAX] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_MAX);
        delete static_cast<AggregatorMax*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_MIN] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_MIN];
        m_free_lists[RecyclableType::RT_AGGREGATOR_MIN] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_MIN);
        delete static_cast<AggregatorMin*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_NONE] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_NONE];
        m_free_lists[RecyclableType::RT_AGGREGATOR_NONE] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_NONE);
        delete static_cast<AggregatorNone*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_PT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_PT];
        m_free_lists[RecyclableType::RT_AGGREGATOR_PT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_PT);
        delete static_cast<AggregatorPercentile*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_SUM] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_SUM];
        m_free_lists[RecyclableType::RT_AGGREGATOR_SUM] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_SUM);
        delete static_cast<AggregatorSum*>(r);
    }

    while (m_free_lists[RecyclableType::RT_AGGREGATOR_TOP] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_AGGREGATOR_TOP];
        m_free_lists[RecyclableType::RT_AGGREGATOR_TOP] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_AGGREGATOR_TOP);
        delete static_cast<AggregatorTop*>(r);
    }

    while (m_free_lists[RecyclableType::RT_BITSET_CURSOR] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_BITSET_CURSOR];
        m_free_lists[RecyclableType::RT_BITSET_CURSOR] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_BITSET_CURSOR);
        delete static_cast<BitSetCursor*>(r);
    }

    while (m_free_lists[RecyclableType::RT_COMPRESSOR_V0] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_COMPRESSOR_V0];
        m_free_lists[RecyclableType::RT_COMPRESSOR_V0] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_COMPRESSOR_V0);
        delete static_cast<Compressor_v0*>(r);
    }

    while (m_free_lists[RecyclableType::RT_COMPRESSOR_V1] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_COMPRESSOR_V1];
        m_free_lists[RecyclableType::RT_COMPRESSOR_V1] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_COMPRESSOR_V1);
        delete static_cast<Compressor_v1*>(r);
    }

    while (m_free_lists[RecyclableType::RT_COMPRESSOR_V2] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_COMPRESSOR_V2];
        m_free_lists[RecyclableType::RT_COMPRESSOR_V2] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_COMPRESSOR_V2);
        delete static_cast<Compressor_v2*>(r);
    }

    while (m_free_lists[RecyclableType::RT_COMPRESSOR_V3] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_COMPRESSOR_V3];
        m_free_lists[RecyclableType::RT_COMPRESSOR_V3] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_COMPRESSOR_V3);
        delete static_cast<Compressor_v3*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DATA_POINT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DATA_POINT];
        m_free_lists[RecyclableType::RT_DATA_POINT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DATA_POINT);
        delete static_cast<DataPoint*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DATA_POINT_CONTAINER] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DATA_POINT_CONTAINER];
        m_free_lists[RecyclableType::RT_DATA_POINT_CONTAINER] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DATA_POINT_CONTAINER);
        delete static_cast<DataPointContainer*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_AVG] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_AVG];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_AVG] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_AVG);
        delete static_cast<DownsamplerAvg*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_COUNT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_COUNT];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_COUNT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_COUNT);
        delete static_cast<DownsamplerCount*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_DEV] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_DEV];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_DEV] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_DEV);
        delete static_cast<DownsamplerDev*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_FIRST] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_FIRST];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_FIRST] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_FIRST);
        delete static_cast<DownsamplerFirst*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_LAST] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_LAST];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_LAST] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_LAST);
        delete static_cast<DownsamplerLast*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_MAX] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_MAX];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_MAX] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_MAX);
        delete static_cast<DownsamplerMax*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_MIN] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_MIN];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_MIN] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_MIN);
        delete static_cast<DownsamplerMin*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_PT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_PT];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_PT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_PT);
        delete static_cast<DownsamplerPercentile*>(r);
    }

    while (m_free_lists[RecyclableType::RT_DOWNSAMPLER_SUM] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DOWNSAMPLER_SUM];
        m_free_lists[RecyclableType::RT_DOWNSAMPLER_SUM] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DOWNSAMPLER_SUM);
        delete static_cast<DownsamplerSum*>(r);
    }

    while (m_free_lists[RecyclableType::RT_HTTP_CONNECTION] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_HTTP_CONNECTION];
        m_free_lists[RecyclableType::RT_HTTP_CONNECTION] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_HTTP_CONNECTION);
        delete static_cast<HttpConnection*>(r);
    }

    while (m_free_lists[RecyclableType::RT_JSON_VALUE] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_JSON_VALUE];
        m_free_lists[RecyclableType::RT_JSON_VALUE] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_JSON_VALUE);
        delete static_cast<JsonValue*>(r);
    }

    while (m_free_lists[RecyclableType::RT_KEY_VALUE_PAIR] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_KEY_VALUE_PAIR];
        m_free_lists[RecyclableType::RT_KEY_VALUE_PAIR] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_KEY_VALUE_PAIR);
        delete static_cast<KeyValuePair*>(r);
    }

    while (m_free_lists[RecyclableType::RT_QUERY_RESULTS] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_QUERY_RESULTS];
        m_free_lists[RecyclableType::RT_QUERY_RESULTS] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_QUERY_RESULTS);
        delete static_cast<QueryResults*>(r);
    }

    while (m_free_lists[RecyclableType::RT_QUERY_TASK] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_QUERY_TASK];
        m_free_lists[RecyclableType::RT_QUERY_TASK] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_QUERY_TASK);
        delete static_cast<QueryTask*>(r);
    }

    while (m_free_lists[RecyclableType::RT_RATE_CALCULATOR] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_RATE_CALCULATOR];
        m_free_lists[RecyclableType::RT_RATE_CALCULATOR] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_RATE_CALCULATOR);
        delete static_cast<RateCalculator*>(r);
    }

    while (m_free_lists[RecyclableType::RT_TAG_MATCHER] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_TAG_MATCHER];
        m_free_lists[RecyclableType::RT_TAG_MATCHER] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_TAG_MATCHER);
        delete static_cast<TagMatcher*>(r);
    }

    while (m_free_lists[RecyclableType::RT_TCP_CONNECTION] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_TCP_CONNECTION];
        m_free_lists[RecyclableType::RT_TCP_CONNECTION] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_TCP_CONNECTION);
        delete static_cast<HttpConnection*>(r);
    }

    // free m_network_buffer_free_list
    char* buff = nullptr;

    {
        std::lock_guard<std::mutex> guard(m_network_lock);
        while ((buff = m_network_buffer_free_list) != nullptr)
        {
            m_network_buffer_free_list = *((char**)buff);
            m_free[RecyclableType::RT_COUNT]--;
            std::free(buff);
        }
    }

    {
        std::lock_guard<std::mutex> guard(m_network_small_lock);
        while ((buff = m_network_buffer_small_free_list) != nullptr)
        {
            m_network_buffer_small_free_list = *((char**)buff);
            m_free[RecyclableType::RT_COUNT+1]--;
            std::free(buff);
        }
    }
}

Recyclable *
MemoryManager::alloc_recyclable(RecyclableType type)
{
    ASSERT((0 <= (int)type) && ((int)type < RecyclableType::RT_COUNT));

    Recyclable *r;

    {
        std::lock_guard<std::mutex> guard(m_locks[type]);
        r = m_free_lists[type];

        if (r != nullptr)
        {
            m_free_lists[type] = r->next();
            m_free[type]--;
#ifdef _DEBUG
            m_maps[type][r] = true;
#endif
        }
    }

    if (r == nullptr)
    {
        try
        {
            switch (type)
            {
                case RecyclableType::RT_AGGREGATOR_AVG:
                    r = new AggregatorAvg();
                    break;

                case RecyclableType::RT_AGGREGATOR_BOTTOM:
                    r = new AggregatorBottom();
                    break;

                case RecyclableType::RT_AGGREGATOR_COUNT:
                    r = new AggregatorCount();
                    break;

                case RecyclableType::RT_AGGREGATOR_DEV:
                    r = new AggregatorDev();
                    break;

                case RecyclableType::RT_AGGREGATOR_MAX:
                    r = new AggregatorMax();
                    break;

                case RecyclableType::RT_AGGREGATOR_MIN:
                    r = new AggregatorMin();
                    break;

                case RecyclableType::RT_AGGREGATOR_NONE:
                    r = new AggregatorNone();
                    break;

                case RecyclableType::RT_AGGREGATOR_PT:
                    r = new AggregatorPercentile();
                    break;

                case RecyclableType::RT_AGGREGATOR_SUM:
                    r = new AggregatorSum();
                    break;

                case RecyclableType::RT_BITSET_CURSOR:
                    r = new BitSetCursor();
                    break;

                case RecyclableType::RT_AGGREGATOR_TOP:
                    r = new AggregatorTop();
                    break;

                case RecyclableType::RT_COMPRESSOR_V0:
                case RecyclableType::RT_COMPRESSOR_V1:
                case RecyclableType::RT_COMPRESSOR_V2:
                case RecyclableType::RT_COMPRESSOR_V3:
                    r = Compressor::create(type - RecyclableType::RT_COMPRESSOR_V0);
                    break;

                case RecyclableType::RT_DATA_POINT:
                    r = new DataPoint();
                    break;

                case RecyclableType::RT_DATA_POINT_CONTAINER:
                    r = new DataPointContainer();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_AVG:
                    r = new DownsamplerAvg();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_COUNT:
                    r = new DownsamplerCount();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_DEV:
                    r = new DownsamplerDev();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_FIRST:
                    r = new DownsamplerFirst();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_LAST:
                    r = new DownsamplerLast();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_MAX:
                    r = new DownsamplerMax();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_MIN:
                    r = new DownsamplerMin();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_PT:
                    r = new DownsamplerPercentile();
                    break;

                case RecyclableType::RT_DOWNSAMPLER_SUM:
                    r = new DownsamplerSum();
                    break;

                case RecyclableType::RT_HTTP_CONNECTION:
                    r = new HttpConnection();
                    break;

                case RecyclableType::RT_JSON_VALUE:
                    r = new JsonValue();
                    break;

                case RecyclableType::RT_KEY_VALUE_PAIR:
                    r = new KeyValuePair();
                    break;

                case RecyclableType::RT_QUERY_RESULTS:
                    r = new QueryResults();
                    break;

                case RecyclableType::RT_QUERY_TASK:
                    r = new QueryTask();
                    break;

                case RecyclableType::RT_RATE_CALCULATOR:
                    r = new RateCalculator();
                    break;

                case RecyclableType::RT_TAG_MATCHER:
                    r = new TagMatcher();
                    break;

                case RecyclableType::RT_TCP_CONNECTION:
                    r = new TcpConnection();
                    break;

                default:
                    Logger::error("Unknown recyclable type: %d", type);
                    break;
            }
        }
        catch (std::bad_alloc& ex)
        {
            HttpResponse response;
            Admin::cmd_stop(nullptr, response);    // shutdown
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
        }

        ASSERT(r != nullptr);
        m_total[type]++;

#ifdef _DEBUG
        std::lock_guard<std::mutex> guard(m_locks[type]);
        m_maps[type][r] = true;
#endif
    }

    if (r != nullptr)
    {
        r->init();
        r->recyclable_type() = type;
        r->next() = nullptr;
    }

    return r;
}

void
MemoryManager::free_recyclable(Recyclable *r)
{
    ASSERT(r != nullptr);

    RecyclableType type = r->recyclable_type();
    //std::lock_guard<std::mutex> guard(m_locks[type]);

#ifdef _DEBUG
    {
        std::lock_guard<std::mutex> guard(m_locks[type]);
        auto search = m_maps[type].find(r);

        if (search == m_maps[type].end())
        {
            Logger::fatal("Trying to free recyclable that's not allocated by MM: %p", r);
            return;
        }

        if (! search->second)
        {
            Logger::fatal("Trying to double free recyclable (%d): %p", (int)type, r);
            return;
        }

        m_maps[type][r] = false;
    }
#endif

    if (r->recycle())   // TODO: also check memory pressure
    {
        std::lock_guard<std::mutex> guard(m_locks[type]);
        r->next() = m_free_lists[type];
        m_free_lists[type] = r;
        m_free[type]++;
    }
    else
    {
        delete r;
        m_total[type]--;
#ifdef _DEBUG
        std::lock_guard<std::mutex> guard(m_locks[type]);
        m_maps[type].erase(r);
#endif
    }
}

void
MemoryManager::free_recyclables(Recyclable *rs)
{
    ASSERT(rs != nullptr);

    RecyclableType type = rs->recyclable_type();
    std::lock_guard<std::mutex> guard(m_locks[type]);

    while (rs != nullptr)
    {
        Recyclable *r = rs;
        rs = r->next();

#ifdef _DEBUG
        auto search = m_maps[type].find(r);

        if (search == m_maps[type].end())
        {
            Logger::fatal("Trying to free recyclable that's not allocated by MM: %p", r);
            return;
        }

        if (! search->second)
        {
            Logger::fatal("Trying to double free recyclable (%d): %p", (int)type, r);
            return;
        }

        m_maps[type][r] = false;
#endif

        if (r->recycle())   // TODO: also check memory pressure
        {
            r->next() = m_free_lists[type];
            m_free_lists[type] = r;
            m_free[type]++;
        }
        else
        {
            delete r;
            m_total[type]--;
#ifdef _DEBUG
            m_maps[type].erase(r);
#endif
        }
    }
}

bool
MemoryManager::collect_garbage(TaskData& data)
{
    std::lock_guard<std::mutex> guard(m_garbage_lock);
    bool gc = (data.integer != 0);

    // record usage stats
    for (int i = 0; i < RecyclableType::RT_COUNT+2; i++)
    {
        m_max_usage[i][m_max_usage_idx] =
            m_total[i].load(std::memory_order_relaxed) - m_free[i].load(std::memory_order_relaxed);
        ASSERT(m_max_usage[i][m_max_usage_idx] >= 0);
    }

    if (++m_max_usage_idx >= MAX_USAGE_SIZE)
    {
        gc = true;
        m_max_usage_idx = 0;
    }

    if (gc)
    {
        // perform gc
        for (int i = 0; i < RecyclableType::RT_COUNT; i++)
        {
            int max_usage = 0;

            for (int j = 0; j < MAX_USAGE_SIZE; j++)
                max_usage = std::max(max_usage, m_max_usage[i][j]);

            if (max_usage < m_total[i].load(std::memory_order_relaxed))
            {
                Logger::debug("[gc] Trying to GC of type %d from %d to %d",
                              i,  m_total[i].load(std::memory_order_relaxed), max_usage);

                std::lock_guard<std::mutex> guard(m_locks[i]);

                while (max_usage < m_total[i].load())
                {
                    Recyclable *r = m_free_lists[i];
                    if (r == nullptr) break;
                    m_free_lists[i] = r->next();
                    delete r;
                    m_free[i]--;
                    m_total[i]--;
                }
            }
        }

        // collect network buffers
        {
            int max_usage = 0;

            for (int j = 0; j < MAX_USAGE_SIZE; j++)
                max_usage = std::max(max_usage, m_max_usage[RecyclableType::RT_COUNT][j]);

            if (max_usage < m_total[RecyclableType::RT_COUNT].load(std::memory_order_relaxed))
            {
                Logger::debug("[gc] Trying to GC of network buffer from %d to %d",
                              m_total[RecyclableType::RT_COUNT].load(std::memory_order_relaxed), max_usage);

                std::lock_guard<std::mutex> guard(m_network_lock);

                while (max_usage < m_total[RecyclableType::RT_COUNT].load())
                {
                    char *buff = m_network_buffer_free_list;
                    if (buff == nullptr) break;
                    m_network_buffer_free_list = *((char**)buff);
                    ASSERT(((long)m_network_buffer_free_list % g_page_size) == 0);
                    std::free(buff);
                    m_free[RecyclableType::RT_COUNT]--;
                    m_total[RecyclableType::RT_COUNT]--;
                }
            }
        }

        // collect network buffers (small)
        {
            int max_usage = 0;

            for (int j = 0; j < MAX_USAGE_SIZE; j++)
                max_usage = std::max(max_usage, m_max_usage[RecyclableType::RT_COUNT+1][j]);

            if (max_usage < m_total[RecyclableType::RT_COUNT+1].load(std::memory_order_relaxed))
            {
                Logger::debug("[gc] Trying to GC of network buffer (small) from %d to %d",
                              m_total[RecyclableType::RT_COUNT+1].load(std::memory_order_relaxed), max_usage);

                std::lock_guard<std::mutex> guard(m_network_small_lock);

                while (max_usage < m_total[RecyclableType::RT_COUNT+1].load())
                {
                    char *buff = m_network_buffer_small_free_list;
                    if (buff == nullptr) break;
                    m_network_buffer_small_free_list = *((char**)buff);
                    std::free(buff);
                    m_free[RecyclableType::RT_COUNT+1]--;
                    m_total[RecyclableType::RT_COUNT+1]--;
                }
            }
        }
    }

#ifdef TT_STATS
    log_stats();
#endif

    return false;
}

void
MemoryManager::assert_recyclable(Recyclable *r)
{
#ifdef _DEBUG
    RecyclableType type = r->recyclable_type();
    std::lock_guard<std::mutex> guard(m_locks[type]);
    auto search = m_maps[type].find(r);
    ASSERT((search != m_maps[type].end()) && (search->second));
#endif
}


}
