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
#include "timer.h"


namespace tt
{


bool MemoryManager::m_initialized = false;
uint64_t MemoryManager::m_network_buffer_len = 0;

std::mutex MemoryManager::m_page_lock;
void *MemoryManager::m_page_free_list = nullptr;

std::mutex MemoryManager::m_network_lock;
char *MemoryManager::m_network_buffer_free_list = nullptr;

std::mutex MemoryManager::m_locks[RecyclableType::RT_COUNT];
Recyclable * MemoryManager::m_free_lists[RecyclableType::RT_COUNT];

std::atomic<int> MemoryManager::m_free[RecyclableType::RT_COUNT+1];
std::atomic<int> MemoryManager::m_total[RecyclableType::RT_COUNT+1];

std::mutex MemoryManager::m_garbage_lock;
int MemoryManager::m_max_usage[RecyclableType::RT_COUNT+1][MAX_USAGE_SIZE];
int MemoryManager::m_max_usage_idx;

#ifdef _DEBUG
std::unordered_map<Recyclable*,bool> MemoryManager::m_maps[RecyclableType::RT_COUNT];
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
            throw std::runtime_error("Out of memory");
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

void
MemoryManager::init()
{
    m_network_buffer_len = Config::get_bytes(CFG_TCP_BUFFER_SIZE, CFG_TCP_BUFFER_SIZE_DEF);
    // make sure it's multiple of g_page_size
    m_network_buffer_len = ((long)m_network_buffer_len / g_page_size) * g_page_size;
    Logger::info("mm::m_network_buffer_len = %d", m_network_buffer_len);
    for (int i = 0; i < RecyclableType::RT_COUNT; i++)
        m_free_lists[i] = nullptr;
    for (int i = 0; i <= RecyclableType::RT_COUNT; i++)
        m_free[i] = m_total[i] = 0;
    m_initialized = true;

    m_max_usage_idx = 0;
    for (int i = 0; i <= RecyclableType::RT_COUNT; i++)
    {
        for (int j = 0; j < MAX_USAGE_SIZE; j++)
            m_max_usage[i][j] = 0;
    }

    if (Config::exists(CFG_TSDB_GC_FREQUENCY))
    {
        int freq = (int)Config::get_time(CFG_TSDB_GC_FREQUENCY, TimeUnit::SEC);
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
        }   \
        {   \
            dps.emplace_back(ts, (double)m_free[RTYPE] * (SIZE_OF_RTYPE));    \
            auto& dp = dps.back();  \
            dp.set_metric("ticktock.mem.reusable.free");    \
            dp.add_tag(TYPE_TAG_NAME, RNAME);    \
        }   \
    }

    COLLECT_STATS_FOR(RT_AGGREGATOR_AVG, "aggregator_avg", sizeof(AggregatorAvg))
    COLLECT_STATS_FOR(RT_AGGREGATOR_COUNT, "aggregator_count", sizeof(AggregatorCount))
    COLLECT_STATS_FOR(RT_AGGREGATOR_DEV, "aggregator_dev", sizeof(AggregatorDev))
    COLLECT_STATS_FOR(RT_AGGREGATOR_MAX, "aggregator_max", sizeof(AggregatorMax))
    COLLECT_STATS_FOR(RT_AGGREGATOR_MIN, "aggregator_min", sizeof(AggregatorMin))
    COLLECT_STATS_FOR(RT_AGGREGATOR_NONE, "aggregator_none", sizeof(AggregatorNone))
    COLLECT_STATS_FOR(RT_AGGREGATOR_PT, "aggregator_pt", sizeof(AggregatorPercentile))
    COLLECT_STATS_FOR(RT_AGGREGATOR_SUM, "aggregator_sum", sizeof(AggregatorSum))
    COLLECT_STATS_FOR(RT_BITSET_CURSOR, "bitset_cursor", sizeof(BitSetCursor))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V0, "compressor_v0", sizeof(Compressor_v0))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V1, "compressor_v1", sizeof(Compressor_v1))
    COLLECT_STATS_FOR(RT_COMPRESSOR_V2, "compressor_v2", sizeof(Compressor_v2))
    COLLECT_STATS_FOR(RT_DATA_POINT, "data_point", sizeof(DataPoint))
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
    COLLECT_STATS_FOR(RT_MAPPING, "mapping", sizeof(Mapping))
    COLLECT_STATS_FOR(RT_PAGE_INFO, "page_info", sizeof(PageInfo))
    COLLECT_STATS_FOR(RT_PAGE_INFO_IN_MEM, "page_info_in_mem", (sizeof(PageInfoInMem)+g_page_size))
    COLLECT_STATS_FOR(RT_QUERY_RESULTS, "query_results", sizeof(QueryResults))
    COLLECT_STATS_FOR(RT_QUERY_TASK, "query_task", sizeof(QueryTask))
    COLLECT_STATS_FOR(RT_RATE_CALCULATOR, "rate_calculator", sizeof(RateCalculator))
    COLLECT_STATS_FOR(RT_TCP_CONNECTION, "tcp_connection", sizeof(TcpConnection))
    COLLECT_STATS_FOR(RT_TIME_SERIES, "time_series", sizeof(TimeSeries))
    COLLECT_STATS_FOR(RT_COUNT, "network_buffer", m_network_buffer_len)

    float total = 0;
    total += m_total[RT_AGGREGATOR_AVG] * sizeof(AggregatorAvg);
    total += m_total[RT_AGGREGATOR_COUNT] * sizeof(AggregatorCount);
    total += m_total[RT_AGGREGATOR_DEV] * sizeof(AggregatorDev);
    total += m_total[RT_AGGREGATOR_MAX] * sizeof(AggregatorMax);
    total += m_total[RT_AGGREGATOR_MIN] * sizeof(AggregatorMin);
    total += m_total[RT_AGGREGATOR_NONE] * sizeof(AggregatorNone);
    total += m_total[RT_AGGREGATOR_PT] * sizeof(AggregatorPercentile);
    total += m_total[RT_AGGREGATOR_SUM] * sizeof(AggregatorSum);
    total += m_total[RT_BITSET_CURSOR] * sizeof(BitSetCursor);
    total += m_total[RT_COMPRESSOR_V0] * sizeof(Compressor_v0);
    total += m_total[RT_COMPRESSOR_V1] * sizeof(Compressor_v1);
    total += m_total[RT_COMPRESSOR_V2] * sizeof(Compressor_v2);
    total += m_total[RT_DATA_POINT] * sizeof(DataPoint);
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
    total += m_total[RT_MAPPING] * sizeof(Mapping);
    total += m_total[RT_PAGE_INFO] * sizeof(PageInfo);
    total += m_total[RT_PAGE_INFO_IN_MEM] * (sizeof(PageInfoInMem) + g_page_size);
    total += m_total[RT_QUERY_RESULTS] * sizeof(QueryResults);
    total += m_total[RT_QUERY_TASK] * sizeof(QueryTask);
    total += m_total[RT_RATE_CALCULATOR] * sizeof(RateCalculator);
    total += m_total[RT_TCP_CONNECTION] * sizeof(TcpConnection);
    total += m_total[RT_TIME_SERIES] * sizeof(TimeSeries);
    total += m_total[RT_COUNT] * m_network_buffer_len;

    dps.emplace_back(ts, total);
    auto& dp = dps.back();
    dp.set_metric("ticktock.mem.reusable.total");
    dp.add_tag(TYPE_TAG_NAME, "all");
}

void
MemoryManager::log_stats()
{
    if (Logger::get_level() > LogLevel::DEBUG)
        return;

#ifdef _DEBUG
    Logger::debug("mm::aggregator_avg = %d", m_maps[RecyclableType::RT_AGGREGATOR_AVG].size());
    Logger::debug("mm::aggregator_count = %d", m_maps[RecyclableType::RT_AGGREGATOR_COUNT].size());
    Logger::debug("mm::aggregator_dev = %d", m_maps[RecyclableType::RT_AGGREGATOR_DEV].size());
    Logger::debug("mm::aggregator_max = %d", m_maps[RecyclableType::RT_AGGREGATOR_MAX].size());
    Logger::debug("mm::aggregator_min = %d", m_maps[RecyclableType::RT_AGGREGATOR_MIN].size());
    Logger::debug("mm::aggregator_none = %d", m_maps[RecyclableType::RT_AGGREGATOR_NONE].size());
    Logger::debug("mm::aggregator_pt = %d", m_maps[RecyclableType::RT_AGGREGATOR_PT].size());
    Logger::debug("mm::aggregator_sum = %d", m_maps[RecyclableType::RT_AGGREGATOR_SUM].size());
    Logger::debug("mm::bitset_cursor = %d", m_maps[RecyclableType::RT_BITSET_CURSOR].size());
    Logger::debug("mm::compressor_v0 = %d", m_maps[RecyclableType::RT_COMPRESSOR_V0].size());
    Logger::debug("mm::compressor_v1 = %d", m_maps[RecyclableType::RT_COMPRESSOR_V1].size());
    Logger::debug("mm::compressor_v2 = %d", m_maps[RecyclableType::RT_COMPRESSOR_V2].size());
    Logger::debug("mm::data_point = %d", m_maps[RecyclableType::RT_DATA_POINT].size());
    Logger::debug("mm::downsampler_avg = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_AVG].size());
    Logger::debug("mm::downsampler_count = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_COUNT].size());
    Logger::debug("mm::downsampler_dev = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_DEV].size());
    Logger::debug("mm::downsampler_first = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_FIRST].size());
    Logger::debug("mm::downsampler_last = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_LAST].size());
    Logger::debug("mm::downsampler_max = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_MAX].size());
    Logger::debug("mm::downsampler_min = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_MIN].size());
    Logger::debug("mm::downsampler_pt = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_PT].size());
    Logger::debug("mm::downsampler_sum = %d", m_maps[RecyclableType::RT_DOWNSAMPLER_SUM].size());
    Logger::debug("mm::http_connection = %d", m_maps[RecyclableType::RT_HTTP_CONNECTION].size());
    Logger::debug("mm::json_value = %d", m_maps[RecyclableType::RT_JSON_VALUE].size());
    Logger::debug("mm::key_value_pair = %d", m_maps[RecyclableType::RT_KEY_VALUE_PAIR].size());
    Logger::debug("mm::mapping = %d", m_maps[RecyclableType::RT_MAPPING].size());
    Logger::debug("mm::page_info = %d", m_maps[RecyclableType::RT_PAGE_INFO].size());
    Logger::debug("mm::page_info_in_mem = %d", m_maps[RecyclableType::RT_PAGE_INFO_IN_MEM].size());
    Logger::debug("mm::query_results = %d", m_maps[RecyclableType::RT_QUERY_RESULTS].size());
    Logger::debug("mm::query_task = %d", m_maps[RecyclableType::RT_QUERY_TASK].size());
    Logger::debug("mm::rate_calculator = %d", m_maps[RecyclableType::RT_RATE_CALCULATOR].size());
    Logger::debug("mm::tcp_connection = %d", m_maps[RecyclableType::RT_TCP_CONNECTION].size());
    Logger::debug("mm::time_series = %d", m_maps[RecyclableType::RT_TIME_SERIES].size());

    int count = 0;
    for (void *next = m_page_free_list; next != nullptr; count++)
    {
        next = *(static_cast<void**>(next));
    }
    Logger::debug("mm::page = %d", count);
    Logger::debug("mm::--------");
#endif
}

int
MemoryManager::get_recyclable_total()
{
    int total = 0;

#ifdef _DEBUG
    for (int i = 0; i < (int)RecyclableType::RT_COUNT; i++)
    {
        total += m_maps[i].size();
    }
#endif

    return total;
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

    while (m_free_lists[RecyclableType::RT_DATA_POINT] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_DATA_POINT];
        m_free_lists[RecyclableType::RT_DATA_POINT] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_DATA_POINT);
        delete static_cast<DataPoint*>(r);
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

    while (m_free_lists[RecyclableType::RT_MAPPING] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_MAPPING];
        m_free_lists[RecyclableType::RT_MAPPING] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_MAPPING);
        delete static_cast<Mapping*>(r);
    }

    while (m_free_lists[RecyclableType::RT_PAGE_INFO] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_PAGE_INFO];
        m_free_lists[RecyclableType::RT_PAGE_INFO] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_PAGE_INFO);
        delete static_cast<PageInfo*>(r);
    }

    while (m_free_lists[RecyclableType::RT_PAGE_INFO_IN_MEM] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_PAGE_INFO_IN_MEM];
        m_free_lists[RecyclableType::RT_PAGE_INFO_IN_MEM] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_PAGE_INFO_IN_MEM);
        delete static_cast<PageInfoInMem*>(r);
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

    while (m_free_lists[RecyclableType::RT_TCP_CONNECTION] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_TCP_CONNECTION];
        m_free_lists[RecyclableType::RT_TCP_CONNECTION] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_TCP_CONNECTION);
        delete static_cast<HttpConnection*>(r);
    }

    while (m_free_lists[RecyclableType::RT_TIME_SERIES] != nullptr)
    {
        Recyclable *r = m_free_lists[RecyclableType::RT_TIME_SERIES];
        m_free_lists[RecyclableType::RT_TIME_SERIES] = r->next();
        ASSERT(r->recyclable_type() == RecyclableType::RT_TIME_SERIES);
        delete static_cast<TimeSeries*>(r);
    }

    // TODO: free m_network_buffer_free_list
}

Recyclable *
MemoryManager::alloc_recyclable(RecyclableType type)
{
    ASSERT((0 <= (int)type) && ((int)type < RecyclableType::RT_COUNT));

    std::lock_guard<std::mutex> guard(m_locks[type]);
    Recyclable *r = m_free_lists[type];

    if (r == nullptr)
    {
        try
        {
            switch (type)
            {
                case RecyclableType::RT_AGGREGATOR_AVG:
                    r = new AggregatorAvg();
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

                case RecyclableType::RT_COMPRESSOR_V0:
                case RecyclableType::RT_COMPRESSOR_V1:
                case RecyclableType::RT_COMPRESSOR_V2:
                    r = Compressor::create(type - RecyclableType::RT_COMPRESSOR_V0);
                    break;

                case RecyclableType::RT_DATA_POINT:
                    r = new DataPoint();
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

                case RecyclableType::RT_MAPPING:
                    r = new Mapping();
                    break;

                case RecyclableType::RT_PAGE_INFO:
                    r = new PageInfo();
                    break;

                case RecyclableType::RT_PAGE_INFO_IN_MEM:
                    r = new PageInfoInMem();
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

                case RecyclableType::RT_TCP_CONNECTION:
                    r = new TcpConnection();
                    break;

                case RecyclableType::RT_TIME_SERIES:
                    r = new TimeSeries();
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
            throw std::runtime_error("Out of memory");
        }

        ASSERT(r != nullptr);
        m_total[type]++;
    }
    else
    {
        m_free_lists[type] = r->next();
        m_free[type]--;
    }

#ifdef _DEBUG
    m_maps[type][r] = true;
#endif

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
    std::lock_guard<std::mutex> guard(m_locks[type]);

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
    for (int i = 0; i <= RecyclableType::RT_COUNT; i++)
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
            if (i == RecyclableType::RT_MAPPING)
                continue;   // workaround issue of contention_free_shared_mutex

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
    }

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
