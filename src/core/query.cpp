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

#include <cassert>
#include <chrono>
#include <cstring>
#include "aggregate.h"
#include "config.h"
#include "down.h"
#include "http.h"
#include "json.h"
#include "tsdb.h"
#include "query.h"
#include "meter.h"
#include "logger.h"
#include "memmgr.h"
#include "rate.h"
#include "strbuf.h"
#include "task.h"
#include "utils.h"


namespace tt
{


QueryExecutor *QueryExecutor::m_instance = nullptr;


Query::Query(JsonMap& map, TimeRange& range, StringBuffer& strbuf, bool ms) :
    m_time_range(range),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_rate_calculator(nullptr),
    m_ms(ms),
    TagOwner(false)
{
    auto search = map.find(METRIC_TAG_NAME);
    ASSERT(search != map.end());
    m_metric = search->second->to_string();

    search = map.find("aggregator");
    if (search != map.end())
    {
        m_aggregate = search->second->to_string();
    }
    m_aggregator = Aggregator::create(m_aggregate);

    search = map.find("downsample");
    if (search != map.end())
    {
        m_downsample = search->second->to_string();
    }

    if (! m_ms && (m_downsample == nullptr))
    {
        char buff[64];
        std::strcpy(buff, "1s-");
        std::strcat(buff, (m_aggregate==nullptr)?"none":m_aggregate);
        m_downsample = strbuf.strdup(buff);
    }

    search = map.find("rate");
    if (search != map.end())
    {
        bool rate = search->second->to_bool();

        if (rate)
        {
            bool counter = false;
            bool drop_resets = false;
            uint64_t counter_max = UINT64_MAX;
            uint64_t reset_value = 0;

            search = map.find("rateOptions");
            if (search != map.end())
            {
                JsonMap& m = search->second->to_map();

                search = m.find("counter");
                if (search != m.end())
                {
                    counter = search->second->to_bool();
                }

                search = m.find("dropResets");
                if (search != m.end())
                {
                    drop_resets = search->second->to_bool();
                }

                search = m.find("counterMax");
                if (search != m.end())
                {
                    counter_max = (uint64_t)search->second->to_double();
                }

                search = m.find("resetValue");
                if (search != m.end())
                {
                    reset_value = (uint64_t)search->second->to_double();
                }
            }

            m_rate_calculator =
                (RateCalculator*)MemoryManager::alloc_recyclable(RecyclableType::RT_RATE_CALCULATOR);
            m_rate_calculator->init(counter, drop_resets, counter_max, reset_value);
        }
    }

    search = map.find("tags");
    if (search != map.end())
    {
        JsonMap& m = search->second->to_map();

        for (auto curr = m.begin(); curr != m.end(); curr++)
        {
            const char *name = curr->first;
            const char *value = curr->second->to_string();
            add_tag(name, value);
        }
    }
}

Query::Query(JsonMap& map, StringBuffer& strbuf) :
    m_time_range(0L, 0L),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_rate_calculator(nullptr),
    m_ms(false),
    TagOwner(false)
{
    // TODO: handle bad request (e.g. missing "start");
    //       better way to parse the url params;
    auto search = map.find("start");
    ASSERT(search != map.end());
    Timestamp start = atol(search->second->to_string());
    start = validate_resolution(start);

    search = map.find("end");
    Timestamp end;
    if (search != map.end())
        end = atol(search->second->to_string());
    else
        end = ts_now();
    end = validate_resolution(end);

    m_time_range = TimeRange(start, end);

    search = map.find("msResolution");
    if (search != map.end())
    {
        m_ms = search->second->to_bool();
    }

    search = map.find("m");
    ASSERT(search != map.end());

    char buff[1024];
    bool decode_ok = url_unescape(search->second->to_string(), buff, sizeof(buff));
    ASSERT(decode_ok);

    Logger::debug("after-decoding: %s", buff);

    std::vector<std::string> tokens;

    tokenize(std::string(buff), tokens, ':');
    ASSERT(tokens.size() >= 2);

    int idx = 0;

    m_aggregate = strbuf.strdup(tokens[idx++].c_str());
    m_aggregator = Aggregator::create(m_aggregate);

    m_downsample = strbuf.strdup(tokens[idx++].c_str());

    if (Downsampler::is_downsampler(m_downsample))
    {
        //Logger::debug("downsampler = %s", m_downsample);
        ASSERT(tokens.size() > idx);
        m_metric = strbuf.strdup(tokens[idx++].c_str());
    }
    else if (std::strncmp(m_downsample, "rate{", 5) == 0)
    {
        std::vector<std::string> opts;
        tokenize(std::string(m_downsample+5), opts, ',');

        bool counter = false;
        bool drop_resets = false;
        uint64_t counter_max = UINT64_MAX;
        uint64_t reset_value = 0;

        if ((opts.size() > 0) && (! opts[0].empty()))
        {
            counter = ((opts[0].front() == 't') || (opts[0].front() == 'T'));
        }

        if ((opts.size() > 1) && (! opts[1].empty()))
        {
            counter_max = std::stoull(opts[1]);
        }

        if ((opts.size() > 2) && (! opts[2].empty()))
        {
            reset_value = std::stoull(opts[2]);
        }

        if ((opts.size() > 3) && (! opts[2].empty()))
        {
            drop_resets = ((opts[2].front() == 't') || (opts[2].front() == 'T'));
        }

        m_rate_calculator =
            (RateCalculator*)MemoryManager::alloc_recyclable(RecyclableType::RT_RATE_CALCULATOR);
        m_rate_calculator->init(counter, drop_resets, counter_max, reset_value);

        ASSERT(tokens.size() > idx);
        m_downsample = strbuf.strdup(tokens[idx++].c_str());
    }
    else if (std::strncmp(m_downsample, "rate", 4) == 0)
    {
        bool counter = false;
        bool drop_resets = false;
        uint64_t counter_max = UINT64_MAX;
        uint64_t reset_value = 0;

        m_rate_calculator =
            (RateCalculator*)MemoryManager::alloc_recyclable(RecyclableType::RT_RATE_CALCULATOR);
        m_rate_calculator->init(counter, drop_resets, counter_max, reset_value);

        ASSERT(tokens.size() > idx);
        m_downsample = strbuf.strdup(tokens[idx++].c_str());
    }
    else
    {
        //Logger::debug("no downsampler found, metric: %s", m_downsample);
        m_metric = m_downsample;
        m_downsample = nullptr;
    }

    if ((m_downsample != nullptr) && ! Downsampler::is_downsampler(m_downsample))
    {
        m_metric = m_downsample;
        m_downsample = nullptr;
    }
    else if (m_metric == nullptr)
    {
        ASSERT(tokens.size() > idx);
        m_metric = strbuf.strdup(tokens[idx++].c_str());
    }

    if (! m_ms && (m_downsample == nullptr))
    {
        char buff[64];
        std::strcpy(buff, "1s-");
        std::strcat(buff, m_aggregate);
        m_downsample = strbuf.strdup(buff);
    }

    char *tag = std::strchr((char*)m_metric, '{');

    if (tag != nullptr)
    {
        JsonMap m;

        if (std::strchr(tag+1, '"') == nullptr)
        {
            JsonParser::parse_map_unquoted(tag, m, '=');
        }
        else
        {
            JsonParser::parse_map(tag, m, '=');
        }

        *tag = 0;

        //Logger::debug("metric: %s", m_metric);

        for (auto it = m.begin(); it != m.end(); it++)
        {
            //Logger::debug("tag: %s, %s", (const char*)it->first, (const char*)it->second);
            add_tag(strbuf.strdup((const char*)it->first), strbuf.strdup(it->second->to_string()));
        }

        JsonParser::free_map(m);
    }

    Logger::debug("query: %s", c_str(buff, sizeof(buff)));
}

Query::~Query()
{
    if (m_aggregator != nullptr)
    {
        MemoryManager::free_recyclable(m_aggregator);
        m_aggregator = nullptr;
    }

    if (m_rate_calculator != nullptr)
    {
        MemoryManager::free_recyclable(m_rate_calculator);
        m_rate_calculator = nullptr;
    }
}

int
Query::add_data_point(DataPointPair& dp, DataPointVector& dps, Downsampler *downsampler)
{
    if (in_range(dp.first))
    {
        if (downsampler != nullptr)
        {
            downsampler->add_data_point(dp, dps);
        }
        else
        {
            dps.push_back(dp);
        }

        return 0;
    }
    else
    {
        return (dp.first < m_time_range.get_from()) ? -1 : 1;
    }
}

void
Query::get_query_tasks(std::vector<QueryTask*>& qtv)
{
    ASSERT(qtv.empty());
    std::vector<Tsdb*> tsdbs;

    Tsdb::insts(m_time_range, tsdbs);

    char buff[64];
    Logger::debug("Found %d tsdbs within %s", tsdbs.size(), m_time_range.c_str(buff, sizeof(buff)));

    std::unordered_map<const char*,QueryTask*,hash_func,eq_func> map;

    for (Tsdb *tsdb: tsdbs)
    {
        tsdb->ensure_readable();    // TODO: take ReadLock of tsdb to prevent from unload

        std::unordered_set<TimeSeries*> v;  // TODO: will tsl::robin_set be faster?
        tsdb->query_for_ts(m_metric, m_tags, v);

        Logger::debug("there are %d ts in %s matching %s and tags",
            v.size(), tsdb->c_str(buff, sizeof(buff)), m_metric);

        for (TimeSeries *ts: v)
        {
            //char buff[1024];
            auto search = map.find(ts->get_key());

            if (search == map.end())
            {
                QueryTask *qt =
                    (QueryTask*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_TASK);

                qt->m_time_range = m_time_range;
                //ASSERT(m_downsample != nullptr);
                qt->m_downsampler = (m_downsample == nullptr) ?
                                    nullptr :
                                    Downsampler::create(m_downsample, m_time_range, m_ms);
                qt->m_tsv.push_back(ts);
                qt->m_done.store(false, std::memory_order_relaxed);

                map.emplace(ts->get_key(), qt);
            }
            else
            {
                QueryTask *qt = search->second;
                qt->m_tsv.push_back(ts);
            }
        }
    }

    for (const auto& kv: map)
    {
        qtv.push_back(kv.second);
    }

    Logger::debug("Got %d query tasks", qtv.size());
}

void
Query::aggregate(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    ASSERT(m_aggregator != nullptr);

    if (m_aggregator->is_none())
    {
        // no aggregation
        for (QueryTask *qt: qtv)
        {
            QueryResults *result =
                (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
            result->m_metric = m_metric;
            result->set_tags(qt->get_cloned_tags(strbuf));
            results.push_back(result);

            DataPointVector& dps = qt->get_dps();
            result->m_dps.insert(result->m_dps.end(), dps.begin(), dps.end());  // TODO: how to avoid copy?
            dps.clear();
        }
    }
    else
    {
        // split tsv into results
        create_query_results(qtv, results, strbuf);

        // aggregate results
        for (QueryResults* result: results)
        {
            // merge dps from dpsv[] into result->dps
            //merge(dpsv, result->dps);
            //result->m_ts_to_dps.clear();
            m_aggregator->aggregate(result);
        }
    }
}

void
Query::calculate_rate(std::vector<QueryResults*>& results)
{
    if (m_rate_calculator != nullptr)
    {
        for (QueryResults *result: results)
        {
            m_rate_calculator->calculate(result->m_dps);
        }
    }
/*
    else
    {
        // OpenTSDB's behavior is that without rate, empty results are removed
        for (auto it = results.begin(); it != results.end(); )
        {
            if ((*it)->empty())
            {
                results.erase(it);
            }
            else
            {
                it++;
            }
        }
    }
*/
}

void
Query::create_query_results(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::vector<const char*> star_tags;

    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        //if (std::strcmp(tag->m_value, "*") == 0)
        if (ends_with(tag->m_value, "*"))
        {
            star_tags.push_back(tag->m_key);
        }
    }

    Logger::debug("There are %d star'ed tags", star_tags.size());

    if (star_tags.empty())
    {
        // in this case there can be only one QueryResults
        QueryResults *result =
            (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);

        result->m_metric = m_metric;
        result->set_tags(get_cloned_tags(strbuf));

        for (QueryTask *qt: qtv)
        {
            result->add_query_task(qt, strbuf);
        }

        results.push_back(result);
    }
    else
    {
        // in this case there could be many QueryResults
        for (QueryTask *qt: qtv)
        {
            // find the existing QueryResults ts belongs to, if any
            QueryResults *result = nullptr;

            for (QueryResults *r: results)
            {
                bool match = true;

                for (Tag *tag = r->get_tags(); tag != nullptr; tag = tag->next())
                {
                    // skip those tags that are not queried
                    if (find_by_key(tag->m_key) == nullptr) continue;

                    if (! Tag::match_value(qt->get_tags(), tag->m_key, tag->m_value))
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    result = r;
                    break;
                }
            }

            if (result == nullptr)
            {
                // did not find the matching QueryResults, create one
                result =
                    (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
                result->m_metric = m_metric;
                result->set_tags(get_cloned_tags(strbuf));
                result->add_query_task(qt, strbuf);

                results.push_back(result);
            }
            else
            {
                result->add_query_task(qt, strbuf);
            }
        }
    }

    Logger::debug("created %d QueryResults", results.size());
}

void
Query::execute(std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::vector<QueryTask*> qtv;

    get_query_tasks(qtv);

    for (QueryTask *qt: qtv)
    {
        qt->perform();
    }

    aggregate(qtv, results, strbuf);
    calculate_rate(results);

    // cleanup
    for (QueryTask *qt: qtv)
    {
        MemoryManager::free_recyclable(qt);
    }

    // The following code are for debugging purposes only.
#ifdef _DEBUG
    int n = 0, c = 0;

    for (QueryResults *qr: results)
    {
        c++;
        n += qr->m_dps.size();
    }

    char buff[64];
    Logger::debug("Finished with %d ts, %d qr and %d dps in range %s", qtv.size(), c, n, m_time_range.c_str(buff, sizeof(buff)));
#endif
}

// perform query by submitting task to QueryExecutor
void
Query::execute_in_parallel(std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::vector<QueryTask*> qtv;
    QueryExecutor *executor = QueryExecutor::inst();

    get_query_tasks(qtv);

    {
        std::lock_guard<std::mutex> guard(executor->m_lock);

        for (QueryTask *qt: qtv)
        {
            ASSERT(! qt->m_tsv.empty());
            executor->submit_query(qt);
        }
    }

    unsigned int progress = SPIN_YIELD_THRESHOLD;
    std::vector<QueryTask*> done_qtv;

    while (! qtv.empty())
    {
        if (progress > 0)
        {
            if (progress < (6 * SPIN_YIELD_THRESHOLD)) progress += SPIN_YIELD_THRESHOLD;
            spin_yield(progress);   // yield for 1ms
        }
        else
        {
            spin_yield(2 * SPIN_YIELD_THRESHOLD);   // yield for 1ms
        }

        for (auto it = qtv.begin(); it != qtv.end(); )
        {
            QueryTask *task = *it;

            ASSERT(! task->m_tsv.empty());

            if (task->m_done)
            {
                // process results
                //QueryResults *result = find_or_create_query_results(task->m_ts, results);
                //result->dps.insert(result->dps.end(), task->m_results.dps.begin(), task->m_results.dps.end());

                progress = 0;
                it = qtv.erase(it);
                done_qtv.push_back(task);
            }
            else
            {
                it++;
            }
        }
    }

    {
        Meter meter(METRIC_TICKTOCK_QUERY_AGGREGATE_LATENCY_MS);
        Logger::trace("calling aggregate()...");
        aggregate(done_qtv, results, strbuf);
        Logger::trace("calling calculate_rate()...");
        calculate_rate(results);
    }

    // cleanup
    Logger::trace("cleanup...");
    for (QueryTask *qt: done_qtv)
    {
        MemoryManager::free_recyclable(qt);
    }

    // The following code are for debugging purposes only.
#ifdef _DEBUG
    int n = 0;

    for (QueryResults *qr: results)
    {
        n += qr->m_dps.size();
    }

    char buff[64];
    Logger::debug("Finished with %d ts and %d dps in range %s", done_qtv.size(), n, m_time_range.c_str(buff, sizeof(buff)));
#endif
}

char *
Query::c_str(char *buff, size_t size) const
{
    ASSERT(buff != nullptr);

    char buf[64];
    int n = snprintf(buff, size, "metric=%s agg=%s down=%s range=%s ms=%s",
        m_metric, m_aggregate, m_downsample, m_time_range.c_str(buf, sizeof(buf)),
        m_ms ? "true" : "false");

    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        n += snprintf(&buff[n], size-n, " %s=%s", tag->m_key, tag->m_value);
    }

    return buff;
}


void
QueryTask::perform()
{
    for (TimeSeries *ts: m_tsv)
    {
        ts->query(m_time_range, m_downsampler, m_dps);
    }

    if (m_downsampler != nullptr)
    {
        m_downsampler->fill_if_needed(m_dps);
        MemoryManager::free_recyclable(m_downsampler);
        m_downsampler = nullptr;
    }
}

Tag *
QueryTask::get_tags()
{
    ASSERT(! m_tsv.empty());
    return m_tsv.front()->get_tags();
}

Tag *
QueryTask::get_cloned_tags(StringBuffer& strbuf)
{
    ASSERT(! m_tsv.empty());
    Tag *tags = m_tsv.front()->get_cloned_tags(strbuf);
    Tag *removed = Tag::remove_first(&tags, METRIC_TAG_NAME);
    Tag::free_list(removed, false);
    return tags;
}

bool
QueryTask::recycle()
{
    m_tsv.clear();
    m_tsv.shrink_to_fit();
    m_dps.clear();
    m_dps.shrink_to_fit();
    m_results.recycle();

    if (m_downsampler != nullptr)
    {
        MemoryManager::free_recyclable(m_downsampler);
        m_downsampler = nullptr;
    }

    return true;
}


QueryExecutor::QueryExecutor() :
    m_executors("qexe",
                Config::get_int(CFG_QUERY_EXECUTOR_THREAD_COUNT,CFG_QUERY_EXECUTOR_THREAD_COUNT_DEF),
                Config::get_int(CFG_QUERY_EXECUTOR_QUEUE_SIZE,CFG_QUERY_EXECUTOR_QUEUE_SIZE_DEF))
{
}

void
QueryExecutor::init()
{
    m_instance = new QueryExecutor;
}

bool
QueryExecutor::http_get_api_query_handler(HttpRequest& request, HttpResponse& response)
{
    Meter meter(METRIC_TICKTOCK_QUERY_LATENCY_MS);

#ifdef _DEBUG
    {
        char *buff = MemoryManager::alloc_network_buffer();
        size_t size = MemoryManager::get_network_buffer_size() - 1;
        Logger::debug("Handling get request: %s", request.c_str(buff, size));
        MemoryManager::free_network_buffer(buff);
    }
#endif

    JsonMap params;
    request.parse_params(params);

    StringBuffer strbuf;
    Query query(params, strbuf);
    std::vector<QueryResults*> results;

    if (Config::get_bool(CFG_QUERY_EXECUTOR_PARALLEL,CFG_QUERY_EXECUTOR_PARALLEL_DEF))
    {
        query.execute_in_parallel(results, strbuf);
    }
    else
    {
        query.execute(results, strbuf);
    }

    JsonParser::free_map(params);

    bool status = prepare_response(results, response);

    for (QueryResults *r: results)
        MemoryManager::free_recyclable(r);

    return status;
}

bool
QueryExecutor::http_post_api_query_handler(HttpRequest& request, HttpResponse& response)
{
    Meter meter(METRIC_TICKTOCK_QUERY_LATENCY_MS);
    bool ms = false;
    JsonMap map;

#ifdef _DEBUG
    {
        char *buff = MemoryManager::alloc_network_buffer();
        size_t size = MemoryManager::get_network_buffer_size() - 1;
        Logger::debug("Handling post request: %s", request.c_str(buff, size));
        MemoryManager::free_network_buffer(buff);
    }
#endif

    JsonParser::parse_map(request.content, map);
    //JsonMap *map = static_cast<JsonMap*>(JsonParser::from_json(request.content));

    auto search = map.find("start");
    if (search == map.end())
        return false;   // will send '400 bad request' back

    Timestamp start = (Timestamp)(search->second->to_double());
    start = validate_resolution(start);

    Timestamp end;
    search = map.find("end");
    if (search != map.end())
        end = (long)(search->second->to_double());
    else
        end = ts_now();
    end = validate_resolution(end);

    search = map.find("msResolution");
    if (search != map.end())
    {
        ms = search->second->to_bool();
    }

    search = map.find("queries");
    ASSERT(search != map.end());
    JsonArray& array = search->second->to_array();

    StringBuffer strbuf;
    std::vector<QueryResults*> results;

    for (int i = 0; i < array.size(); i++)
    {
        JsonMap& m = array[i]->to_map();
        TimeRange range(start, end);
        Query query(m, range, strbuf, ms);

        char tmp[1024];
        Logger::debug("query: %s", query.c_str(tmp, sizeof(tmp)-1));

        std::vector<QueryResults*> res;

        if (Config::get_bool(CFG_QUERY_EXECUTOR_PARALLEL,CFG_QUERY_EXECUTOR_PARALLEL_DEF))
        {
            query.execute_in_parallel(res, strbuf);
        }
        else
        {
            query.execute(res, strbuf);
        }

        if (! res.empty())
        {
            results.insert(results.end(), res.begin(), res.end());
        }
    }

    JsonParser::free_map(map);

    bool status = prepare_response(results, response);

    for (QueryResults *r: results)
        MemoryManager::free_recyclable(r);

    return status;
}

bool
QueryExecutor::prepare_response(std::vector<QueryResults*>& results, HttpResponse& response)
{
    char *buff = response.get_buffer();
    int size = response.get_buffer_size();

    buff[0] = '[';
    buff[1] = 0;

    int n = 1;
    bool status = true;

    for (QueryResults *r: results)
    {
        if (r->empty()) continue;

        if (*(buff+n-1) != '[')
        {
            n += snprintf(buff+n, size-n, ",");
        }

        n += r->to_json(buff+n, size-n);
        if (n >= size) break;
    }

    if (n >= size)
    {
        Logger::error("response too large, %d >= %d", n, size);
        response.init(413, HttpContentType::PLAIN, 0, nullptr);
        status = false;
    }
    else
    {
        buff[n++] = ']';
        buff[n] = 0;

        ASSERT(std::strchr(buff, ' ') == nullptr);
        ASSERT(std::strlen(buff) == n);
        response.init(200, HttpContentType::JSON, n);
    }

#ifdef _DEBUG
    {
        char *buf = MemoryManager::alloc_network_buffer();
        size_t size = MemoryManager::get_network_buffer_size() - 1;
        Logger::debug("response: %s", response.c_str(buf, size));
        MemoryManager::free_network_buffer(buf);
    }
#endif

    return status;
}

void
QueryExecutor::submit_query(QueryTask *query_task)
{
    Task task;

    task.doit = QueryExecutor::perform_query;
    task.data.pointer = query_task;

    m_executors.submit_task(task);
}

bool
QueryExecutor::perform_query(TaskData& data)
{
    QueryTask *task = (QueryTask*)data.pointer;
    task->perform();
    task->m_done = true;
    return false;
}

void
QueryExecutor::shutdown(ShutdownRequest request)
{
    std::lock_guard<std::mutex> guard(m_lock);

    Stoppable::shutdown(request);
    m_executors.shutdown(request);
    m_executors.wait(5);
}


void
QueryResults::add_query_task(QueryTask *qt, StringBuffer& strbuf)
{
    ASSERT(qt != nullptr);

    for (Tag *tag = qt->get_tags(); tag != nullptr; tag = tag->next())
    {
        if (std::strcmp(tag->m_key, METRIC_TAG_NAME) == 0) continue;

        Tag *match = find_by_key(tag->m_key);

        if (match == nullptr)
        {
            // see if it's in aggregate_tags[]
            bool found = false;

            for (const char *t: m_aggregate_tags)
            {
                if (std::strcmp(t, tag->m_key) == 0)
                {
                    found = true;
                    break;
                }
            }

            if (! found)
            {
                add_tag(strbuf.strdup(tag->m_key), strbuf.strdup(tag->m_value));
            }
        }
        else if (ends_with(match->m_value, "*"))
        {
            // move it from tags to aggregate_tags
            remove_tag(match->m_key);
            add_tag(strbuf.strdup(tag->m_key), strbuf.strdup(tag->m_value));
        }
        else if (std::strcmp(match->m_value, tag->m_value) != 0)
        {
            // move it from tags to aggregate_tags
            remove_tag(match->m_key);
            add_aggregate_tag(strbuf.strdup(tag->m_key));
            //m_aggregate_tags.push_back(tag->m_key);
        }
    }

    m_qtv.push_back(qt);
}

// returns supported filters, for example:
// {
//   "iliteral_or": {
//     "examples": "host=iliteral_or(web01), host=iliteral_or(web01|web02|web03) {\"type\":\"iliteral_or\",\"tagk\":\"host\",\"filter\":\"web01|web02|web03\",\"groupBy\":false}",
//     "description": "Accepts one or more exact values and matches if the series contains any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case insensitive and will not allow characters that TSDB does not allow at write time."
//   },
//   "wildcard": {
//     "examples": "host=wildcard(web*), host=wildcard(web*.tsdb.net) {\"type\":\"wildcard\",\"tagk\":\"host\",\"filter\":\"web*.tsdb.net\",\"groupBy\":false}",
//     "description": "Performs pre, post and in-fix glob matching of values. The globs are case sensitive and multiple wildcards can be used. The wildcard character is the * (asterisk). At least one wildcard must be present in the filter value. A wildcard by itself can be used as well to match on any value for the tag key."
//   }
// }
bool
QueryExecutor::http_get_api_config_filters_handler(HttpRequest& request, HttpResponse& response)
{
    // right now we do not support any filters
    response.init(200, HttpContentType::JSON, 2, "{}");
    return true;
}


}
