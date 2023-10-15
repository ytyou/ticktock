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
#include <chrono>
#include <cstring>
#include "aggregate.h"
#include "compress.h"
#include "config.h"
#include "down.h"
#include "http.h"
#include "json.h"
#include "limit.h"
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


#ifdef TT_STATS
#ifdef __x86_64__
static std::atomic<uint64_t> s_dp_count {0};
#else
static std::atomic<unsigned long> s_dp_count {0};
#endif
#endif
//QueryExecutor *QueryExecutor::m_instance = nullptr;


Query::Query(JsonMap& map, TimeRange& range, StringBuffer& strbuf, bool ms) :
    m_time_range(range),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_rate_calculator(nullptr),
    m_ms(ms),
    m_explicit_tags(false),
    m_non_grouping_tags(nullptr),
    m_errno(0),
    TagOwner(false)
{
    auto search = map.find(METRIC_TAG_NAME);
    if (search == map.end())
        throw std::runtime_error("Must specify metric name when query.");
    m_metric = search->second->to_string();

    search = map.find("aggregator");
    if (search != map.end())
        m_aggregate = search->second->to_string();
    m_aggregator = Aggregator::create(m_aggregate);

    search = map.find("downsample");
    if (search != map.end())
        m_downsample = search->second->to_string();

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

/* Syntax:
 *  m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]}]:][<down_sampler>:][percentiles\[<p1>, <pn>\]:][explicit_tags:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
 */
Query::Query(JsonMap& map, StringBuffer& strbuf) :
    m_time_range(0L, 0L),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_rate_calculator(nullptr),
    m_ms(false),
    m_explicit_tags(false),
    m_non_grouping_tags(nullptr),
    m_errno(0),
    TagOwner(false)
{
    Timestamp now = ts_now();
    auto search = map.find("start");
    if (search == map.end())
        throw std::runtime_error("Must specify start time when query.");
    Timestamp start = parse_ts(search->second, now);
    start = validate_resolution(start);

    search = map.find("end");
    Timestamp end;
    if (search != map.end())
        end = parse_ts(search->second, now);
    else
        end = now;
    end = validate_resolution(end);

    m_time_range = TimeRange(start, end);

    search = map.find("msResolution");
    if (search != map.end())
    {
        m_ms = search->second->to_bool();
    }

    search = map.find("m");
    if (search == map.end())
        throw std::runtime_error("Must specify m parameter when query.");

    char buff[1024];
    bool decode_ok = url_unescape(search->second->to_string(), buff, sizeof(buff));
    if (! decode_ok)
        throw std::runtime_error("Failed to URL decode query.");

    Logger::debug("after-decoding: %s", buff);

    std::vector<std::string> tokens;

    tokenize(std::string(buff), tokens, ':');
    if (tokens.size() < 2)
        throw std::runtime_error(std::string("Failed to parse query: ") + buff);

    int idx = 0;

    m_aggregate = strbuf.strdup(tokens[idx++].c_str());
    m_aggregator = Aggregator::create(m_aggregate);

    while (idx < (tokens.size()-1))
    {
        char *token = strbuf.strdup(tokens[idx++].c_str());

        if (std::strncmp(token, "rate{", 5) == 0)
        {
            std::vector<std::string> opts;
            tokenize(std::string(token+5), opts, ',');

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
        }
        else if (std::strncmp(token, "rate", 4) == 0)
        {
            bool counter = false;
            bool drop_resets = false;
            uint64_t counter_max = UINT64_MAX;
            uint64_t reset_value = 0;

            m_rate_calculator =
                (RateCalculator*)MemoryManager::alloc_recyclable(RecyclableType::RT_RATE_CALCULATOR);
            m_rate_calculator->init(counter, drop_resets, counter_max, reset_value);
        }
        else if (std::strncmp(token, "percentiles[", 12) == 0)
        {
            Logger::warn("percentiles in query param not supported");
        }
        else if (std::strcmp(token, "explicit_tags") == 0)
        {
            m_explicit_tags = true;
        }
        else    // it's downsampler
        {
            m_downsample = token;
        }
    }

    ASSERT(idx == (tokens.size()-1));
    m_metric = strbuf.strdup(tokens[idx++].c_str());

/*
    if (! m_ms && (m_downsample == nullptr))
    {
        char buff[64];
        std::strcpy(buff, "1s-");
        std::strcat(buff, m_aggregate);
        m_downsample = strbuf.strdup(buff);
    }
*/

    char *tag = std::strchr((char*)m_metric, '{');

    if (tag != nullptr)
    {
        JsonMap m;
        char *curr;

        if (std::strchr(tag+1, '"') == nullptr)
            curr = JsonParser::parse_map_unquoted(tag, m, '=');
        else
            curr = JsonParser::parse_map(tag, m, '=');

        *tag = 0;
        tag = std::strchr(curr, '{');

        for (auto it = m.begin(); it != m.end(); it++)
            add_tag(strbuf.strdup((const char*)it->first), strbuf.strdup(it->second->to_string()));

        JsonParser::free_map(m);
    }

    // non-grouping tags?
    if (tag != nullptr)
    {
        JsonMap m;

        if (std::strchr(tag+1, '"') == nullptr)
            JsonParser::parse_map_unquoted(tag, m, '=');
        else
            JsonParser::parse_map(tag, m, '=');

        for (auto it = m.begin(); it != m.end(); it++)
        {
            char *key = strbuf.strdup((const char*)it->first);
            char *value = strbuf.strdup(it->second->to_string());

            add_tag(key, value);
            TagOwner::add_tag(&m_non_grouping_tags, key, value);
        }

        JsonParser::free_map(m);
    }

    Logger::debug("query: %T", this);
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
    int n = in_range(dp.first);

    if (n == 0)
    {
        if (downsampler != nullptr)
            downsampler->add_data_point(dp, dps);
        else
            dps.push_back(dp);
    }

    return n;
}

void
Query::get_query_tasks(QuerySuperTask& super_task)
{
    //ASSERT(qtv.empty());
    //ASSERT(tsdbs != nullptr);

    //Tsdb::insts(m_time_range, *tsdbs);
    //Logger::debug("Found %d tsdbs within %T", tsdbs->size(), &m_time_range);

    std::unordered_set<TimeSeries*> tsv;
    char buff[MAX_TOTAL_TAG_LENGTH];
    get_ordered_tags(buff, sizeof(buff));
    MetricId mid = Tsdb::query_for_ts(m_metric, m_tags, tsv, buff, m_explicit_tags);

    for (TimeSeries *ts: tsv)
        super_task.add_task(ts);
    super_task.set_metric_id(mid);
}

void
Query::aggregate(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    ASSERT(m_aggregator != nullptr);

    if (m_aggregator->is_none())
    {
        m_aggregator->aggregate(m_metric, qtv, results, strbuf);
#if 0
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
#endif
    }
    else
    {
        // split qtv into results
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

QueryResults *
Query::create_one_query_results(StringBuffer& strbuf)
{
    QueryResults *result =
        (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
    result->m_metric = m_metric;
    result->set_tags(get_cloned_tags(strbuf));
    return result;
}

void
Query::create_query_results(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    bool star_tags = false;

    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        if (ends_with(tag->m_value, '*') || (std::strchr(tag->m_value, '|') != nullptr))
        {
            star_tags = true;
            break;
        }
    }

    Logger::debug("There are star'ed or multiple-choice tags");

    if (! star_tags)
    {
        // in this case there can be only one QueryResults
        QueryResults *result = create_one_query_results(strbuf);

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
            Tag_v2& qt_tags = qt->get_v2_tags();

            for (QueryResults *r: results)
            {
                bool match = true;

                for (Tag *tag = r->get_tags(); tag != nullptr; tag = tag->next())
                {
                    // skip those tags that are not queried
                    if (find_by_key(tag->m_key) == nullptr) continue;

                    // skip non-grouping tags
                    if (TagOwner::find_by_key(m_non_grouping_tags, tag->m_key) != nullptr) continue;

                    //if (! Tag::match_value(qt->get_tags(), tag->m_key, tag->m_value))
                    if (! qt_tags.match(tag->m_key, tag->m_value))
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
                result = create_one_query_results(strbuf);
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
    //std::vector<QueryTask*> qtv;
    //std::vector<Tsdb*> tsdbs;
    QuerySuperTask super_task(m_time_range, m_downsample, m_ms);

    get_query_tasks(super_task);
    super_task.perform();

    //for (QueryTask *qt: qtv)
        //qt->perform();

    aggregate(super_task.get_tasks(), results, strbuf);
    calculate_rate(results);

    m_errno = super_task.get_errno();
/*
    // cleanup
    for (QueryTask *qt: qtv)
    {
        if (qt->get_errno() != 0)
            m_errno = qt->get_errno();
        MemoryManager::free_recyclable(qt);
    }

    for (Tsdb *tsdb: tsdbs)
        tsdb->dec_ref_count();
*/
}

#if 0

// perform query by submitting task to QueryExecutor
void
Query::execute_in_parallel(std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::vector<QueryTask*> qtv;
    std::vector<Tsdb*> tsdbs;
    QueryExecutor *executor = QueryExecutor::inst();

    get_query_tasks(qtv, &tsdbs);

    if (qtv.size() > 1) // TODO: config?
    {
        int size = qtv.size() - 1;
        CountingSignal signal(size);

        {
            std::lock_guard<std::mutex> guard(executor->m_lock);

            for (int i = 0; i < size; i++)
            {
                QueryTask *task = qtv[i];
                //ASSERT(! task->m_tsv.empty());
                task->set_signal(&signal);
                executor->submit_query(task);
            }
        }

        qtv[size]->perform();
        signal.wait(false);
    }
    else
    {
        for (QueryTask *qt: qtv)
            qt->perform();
    }

    {
        Meter meter(METRIC_TICKTOCK_QUERY_AGGREGATE_LATENCY_MS);
        Logger::trace("calling aggregate()...");
        aggregate(qtv, results, strbuf);
        Logger::trace("calling calculate_rate()...");
        calculate_rate(results);
    }

    // cleanup
    Logger::trace("cleanup...");
    for (QueryTask *qt: qtv)
    {
        if (qt->get_errno() != 0)
            m_errno = qt->get_errno();
        MemoryManager::free_recyclable(qt);
    }

    for (Tsdb *tsdb: tsdbs)
        tsdb->dec_ref_count();
}

#endif

uint64_t
Query::get_dp_count()
{
    uint64_t cnt = 0;
#ifdef TT_STATS
    cnt = s_dp_count.load(std::memory_order_relaxed);
#endif
    return cnt;
}

const char *
Query::c_str(char *buff) const
{
    ASSERT(buff != nullptr);
    char buf[m_time_range.c_size()];
    size_t size = c_size();

    int n = snprintf(buff, size, "metric=%s agg=%s down=%s range=%s ms=%s",
        m_metric, m_aggregate, m_downsample, m_time_range.c_str(buf),
        m_ms ? "true" : "false");

    for (Tag *tag = m_tags; (tag != nullptr) && (size > n); tag = tag->next())
        n += snprintf(&buff[n], size-n, " %s=%s", tag->m_key, tag->m_value);

    return buff;
}


QueryTask::QueryTask()
{
    init();
}

void
QueryTask::query_ts_data(Tsdb *tsdb, RollupType rollup)
{
    ASSERT(m_ts != nullptr);

    if (rollup != RollupType::RU_NONE)
    {
        m_has_ooo = false;
        m_ts->query_for_rollup(tsdb, m_time_range, m_data, rollup);
    }
    else if (m_ts->query_for_data(tsdb, m_time_range, m_data))
        m_has_ooo = true;
}

void
QueryTask::merge_data()
{
    if (m_has_ooo)
        query_with_ooo();
    else
        query_without_ooo();

    for (DataPointContainer *container: m_data)
        MemoryManager::free_recyclable(container);
    m_data.clear();
    m_rollup_entries.clear();
}

void
QueryTask::fill()
{
    if (m_downsampler != nullptr)
    {
        m_downsampler->fill_if_needed(m_dps);
        MemoryManager::free_recyclable(m_downsampler);
        m_downsampler = nullptr;
    }
}

void
QueryTask::add_container(DataPointContainer *container)
{
    ASSERT(container != nullptr);
    //ASSERT(container->size() > 0);
    m_data.push_back(container);
}

void
QueryTask::query_with_ooo()
{
    using container_it = std::pair<DataPointContainer*,int>;
    auto container_cmp = [](const container_it &lhs, const container_it &rhs)
    {
        if (lhs.first->get_data_point(lhs.second).first > rhs.first->get_data_point(rhs.second).first)
        {
            return true;
        }
        else if (lhs.first->get_data_point(lhs.second).first == rhs.first->get_data_point(rhs.second).first)
        {
            if (lhs.first->is_out_of_order() && ! rhs.first->is_out_of_order())
                return true;
            else if (! lhs.first->is_out_of_order() && rhs.first->is_out_of_order())
                return false;

            //if (lhs.first->get_page_index() == 0)
                //return true;
            //else if (rhs.first->get_page_index() == 0)
                //return false;
            //else
                return (lhs.first->get_page_index() > rhs.first->get_page_index());
        }
        else
        {
            return false;
        }
    };
    std::priority_queue<container_it, std::vector<container_it>, decltype(container_cmp)> pq(container_cmp);
    uint64_t dp_count = 0;
    DataPointPair prev_dp(TT_INVALID_TIMESTAMP,0);

    for (auto container: m_data)
    {
        dp_count += container->size();
        pq.emplace(container, 0);
    }

#ifdef TT_STATS
    s_dp_count.fetch_add(dp_count, std::memory_order_relaxed);
#endif

    while (! pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        DataPointContainer *container = top.first;
        int i = top.second;
        DataPointPair& dp = container->get_data_point(i);
        int in_range = m_time_range.in_range(dp.first);

        if (in_range == 0)
        {
            // remove duplicates
            //if ((! m_dps.empty()) && (m_dps.back().first == dp.first))
            if (prev_dp.first == dp.first)
            {
                prev_dp.second = dp.second;
            }
            else
            {
                if (prev_dp.first != TT_INVALID_TIMESTAMP)
                {
                    if (m_downsampler == nullptr)
                        m_dps.emplace_back(prev_dp.first, prev_dp.second);
                    else
                        m_downsampler->add_data_point(prev_dp, m_dps);
                }

                prev_dp = dp;
            }
        }
        else if (in_range > 0)
        {
            break;
        }

        if ((i+1) < container->size())
        {
            pq.emplace(container, i+1);
        }
    }

    if (prev_dp.first != TT_INVALID_TIMESTAMP)
    {
        if (m_downsampler == nullptr)
            m_dps.emplace_back(prev_dp.first, prev_dp.second);
        else
            m_downsampler->add_data_point(prev_dp, m_dps);
    }
}

void
QueryTask::query_without_ooo()
{
    uint64_t dp_count = 0;

    for (DataPointContainer *container: m_data)
    {
        dp_count += container->size();

        for (int i = 0; i < container->size(); i++)
        {
            DataPointPair& dp = container->get_data_point(i);
            int in_range = m_time_range.in_range(dp.first);

            if (in_range == 0)
            {
                if (m_downsampler == nullptr)
                    m_dps.push_back(dp);
                else
                    m_downsampler->add_data_point(dp, m_dps);
            }
            else if (in_range > 0)
            {
                break;
            }
        }
    }

#ifdef TT_STATS
    s_dp_count.fetch_add(dp_count, std::memory_order_relaxed);
#endif
}

TimeSeriesId
QueryTask::get_ts_id() const
{
    ASSERT(m_ts != nullptr);
    return m_ts->get_id();
}

double
QueryTask::get_max(int n) const
{
    double max = std::numeric_limits<double>::min();

    for (int i = m_dps.size()-1; i >= 0 && n > 0; i--, n--)
    {
        if (max < m_dps[i].second)
            max = m_dps[i].second;
    }

    return max;
}

double
QueryTask::get_min(int n) const
{
    double min = std::numeric_limits<double>::max();

    for (int i = m_dps.size()-1; i >= 0 && n > 0; i--, n--)
    {
        if (min > m_dps[i].second)
            min = m_dps[i].second;
    }

    return min;
}

Tag *
QueryTask::get_tags()
{
    ASSERT(m_ts != nullptr);
    return m_ts->get_tags();
}

Tag_v2 &
QueryTask::get_v2_tags()
{
    ASSERT(m_ts != nullptr);
    return m_ts->get_v2_tags();
}

Tag *
QueryTask::get_cloned_tags(StringBuffer& strbuf)
{
    ASSERT(m_ts != nullptr);
    Tag *tags = m_ts->get_cloned_tags(strbuf);
    //Tag *removed = Tag::remove_first(&tags, METRIC_TAG_NAME);
    //Tag::free_list(removed, false);
    return tags;
}

void
QueryTask::init()
{
    m_ts = nullptr;
    m_has_ooo = false;
    m_file_index = TT_INVALID_FILE_INDEX;
    m_header_index = TT_INVALID_HEADER_INDEX;
    m_downsampler = nullptr;
    m_tstamp_from = 0;
    ASSERT(m_data.empty());
}

bool
QueryTask::recycle()
{
    m_dps.clear();
    m_dps.shrink_to_fit();
    m_results.recycle();
    m_rollup_entries.clear();
    m_rollup_entries.shrink_to_fit();
    ASSERT(m_data.empty());

    m_ts = nullptr;

    if (m_downsampler != nullptr)
    {
        MemoryManager::free_recyclable(m_downsampler);
        m_downsampler = nullptr;
    }

    return true;
}


QuerySuperTask::QuerySuperTask(TimeRange& range, const char* ds, bool ms) :
    m_ms(ms),
    m_errno(0),
    m_downsample(ds),
    m_compact(false),
    m_time_range(range)
{
    Tsdb::insts(m_time_range, m_tsdbs);
}

// this one is called by Tsdb::compact()
QuerySuperTask::QuerySuperTask(Tsdb *tsdb) :
    m_ms(true),
    m_errno(0),
    m_downsample(nullptr),
    m_time_range(tsdb->get_time_range())
{
    m_tsdbs.push_back(tsdb);
    m_compact = true;
}

QuerySuperTask::~QuerySuperTask()
{
    for (QueryTask *qt : m_tasks)
        MemoryManager::free_recyclable(qt);

    // ref-count was incremented in the constructor, by Tsdb::insts()
    if (! m_compact)
    {
        for (Tsdb *tsdb : m_tsdbs)
            tsdb->dec_ref_count();
    }
}

void
QuerySuperTask::empty_tasks()
{
    for (QueryTask *qt : m_tasks)
        MemoryManager::free_recyclable(qt);
    m_tasks.clear();
}

void
QuerySuperTask::add_task(TimeSeries *ts)
{
    QueryTask *qt =
        (QueryTask*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_TASK);

    qt->m_ts = ts;
    qt->m_time_range = m_time_range;
    qt->m_downsampler = (m_downsample == nullptr) ?
                        nullptr :
                        Downsampler::create(m_downsample, m_time_range, m_ms);

    m_tasks.push_back(qt);
}

RollupType
QuerySuperTask::use_rollup(Tsdb *tsdb) const
{
    ASSERT(tsdb != nullptr);
    RollupType rollup = RollupType::RU_NONE;

    if (! m_tasks.empty() && tsdb->is_rolled_up() && !tsdb->is_crashed())
    {
        auto task = m_tasks.front();
        Downsampler *downsampler = task->get_downsampler();

        if (downsampler != nullptr)
        {
            Timestamp interval = downsampler->get_interval();
            Timestamp rollup_interval = tsdb->get_rollup_interval();

            if (g_tstamp_resolution_ms) rollup_interval *= 1000;

            // TODO: config
            if (((double)rollup_interval * (double)0.9) <= (double)interval)
            {
                rollup = downsampler->get_rollup_type();
                Timestamp i = (interval / rollup_interval) * rollup_interval;

                if ((i + rollup_interval - interval) < (interval - i))
                    i += rollup_interval;

                // update downsample interval to i
                for (QueryTask *task : m_tasks)
                {
                    downsampler = task->get_downsampler();
                    downsampler->set_interval(i);
                }
            }
        }
    }

    return rollup;
}

void
QuerySuperTask::perform(bool lock)
{
    try
    {
        for (auto tsdb: m_tsdbs)
        {
            RollupType rollup = use_rollup(tsdb);

            if (lock)
                tsdb->query_for_data(m_metric_id, m_time_range, m_tasks, m_compact, rollup);
            else
                tsdb->query_for_data_no_lock(m_metric_id, m_time_range, m_tasks, m_compact, rollup);

            for (QueryTask *task : m_tasks)
            {
                task->query_ts_data(tsdb, rollup);
                task->merge_data();
                task->set_tstamp_from(0);
            }
        }

        for (QueryTask *task : m_tasks)
            task->fill();
    }
    catch (const std::exception& e)
    {
        if (std::strcmp(e.what(), TT_MSG_OUT_OF_MEMORY) == 0)
            m_errno = ENOMEM;     // out of memory
        else
        {
            m_errno = -1;
            Logger::error("QuerySuperTask: caught exception %s", e.what());
        }
    }
}


bool
QueryExecutor::http_get_api_query_handler(HttpRequest& request, HttpResponse& response)
{
#ifdef TT_STATS
    Timestamp ts_start = ts_now_ms();
#endif
    Meter meter(METRIC_TICKTOCK_QUERY_LATENCY_MS);
    Logger::debug("Handling get request: %T", &request);

    JsonMap params;
    request.parse_params(params);

    StringBuffer strbuf;
    Query query(params, strbuf);
    std::vector<QueryResults*> results;
    int error = 0;

    query.execute(results, strbuf);

    if (query.get_errno() != 0)
        error = query.get_errno();

    JsonParser::free_map(params);

    bool status = prepare_response(results, response, error);

    for (QueryResults *r: results)
        MemoryManager::free_recyclable(r);

#ifdef TT_STATS
    Timestamp ts_end = ts_now_ms();
    g_query_count++;
    g_query_latency_ms += ts_end - ts_start;
#endif

    return status;
}

bool
QueryExecutor::http_post_api_query_handler(HttpRequest& request, HttpResponse& response)
{
#ifdef TT_STATS
    Timestamp ts_start = ts_now_ms();
#endif
    Meter meter(METRIC_TICKTOCK_QUERY_LATENCY_MS);
    bool ms = false;
    JsonMap map;

    Logger::debug("Handling post request: %T", &request);

    if (request.content == nullptr)
    {
        const char* errMsg = "Error: POST request content is null. Did you mean to use GET instead?\n";
        response.init(400, HttpContentType::PLAIN, strlen(errMsg), errMsg);

        return false;
    }

    JsonParser::parse_map(request.content, map);
    auto search = map.find("start");
    if (search == map.end())
    {
        const char* errMsg = "Error: POST request doesn't specify parameter 'start'!\n";
        response.init(400, HttpContentType::PLAIN, strlen(errMsg), errMsg);
        return false;
    }

    Timestamp now = ts_now();
    Timestamp start = parse_ts(search->second, now);
    start = validate_resolution(start);

    Timestamp end;
    search = map.find("end");
    if (search != map.end())
        end = parse_ts(search->second, now);
    else
        end = now;
    end = validate_resolution(end);

    search = map.find("msResolution");
    if (search != map.end())
    {
        ms = search->second->to_bool();
    }

    search = map.find("queries");
    if (search == map.end())
    {
        const char* errMsg = "Error: POST request doesn't specify parameter 'queries'!\n";
        response.init(400, HttpContentType::PLAIN, strlen(errMsg), errMsg);
        return false;
    }
    JsonArray& array = search->second->to_array();

    StringBuffer strbuf;
    std::vector<QueryResults*> results;
    int error = 0;

    for (int i = 0; i < array.size(); i++)
    {
        JsonMap& m = array[i]->to_map();
        TimeRange range(start, end);
        Query query(m, range, strbuf, ms);
        std::vector<QueryResults*> res;

        Logger::debug("query: %T", &query);
        query.execute(res, strbuf);

        if (query.get_errno() != 0)
            error = query.get_errno();

        if (! res.empty())
        {
            results.insert(results.end(), res.begin(), res.end());
        }
    }

    JsonParser::free_map(map);

    bool status = prepare_response(results, response, error);

    for (QueryResults *r: results)
        MemoryManager::free_recyclable(r);

#ifdef TT_STATS
    Timestamp ts_end = ts_now_ms();
    g_query_count++;
    g_query_latency_ms += ts_end - ts_start;
#endif

    return status;
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

bool
QueryExecutor::prepare_response(std::vector<QueryResults*>& results, HttpResponse& response, int error)
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
            n += snprintf(buff+n, size-n, ",");
        if (size > n)
            n += r->to_json(buff+n, size-n);
        if (n >= size) break;
    }

    if (UNLIKELY(error != 0))
    {
        switch (error)
        {
            case ENOMEM:
                response.init(503, HttpContentType::PLAIN, 0, nullptr);
                break;

            default:
                response.init(500, HttpContentType::PLAIN, 0, nullptr);
                break;
        }

        status = false;
    }
    else if (n >= size)
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

    Logger::debug("response: %T", &response);
    return status;
}


void
QueryResults::add_query_task(QueryTask *qt, StringBuffer& strbuf)
{
    ASSERT(qt != nullptr);
    Tag *tag_head = qt->get_tags();

    for (Tag *tag = tag_head; tag != nullptr; tag = tag->next())
    {
        //if (std::strcmp(tag->m_key, METRIC_TAG_NAME) == 0) continue;
        ASSERT(std::strcmp(tag->m_key, METRIC_TAG_NAME) != 0);

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
        else if (ends_with(match->m_value, '*') || ((std::strchr(match->m_value, '|') != nullptr)))
        {
            // move it from tags to aggregate_tags
            remove_tag(match->m_key, true); // free the tag just removed, instead of return it
            add_tag(strbuf.strdup(tag->m_key), strbuf.strdup(tag->m_value));
        }
        else if (std::strcmp(match->m_value, tag->m_value) != 0)
        {
            // move it from tags to aggregate_tags
            remove_tag(match->m_key, true); // free the tag just removed, instead of return it
            add_aggregate_tag(strbuf.strdup(tag->m_key));
            //m_aggregate_tags.push_back(tag->m_key);
        }
    }

    if (tag_head != nullptr)
        Tag::free_list(tag_head);

    m_qtv.push_back(qt);
}


void
DataPointContainer::collect_data(PageInMemory *page)
{
    ASSERT(page != nullptr);
    set_out_of_order(page->is_out_of_order());
    page->get_all_data_points(m_dps);
}

void
DataPointContainer::collect_data(Timestamp from, struct tsdb_header *tsdb_header, struct page_info_on_disk *page_header, void *page)
{
    ASSERT(tsdb_header != nullptr);
    ASSERT(page_header != nullptr);
    ASSERT(page != nullptr);

    struct compress_info_on_disk *ciod = reinterpret_cast<struct compress_info_on_disk*>(page);
    CompressorPosition position(ciod);
    int compressor_version = tsdb_header->get_compressor_version();
    RecyclableType type;
    if (page_header->is_out_of_order())
        type = RecyclableType::RT_COMPRESSOR_V0;
    else
        type = (RecyclableType)(compressor_version + RecyclableType::RT_COMPRESSOR_V0);
    Compressor *compressor = (Compressor*)MemoryManager::alloc_recyclable(type);
    compressor->init(from,
                     reinterpret_cast<uint8_t*>(page) + sizeof(struct compress_info_on_disk),
                     tsdb_header->m_page_size);
    compressor->restore(m_dps, position, (uint8_t*)page + sizeof(struct compress_info_on_disk));
    ASSERT(! m_dps.empty());
    MemoryManager::free_recyclable(compressor);
}

void
DataPointContainer::collect_data(RollupManager& rollup_mgr, RollupType rollup_type)
{
    DataPointPair dp;
    if (rollup_mgr.query(rollup_type, dp))
        m_dps.emplace_back(dp.first, dp.second);
}


}
