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
#include <algorithm>
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


Query::Query(JsonMap& map, TimeRange& range, StringBuffer& strbuf, bool ms, const char *tz) :
    m_time_range(range),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_tz(tz),
    m_rate_calculator(nullptr),
    m_ms(ms),
    m_explicit_tags(false),
    m_rollup(RollupUsage::RU_FALLBACK_RAW),
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

    search = map.find("rollupUsage");
    if (search != map.end())
    {
        const char *rollup = search->second->to_string();
        ASSERT(rollup != nullptr);

        if (std::strcmp(rollup, "ROLLUP_RAW") == 0)
            m_rollup = RollupUsage::RU_RAW;
        else if (std::strcmp(rollup, "ROLLUP_FALLBACK_RAW") == 0)
            m_rollup = RollupUsage::RU_FALLBACK_RAW;
        else
            Logger::warn("Ignoring unrecognized rollupUsage: %s", rollup);
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

/* Syntax:
 *  m=<aggregator>:[rate[{counter[,<counter_max>[,<reset_value>]]}]:][<down_sampler>:][percentiles\[<p1>, <pn>\]:][explicit_tags:]<metric_name>[{<tag_name1>=<grouping filter>[,...<tag_nameN>=<grouping_filter>]}][{<tag_name1>=<non grouping filter>[,...<tag_nameN>=<non_grouping_filter>]}]
 */
Query::Query(JsonMap& map, StringBuffer& strbuf) :
    m_time_range(0L, 0L),
    m_metric(nullptr),
    m_aggregate(nullptr),
    m_aggregator(nullptr),
    m_downsample(nullptr),
    m_tz(nullptr),
    m_rate_calculator(nullptr),
    m_ms(false),
    m_explicit_tags(false),
    m_rollup(RollupUsage::RU_FALLBACK_RAW),
    m_non_grouping_tags(nullptr),
    m_errno(0),
    TagOwner(false)
{
    auto search = map.find("tz");

    // timezone
    if (search != map.end())
        m_tz = search->second->to_string();
    else
        m_tz = g_timezone.c_str();

    Timestamp now = ts_now();
    search = map.find("start");
    if (search == map.end())
        throw std::runtime_error("Must specify start time when query.");
    Timestamp start = parse_ts(search->second, now, m_tz);
    start = validate_resolution(start);

    search = map.find("end");
    Timestamp end;
    if (search != map.end())
        end = parse_ts(search->second, now, m_tz);
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

    char buff[MAX_URL_LENGTH];
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
        else if (std::strncmp(token, "rollupUsage=", 12) == 0)
        {
            // token example: rollupUsage=ROLLUP_RAW
            if (std::strcmp(token+12, "ROLLUP_RAW") == 0)
                m_rollup = RollupUsage::RU_RAW;
            else if (std::strcmp(token+12, "ROLLUP_FALLBACK_RAW") == 0)
                m_rollup = RollupUsage::RU_FALLBACK_RAW;
            else
                Logger::warn("Ignoring unrecognized rollupUsage: %s", token);
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
        {
            const char *name = strbuf.strdup((const char*)it->first);
            const char *value = strbuf.strdup(it->second->to_string());
            add_tag(name, value);
        }

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

    if (m_non_grouping_tags != nullptr)
    {
        Tag::free_list(m_non_grouping_tags, false);
        m_non_grouping_tags = nullptr;
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
        m_aggregator->aggregate(m_metric, qtv, results, strbuf);
    else
    {
        // group qtv into results
        create_query_results(qtv, results, strbuf);

        // aggregate results
        for (QueryResults* result: results)
            m_aggregator->aggregate(result);
    }

    // sort query result-sets in alphabetical order
    if (results.size() > 1)
    {
        std::stable_sort(results.begin(), results.end(),
            [](const QueryResults* const & left, const QueryResults* const & right)
            {
                return left->less_than(*right);
            });
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
Query::create_one_query_results(QueryTask *qtask, StringBuffer& strbuf)
{
    ASSERT(qtask != nullptr);

    QueryResults *result =
        (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
    result->m_metric = m_metric;
    result->add_first_query_task(qtask, strbuf);
    return result;
}

void
Query::create_query_results(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    ASSERT(results.empty());

    for (QueryTask *qt: qtv)
    {
        QueryResults *result = nullptr;
        Tag_v2& qt_tags = qt->get_v2_tags();

        // see if 'qt' belongs to any existing QueryResults
        for (QueryResults *qr: results)
        {
            bool match = true;

            for (Tag *tag = qr->get_tags(); tag != nullptr; tag = tag->next())
            {
                // skip those tags that are not queried, including non-grouping ones
                if (find_by_key(tag->m_key) == nullptr)
                    continue;

                // skip non-grouping tags
                if (TagOwner::find_by_key(m_non_grouping_tags, tag->m_key) != nullptr)
                    continue;

                if (! qt_tags.match(tag->m_key, tag->m_value))
                {
                    match = false;
                    break;
                }
            }

            if (match)
            {
                result = qr;
                break;
            }
        }

        if (result == nullptr)  // if 'qt' does not belong to any existing QueryResults, create one
        {
            result = create_one_query_results(qt, strbuf);
            results.push_back(result);
        }
        else                    // add 'qt' to the QueryResults it belongs to
            result->add_query_task(qt, m_tags, m_non_grouping_tags, strbuf);
    }

    Logger::debug("created %d QueryResults", results.size());
}

void
Query::execute(std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    QuerySuperTask super_task(m_time_range, m_downsample, m_ms, m_rollup);

    get_query_tasks(super_task);

    if (super_task.get_metric_id() != TT_INVALID_METRIC_ID)
    {
        super_task.perform();
        aggregate(super_task.get_tasks(), results, strbuf);
        calculate_rate(results);
        m_errno = super_task.get_errno();
    }
}

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
QueryTask::query_ts_data(Tsdb *tsdb)
{
    ASSERT(m_ts != nullptr);

    if (m_ts->query_for_data(tsdb, m_time_range, m_data))
        m_has_ooo = true;
}

void
QueryTask::query_ts_data(const TimeRange& range, RollupType rollup, bool ms)
{
    ASSERT(rollup != RollupType::RU_NONE);
    ASSERT(m_ts != nullptr);

    m_has_ooo = false;
    m_ts->query_for_rollup(range, this, rollup, ms);
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

// add rolled-up data
void
QueryTask::add_data_point(struct rollup_entry_ext *entry, RollupType rollup)
{
    ASSERT(entry != nullptr);

    if (m_downsampler == nullptr)
    {
        double val = RollupManager::query((struct rollup_entry*)entry, rollup);
        m_dps.emplace_back((Timestamp)entry->tstamp, val);
    }
    else
    {
        m_downsampler->add_data_point(entry, rollup, m_dps);
    }
}

void
QueryTask::convert_to_ms()
{
    for (DataPointPair& dp: m_dps)
        dp.first = to_ms(dp.first);
}

void
QueryTask::convert_to_sec()
{
    for (DataPointPair& dp: m_dps)
        dp.first = to_sec(dp.first);
}

void
QueryTask::remove_dps(const TimeRange& range)
{
    for (auto it = m_dps.begin(); it != m_dps.end(); )
    {
        if (range.in_range(it->first) == 0)
            it = m_dps.erase(it);
        else
            it++;
    }

    m_last_tstamp = TT_INVALID_TIMESTAMP;
    m_downsampler->recycle();
    m_sort_needed = true;
}

void
QueryTask::sort_if_needed()
{
    if (m_sort_needed)
        std::sort(m_dps.begin(), m_dps.end());
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
    double max = std::numeric_limits<double>::lowest();

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
    m_sort_needed = false;
    m_file_index = TT_INVALID_FILE_INDEX;
    m_header_index = TT_INVALID_HEADER_INDEX;
    m_downsampler = nullptr;
    m_tstamp_from = 0;
    m_last_tstamp = TT_INVALID_TIMESTAMP;
    ASSERT(m_data.empty());
}

bool
QueryTask::recycle()
{
    m_dps.clear();
    m_dps.shrink_to_fit();
    m_results.recycle();
    ASSERT(m_data.empty());

    m_ts = nullptr;

    if (m_downsampler != nullptr)
    {
        MemoryManager::free_recyclable(m_downsampler);
        m_downsampler = nullptr;
    }

    return true;
}


QuerySuperTask::QuerySuperTask(TimeRange& range, const char* ds, bool ms, RollupUsage rollup) :
    m_ms(ms),
    m_errno(0),
    m_downsample(ds),
    m_compact(false),
    m_time_range(range),
    m_rollup(rollup),
    m_metric_id(TT_INVALID_METRIC_ID)
{
}

// this one is called by Tsdb::compact()
QuerySuperTask::QuerySuperTask(Tsdb *tsdb) :
    m_ms(true),
    m_errno(0),
    m_downsample(nullptr),
    m_metric_id(TT_INVALID_METRIC_ID),
    m_time_range(tsdb->get_time_range()),
    m_rollup(RollupUsage::RU_FALLBACK_RAW)
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
QuerySuperTask::use_rollup() const
{
    RollupType rollup = RollupType::RU_NONE;

    if ((m_rollup == RollupUsage::RU_UNKNOWN) || (m_rollup == RollupUsage::RU_RAW))
        return rollup;  // do not use rollup data

    if (! m_tasks.empty())
    {
        auto task = m_tasks.front();
        Downsampler *downsampler = task->get_downsampler();

        if (downsampler != nullptr)
        {
            Timestamp downsample_interval = downsampler->get_interval();
            Timestamp rollup_interval = g_rollup_interval_1h;   // always in secs
            Timestamp rollup_interval2 = g_rollup_interval_1d;  // always in secs
            bool level2 = false;    // default to 'use rollup_interval' instead of rollup_interval2

            if (g_tstamp_resolution_ms)
            {
                rollup_interval *= 1000;
                rollup_interval2 *= 1000;

                if (! m_ms) downsample_interval *= 1000;
            }
            else if (m_ms)
                downsample_interval /= 1000;

            // TODO: config
            if (rollup_interval2 <= downsample_interval)
            {
                level2 = true;
                rollup_interval = rollup_interval2;     // use rollup_interval2
            }

            if (rollup_interval <= downsample_interval)
            {
                rollup = downsampler->get_rollup_type();

                if (rollup != RollupType::RU_NONE)
                {
                    set_rollup_level(rollup, level2);

                    // round to nearest multiple of rollup_interval
                    Timestamp i = (downsample_interval / rollup_interval) * rollup_interval;

                    if ((i + rollup_interval - downsample_interval) < (downsample_interval - i))
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
    }

    return rollup;
}

void
QuerySuperTask::query_raw(Tsdb *tsdb, std::vector<QueryTask*>& tasks)
{
    ASSERT(tsdb != nullptr);

    tsdb->query_for_data(m_metric_id, m_time_range, tasks, m_compact);

    for (QueryTask *task : tasks)
    {
        task->query_ts_data(tsdb);
        task->merge_data();
        task->set_tstamp_from(0);
    }
}

void
QuerySuperTask::query_rollup_hourly(const TimeRange& range, const std::vector<Tsdb*>& tsdbs, RollupType rollup)
{
    ASSERT(rollup != RollupType::RU_NONE);

    set_rollup_level(rollup, false);    // convert to level1 rollup
    RollupManager::query(m_metric_id, range, m_tasks, rollup);

    // for those with invalid rollup, we'll query raw instead
    for (auto tsdb: tsdbs)
    {
        if (tsdb->can_use_rollup(false))
            continue;

        std::vector<QueryTask*> tasks;

        for (QueryTask *task : m_tasks)
        {
            if (! tsdb->can_use_rollup(task->get_ts_id()))
            {
                tasks.push_back(task);
                task->remove_dps(tsdb->get_time_range());
            }
        }

        if (! tasks.empty())
            query_raw(tsdb, tasks);
    }

    for (QueryTask *task : m_tasks)
        task->query_ts_data(range, rollup, m_ms);
}

void
QuerySuperTask::query_rollup_daily(RollupType rollup)
{
    ASSERT(is_rollup_level2(rollup));
    ASSERT(rollup != RollupType::RU_NONE);

    RollupManager::query(m_metric_id, m_time_range, m_tasks, rollup);

    // for those with invalid rollup, we'll query hourly/raw instead
    while (! m_tsdbs.empty())
    {
        // if its rollup data is fine, skip it
        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); )
        {
            Tsdb *tsdb = *it;
            if (! tsdb->can_use_rollup(true))
                break;
            tsdb->dec_ref_count();
            it = m_tsdbs.erase(it);
        }

        std::vector<Tsdb*> tsdbs;

        // look for ones whose daily rollup is not available
        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); )
        {
            Tsdb *tsdb = *it;
            if (tsdb->has_daily_rollup())
                break;
            tsdbs.push_back(tsdb);
            it = m_tsdbs.erase(it);
        }

        if (! tsdbs.empty())
        {
            TimeRange range(TimeRange::MIN);

            for (auto tsdb: tsdbs)
                range.merge(tsdb->get_time_range());
            range.intersect(m_time_range);

            for (auto task: m_tasks)
                task->set_last_tstamp(TT_INVALID_TIMESTAMP);

            ASSERT(rollup != RollupType::RU_NONE);
            query_rollup_hourly(range, tsdbs, rollup);

            // cleanup
            for (auto tsdb: tsdbs)
                tsdb->dec_ref_count();
            tsdbs.clear();
        }

        if (m_tsdbs.empty())
            break;

        // look for ones with out-of-order data;
        // they invalidated both hourly & daily rollup data
        Tsdb *tsdb = m_tsdbs.front();
        ASSERT(tsdb->has_daily_rollup());

        if (tsdb->can_use_rollup(true))
            continue;
        m_tsdbs.erase(m_tsdbs.begin());

        // query raw instead
        std::vector<QueryTask*> tasks;

        for (auto task: m_tasks)
        {
            if (! tsdb->can_use_rollup(task->get_ts_id()))
            {
                tasks.push_back(task);
                task->remove_dps(tsdb->get_time_range());
            }
        }

        if (! tasks.empty())
            query_raw(tsdb, tasks);

        tsdb->dec_ref_count();
    }
}

void
QuerySuperTask::perform(bool lock)
{
    try
    {
        RollupType rollup = use_rollup();
        bool level2 = is_rollup_level2(rollup);
        Tsdb::insts(m_time_range, m_tsdbs);

        if (rollup == RollupType::RU_NONE)
        {
            for (auto tsdb: m_tsdbs)
                query_raw(tsdb, m_tasks);
        }
        else if (! level2)  // query hourly rollup
        {
            query_rollup_hourly(m_time_range, m_tsdbs, rollup);
        }
        else    // query daily rollup
        {
            query_rollup_daily(rollup);
        }

        for (auto task: m_tasks)
        {
            task->sort_if_needed();
            task->fill();

            if (m_ms)
                task->convert_to_ms();
            else
                task->convert_to_sec();
        }
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
    const char *tz;
    auto search = map.find("tz");

    if (search != map.end())
        tz = search->second->to_string();
    else
        tz = g_timezone.c_str();

    search = map.find("start");
    if (search == map.end())
    {
        const char* errMsg = "Error: POST request doesn't specify parameter 'start'!\n";
        response.init(400, HttpContentType::PLAIN, strlen(errMsg), errMsg);
        return false;
    }

    Timestamp now = ts_now();
    Timestamp start = parse_ts(search->second, now, tz);
    start = validate_resolution(start);

    Timestamp end;
    search = map.find("end");
    if (search != map.end())
        end = parse_ts(search->second, now, tz);
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
        Query query(m, range, strbuf, ms, tz);
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
    static const char *filters =
        "{\"literal_or\":{\"examples\":\"host=literal_or(web01), host=literal_or(web01|web02|web03) {\\\"type\\\":\\\"literal_or\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web01|web02|web03\\\",\\\"groupBy\\\":false}\",\"description\":\"Accepts one or more exact values and matches if the series contains any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case sensitive and will not allow characters that TickTockDB does not allow at write time.\"},\"iliteral_or\":{\"examples\":\"host=iliteral_or(web01), host=iliteral_or(Web01|WEB02|web03) {\\\"type\\\":\\\"iliteral_or\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web01|web02|web03\\\",\\\"groupBy\\\":false}\",\"description\":\"Accepts one or more exact values and matches if the series contains any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case insensitive and will not allow characters that TickTockDB does not allow at write time.\"},\"not_literal_or\":{\"examples\":\"host=not_literal_or(web01), host=not_literal_or(web01|web02|web03) {\\\"type\\\":\\\"not_literal_or\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web01|web02|web03\\\",\\\"groupBy\\\":false}\",\"description\":\"Accepts one or more exact values and matches if the series does NOT contain any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case sensitive and will not allow characters that TickTockDB does not allow at write time.\"},\"not_iliteral_or\":{\"examples\":\"host=not_iliteral_or(web01), host=not_iliteral_or(Web01|WEB02|web03) {\\\"type\\\":\\\"not_iliteral_or\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web01|web02|web03\\\",\\\"groupBy\\\":false}\",\"description\":\"Accepts one or more exact values and matches if the series does NOT contain any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case insensitive and will not allow characters that TickTockDB does not allow at write time.\"},\"regexp\":{\"examples\":\"host=regexp(.*)  {\\\"type\\\":\\\"regexp\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\".*\\\",\\\"groupBy\\\":false}\",\"description\":\"Provides full, POSIX compliant regular expression using the built in Java Pattern class. Note that an expression containing curly braces {} will not parse properly in URLs. If the pattern is not a valid regular expression then an exception will be raised.\"},\"wildcard\":{\"examples\":\"host=wildcard(web*), host=wildcard(web*.ticktockdb.com) {\\\"type\\\":\\\"wildcard\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web*.ticktockdb.com\\\",\\\"groupBy\\\":false}\",\"description\":\"Performs pre, post and in-fix glob matching of values. The globs are case sensitive and multiple wildcards can be used. The wildcard character is the * (asterisk). At least one wildcard must be present in the filter value. A wildcard by itself can be used as well to match on any value for the tag key.\"},\"iwildcard\":{\"examples\":\"host=iwildcard(web*),  host=iwildcard(web*.ticktockdb.com) {\\\"type\\\":\\\"iwildcard\\\",\\\"tagk\\\":\\\"host\\\",\\\"filter\\\":\\\"web*.ticktockdb.com\\\",\\\"groupBy\\\":false}\",\"description\":\"Performs pre, post and in-fix glob matching of values. The globs are case insensitive and multiple wildcards can be used. The wildcard character is the * (asterisk). Case insensitivity is achieved by dropping all values to lower case. At least one wildcard must be present in the filter value. A wildcard by itself can be used as well to match on any value for the tag key.\"}}";

    // right now we do not support any filters
    response.init(200, HttpContentType::JSON, std::strlen(filters), filters);
    return true;
}

bool
QueryExecutor::http_get_api_search_lookup_handler(HttpRequest& request, HttpResponse& response)
{
    Logger::debug("Handling get request: %T", &request);

    // parse request
    JsonMap params;
    request.parse_params(params);

    int limit = 25;
    auto search = params.find("limit");
    if (search != params.end())
        limit = (int)search->second->to_double();

    int index = 0;
    search = params.find("startIndex");
    if (search != params.end())
        index = (int)search->second->to_double();

    search = params.find("m");
    if (search == params.end())
        throw std::runtime_error("Must specify m parameter when performing search/lookup.");

    char buff[MAX_URL_LENGTH];
    bool decode_ok = url_unescape(search->second->to_string(), buff, sizeof(buff));
    if (! decode_ok)
        throw std::runtime_error("Failed to URL decode search/lookup.");

    Logger::debug("after-decoding of search/lookup: %s; limit=%d", buff, limit);

    TagOwner tags(false);   // we do not own memory for those tags
    char *metric = &buff[0];
    char *kvs = std::strchr(metric, '{');
    StringBuffer strbuf;

    if (kvs != nullptr)
    {
        JsonMap m;

        if (std::strchr(kvs+1, '"') == nullptr)
            JsonParser::parse_map_unquoted(kvs, m, '=');
        else
            JsonParser::parse_map(kvs, m, '=');

        for (auto it = m.begin(); it != m.end(); it++)
        {
            char *key = strbuf.strdup((const char*)it->first);
            char *value = strbuf.strdup(it->second->to_string());
            tags.add_tag(key, value);
        }

        JsonParser::free_map(m);
        *kvs = 0;
    }

    JsonParser::free_map(params);

    // perform lookup
    std::unordered_set<TimeSeries*> tsv;

    if (*metric == '*')     // apply to ALL metrics
        Tsdb::query_for_ts(tags.get_tags(), tsv);
    else
        Tsdb::query_for_ts(metric, tags.get_tags(), tsv, nullptr, false);

    char *resp_buff = response.get_buffer();
    int size = response.get_buffer_size();

    if (tsv.empty())
    {
        char msg[1024];
        snprintf(msg, sizeof(msg), "ERROR: metric name \"%s\" not found.", metric);
        response.init(400, HttpContentType::PLAIN, std::strlen(msg), msg);
    }
    else
    {
        ASSERT(tsv.size() > 0);
        int n = 0;  // resp_buff position
        int i = 0;  // starting index tracker
        int l = 0;  // limit tracker

        // meta
        n += snprintf(resp_buff+n, size-n,
            "{\"type\":\"LOOKUP\",\"limit\":%d,\"startIndex\":%d,\"totalResults\":%u,\"metric\":\"%s\",\"tags\":[",
            limit, index, (unsigned int)tsv.size(), metric);

        // tags
        for (auto tag = tags.get_tags(); tag != nullptr; tag = tag->next())
            n += snprintf(resp_buff+n, size-n, "{\"key\":\"%s\",\"value\":\"%s\"},", tag->m_key, tag->m_value);

        // remove last comma
        if (tags.get_tags() != nullptr) n--;
        n += snprintf(resp_buff+n, size-n, "],\"results\":[");

        // results
        for (auto ts: tsv)
        {
            if (i++ < index) continue;
            if (l++ >= limit) break;

            n += snprintf(resp_buff+n, size-n, "{\"tsuid\":\"%09X\",\"metric\":\"%s\",\"tags\":{",
                ts->get_id(), metric);

            Tag *tags = ts->get_tags();
            for (auto tag = tags; tag != nullptr; tag = tag->next())
                n += snprintf(resp_buff+n, size-n, "\"%s\":\"%s\",", tag->m_key, tag->m_value);

            if (tags != nullptr)
            {
                n--;
                Tag::free_list(tags, false);
            }

            n += snprintf(resp_buff+n, size-n, "}},");
        }

        // remove last comma
        if (! tsv.empty()) n--;
        n += snprintf(resp_buff+n, size-n, "]}");
        ASSERT((n+1) < size);

        response.init(200, HttpContentType::JSON, n);
    }

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
QueryResults::add_first_query_task(QueryTask *qtask, StringBuffer& strbuf)
{
    ASSERT(qtask != nullptr);
    ASSERT(m_qtv.empty());

    Tag *ts_tags = qtask->get_tags();

    // clone TimeSeries' tags
    for (Tag *tag = ts_tags; tag != nullptr; tag = tag->next())
    {
        ASSERT(std::strcmp(tag->m_key, METRIC_TAG_NAME) != 0);
        add_tag(strbuf.strdup(tag->m_key), strbuf.strdup(tag->m_value));    // TODO: do we need to strdup()?
    }

    if (ts_tags != nullptr)
        Tag::free_list(ts_tags);

    m_qtv.push_back(qtask);
}

/* @param tags Tags that are being queried, including non-grouping ones;
 */
void
QueryResults::add_query_task(QueryTask *qtask, Tag *grouping_tags, Tag *non_grouping_tags, StringBuffer& strbuf)
{
    ASSERT(qtask != nullptr);
    ASSERT(! m_qtv.empty());

    Tag *tag_head = qtask->get_tags();

    for (Tag *tag = tag_head; tag != nullptr; tag = tag->next())
    {
        ASSERT(std::strcmp(tag->m_key, METRIC_TAG_NAME) != 0);

        if ((find_by_key(grouping_tags, tag->m_key) != nullptr) &&
            (find_by_key(non_grouping_tags, tag->m_key) == nullptr))
            continue;

        Tag *match = find_by_key(tag->m_key);
        if (match == nullptr) continue;

        if (std::strcmp(match->m_value, tag->m_value) != 0)
        {
            // move it from tags to aggregate_tags
            remove_tag(match->m_key, true); // free the tag just removed, instead of return it
            add_aggregate_tag(strbuf.strdup(tag->m_key));
        }
    }

    // remove any aggregate-tags that's not also in 'qtask'
    // because tags in aggregate-tags must be common to ALL tasks
    for (auto it = m_aggregate_tags.begin(); it != m_aggregate_tags.end(); )
    {
        char *key = *it;

        if (TagOwner::find_by_key(tag_head, key) == nullptr)
            it = m_aggregate_tags.erase(it);
        else
            it++;
    }

    Tag *next;

    for (Tag *tag = m_tags; tag != nullptr; tag = next)
    {
        next = tag->next();

        if (TagOwner::find_by_key(tag_head, tag->m_key) == nullptr)
            remove_tag(tag->m_key, true); // free the tag just removed, instead of return it
    }

    if (tag_head != nullptr)
        Tag::free_list(tag_head);

    m_qtv.push_back(qtask);
}

char *
QueryResults::to_json_aggregate_tags(char *buff, int size) const
{
    int n = snprintf(buff, size, "\"aggregateTags\":[");

    for (const char *name: m_aggregate_tags)
    {
        if ((n+3) >= size) break;
        if (buff[n-1] != '[')
        {
            n += snprintf(buff+n, size-n, ",");
        }
        n += snprintf(buff+n, size-n, "\"%s\"", name);
    }

    if (size > n)
        snprintf(buff+n, size-n, "]");

    return buff;
}

int
QueryResults::to_json(char *buff, int size) const
{
    char buf1[1024];    // TODO: no magic numbers
    char buf2[1024];    // TODO: no magic numbers

    int n = snprintf(buff, size, "{\"metric\":\"%s\",%s,%s,\"dps\":{",
        m_metric, to_json_tags(buf1, sizeof(buf1)),
        to_json_aggregate_tags(buf2, sizeof(buf2)));

    for (const DataPointPair& dp: m_dps)
    {
        if ((n+8) >= size) break;
        if (buff[n-1] != '{')
            n += snprintf(buff+n, size-n, ",");
        if (UNLIKELY(isinf(dp.second)))
            n += snprintf(buff+n, size-n, "\"%" PRIu64 "\":\"Inf\"", dp.first);
        else if (UNLIKELY(isnan(dp.second)))
            n += snprintf(buff+n, size-n, "\"%" PRIu64 "\":\"NaN\"", dp.first);
        else
            n += snprintf(buff+n, size-n, "\"%" PRIu64 "\":%.16lf", dp.first, dp.second);
        if (n > size) n = size;
        while ((buff[n-1] == '0') && (buff[n-2] != '.') && (buff[n-2] != ':')) buff[--n] = 0;
    }

    if (size > n)
        n += snprintf(buff+n, size-n, "}}");

    return (n <= size) ? n : size;
}

char *
QueryResults::to_json_tags(char *buff, int size) const
{
    int n = snprintf(buff, size, "\"tags\":");
    ASSERT(size > n);
    KeyValuePair::to_json(m_tags, buff+n, size-n);
    return buff;
}


void
DataPointContainer::collect_data(PageInMemory *page)
{
    ASSERT(page != nullptr);
    set_out_of_order(page->is_out_of_order());
    page->get_all_data_points(m_dps);
}

void
DataPointContainer::collect_data(Timestamp from, PageSize page_size, int compressor_version, struct page_info_on_disk *page_header, void *page)
{
    ASSERT(page_header != nullptr);
    ASSERT(page != nullptr);
    ASSERT(compressor_version > 0 && compressor_version < 5);

    struct compress_info_on_disk *ciod = reinterpret_cast<struct compress_info_on_disk*>(page);
    CompressorPosition position(ciod);
    RecyclableType type;
    if (page_header->is_out_of_order())
        type = RecyclableType::RT_COMPRESSOR_V0;
    else
        type = (RecyclableType)(compressor_version + RecyclableType::RT_COMPRESSOR_V0);
    Compressor *compressor = (Compressor*)MemoryManager::alloc_recyclable(type);
    compressor->init(from,
                     reinterpret_cast<uint8_t*>(page) + sizeof(struct compress_info_on_disk),
                     page_size - sizeof(struct compress_info_on_disk));
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
