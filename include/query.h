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

#include <map>
#include <vector>
#include "json.h"
#include "range.h"
#include "http.h"
#include "recycle.h"
#include "strbuf.h"
#include "stop.h"
#include "tag.h"
#include "type.h"


namespace tt
{


class QueryTask;
class TimeSeries;


class QueryResults : public TagOwner, public Recyclable
{
// TODO: make them private
public:
    const char *m_metric;
    std::vector<char*> m_aggregate_tags;
    DataPointVector m_dps;
    std::vector<QueryTask*> m_qtv;

    QueryResults() :
        m_metric(nullptr),
        TagOwner(false)
    {
    }

    ~QueryResults()
    {
        recycle();
    }

    bool recycle()
    {
        // we don't own 'metric' so don't free it
        m_metric = nullptr;
        m_dps.clear();
        m_dps.shrink_to_fit();
        m_aggregate_tags.clear();
        m_aggregate_tags.shrink_to_fit();
        m_qtv.clear();
        m_qtv.shrink_to_fit();
        if (m_tags != nullptr)
        {
            Tag::free_list(m_tags, false);
            m_tags = nullptr;
        }
        return true;
    }

    void add_query_task(QueryTask *qt, StringBuffer& strbuf);

    inline void add_aggregate_tag(char *key)
    {
        ASSERT(key != nullptr);
        m_aggregate_tags.push_back(key);
    }

    bool empty() const
    {
        return m_dps.empty();
    }

    bool has_tag(const char *key) const
    {
        ASSERT(key != nullptr);
        return Tag::has_key(m_tags, key);
    }

    bool has_exact_tag(Tag *tag) const
    {
        ASSERT(tag != nullptr);
        return Tag::has_key_value(m_tags, tag->m_key, tag->m_value);
    }

    char *to_json_tags(char *buff, int size) const
    {
        int n = snprintf(buff, size, "\"tags\":");
        KeyValuePair::to_json(m_tags, buff+n, size-n);
        return buff;
    }

    char *to_json_aggregate_tags(char *buff, int size) const
    {
        int n = snprintf(buff, size, "\"aggregateTags\":[");

        for (const char *name: m_aggregate_tags)
        {
            if (buff[n-1] != '[')
            {
                n += snprintf(buff+n, size-n, ",");
            }
            n += snprintf(buff+n, size-n, "\"%s\"", name);
        }

        n += snprintf(buff+n, size-n, "]");

        return buff;
    }

    int to_json(char *buff, int size) const
    {
        char buf1[1024];
        char buf2[1024];

        int n = snprintf(buff, size, "{\"metric\":\"%s\",%s,%s,\"dps\":{",
            m_metric, to_json_tags(buf1, sizeof(buf1)),
            to_json_aggregate_tags(buf2, sizeof(buf2)));

        for (const DataPointPair& dp: m_dps)
        {
            if (size <= n) break;
            if (buff[n-1] != '{')
            {
                n += snprintf(buff+n, size-n, ",");
            }
            n += snprintf(buff+n, size-n, "\"%" PRIu64 "\":%.16lf", dp.first, dp.second);
            while ((buff[n-1] == '0') && (buff[n-2] != '.') && (buff[n-2] != ':')) buff[--n] = 0;
        }

        n += snprintf(buff+n, size-n, "}}");

        return n;
    }
};


class Aggregator;
class Downsampler;
class RateCalculator;


class Query : public TagOwner
{
public:
    Query(JsonMap& map, StringBuffer& strbuf);
    Query(JsonMap& map, TimeRange& range, StringBuffer& strbuf, bool ms);
    virtual ~Query();

    inline bool in_range(const TimeRange& range) const
    {
        return m_time_range.has_intersection(range);
    }

    // return 0 if dp was in range and added;
    // return <0 if dp was too early;
    // return >0 if dp was too late, indicating the rest
    //           of the dps may be skipped;
    int add_data_point(DataPointPair& dp, DataPointVector& dps, Downsampler *downsampler);
    void get_query_tasks(std::vector<QueryTask*>& qtv);

    void execute(std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void execute_in_parallel(std::vector<QueryResults*>& results, StringBuffer& strbuf);

    char *c_str(char *buff, size_t size) const;

private:
    Query(const Query&) = delete;

    inline bool in_range(Timestamp tstamp) const
    {
        return m_time_range.in_range(tstamp);
    }

    void create_query_results(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void aggregate(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void calculate_rate(std::vector<QueryResults*>& results);

    TimeRange m_time_range;

    bool m_ms;  // milli-second resolution?
    char *m_metric;
    char *m_aggregate;
    char *m_downsample;

    Aggregator *m_aggregator;
    RateCalculator *m_rate_calculator;
};


class QueryTask : public Recyclable
{
public:
    void perform();

    Tag *get_tags();
    Tag *get_cloned_tags(StringBuffer& strbuf);

    inline DataPointVector& get_dps()
    {
        return m_dps;
    }

private:
    friend class MemoryManager;
    friend class Query;
    friend class QueryExecutor;

    bool recycle();

    TimeRange m_time_range;
    Downsampler *m_downsampler;
    std::vector<TimeSeries*> m_tsv;
    DataPointVector m_dps;  // results before aggregation
    QueryResults m_results; // results after aggregation
    volatile std::atomic<bool> m_done;
};


// this is a singleton
class QueryExecutor : public Stoppable
{
public:
    static void init();

    inline static QueryExecutor *inst()
    {
        return m_instance;
    }

    void submit_query(QueryTask *task);
    void shutdown(ShutdownRequest request = ShutdownRequest::ASAP);

    static bool http_get_api_config_filters_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_query_handler(HttpRequest& request, HttpResponse& response);
    static bool http_post_api_query_handler(HttpRequest& request, HttpResponse& response);

    static bool perform_query(TaskData& data);

private:
    friend class Query;

    QueryExecutor();
    static bool prepare_response(std::vector<QueryResults*>& results, HttpResponse& response);

    std::mutex m_lock;
    TaskScheduler m_executors;

    static QueryExecutor *m_instance;
};


}
