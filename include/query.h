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

#include <map>
#include <vector>
#include "json.h"
#include "range.h"
#include "http.h"
#include "recycle.h"
#include "strbuf.h"
#include "stop.h"
#include "sync.h"
#include "tag.h"
#include "type.h"


namespace tt
{


class DataPointContainer;
class PageInMemory;
class QueryTask;
class TimeSeries;
class Tsdb;


class QueryResults : public TagOwner, public Recyclable
{
// TODO: make them private
public:
    const char *m_metric;
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

    bool recycle() override
    {
        // we don't own 'metric' so don't free it
        m_metric = nullptr;
        m_dps.clear();
        m_dps.shrink_to_fit();
        m_aggregate_tags.clear();
        m_aggregate_tags.shrink_to_fit();
        m_qtv.clear();
        m_qtv.shrink_to_fit();
        TagOwner::recycle();
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

/*
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
*/

    char *to_json_aggregate_tags(char *buff, int size) const
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

    int to_json(char *buff, int size) const
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
            n += snprintf(buff+n, size-n, "\"%" PRIu64 "\":%.16lf", dp.first, dp.second);
            if (n > size) n = size;
            while ((buff[n-1] == '0') && (buff[n-2] != '.') && (buff[n-2] != ':')) buff[--n] = 0;
        }

        if (size > n)
            n += snprintf(buff+n, size-n, "}}");

        return (n <= size) ? n : size;
    }

private:
    char *to_json_tags(char *buff, int size) const
    {
        int n = snprintf(buff, size, "\"tags\":");
        ASSERT(size > n);
        KeyValuePair::to_json(m_tags, buff+n, size-n);
        return buff;
    }

    std::vector<char*> m_aggregate_tags;
};


class Aggregator;
class Downsampler;
class RateCalculator;


class Query : public Serializable, public TagOwner
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
    void get_query_tasks(std::vector<QueryTask*>& qtv, std::vector<Tsdb*> *tsdbs);

    void execute(std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void execute_in_parallel(std::vector<QueryResults*>& results, StringBuffer& strbuf);

    inline int get_errno() const
    { return m_errno; }

    static uint64_t get_dp_count();

    inline size_t c_size() const override { return 1024; }
    const char *c_str(char *buff) const override;

private:
    Query(const Query&) = delete;

    inline int in_range(Timestamp tstamp) const
    {
        return m_time_range.in_range(tstamp);
    }

    QueryResults *create_one_query_results(StringBuffer& strbuf);
    void create_query_results(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void aggregate(std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf);
    void calculate_rate(std::vector<QueryResults*>& results);

    TimeRange m_time_range;

    bool m_ms;  // milli-second resolution?
    bool m_explicit_tags;
    int m_errno;
    const char *m_metric;
    const char *m_aggregate;
    const char *m_downsample;

    Tag *m_non_grouping_tags;
    Aggregator *m_aggregator;
    RateCalculator *m_rate_calculator;
};


class QueryTask : public Recyclable
{
public:
    QueryTask();
    void perform();
    void perform(TimeSeriesId id);

    void init(std::vector<Tsdb*> *tsdbs, const TimeRange& range);   // used by Tsdb::compact()

    // return max/min value of the last n dps in m_dps[]
    double get_max(int n) const;
    double get_min(int n) const;

    Tag *get_tags();
    Tag_v2& get_v2_tags();
    Tag *get_cloned_tags(StringBuffer& strbuf);

    inline int get_errno() const
    { return m_errno; }

    inline DataPointVector& get_dps()
    {
        return m_dps;
    }

    inline void set_signal(CountingSignal *signal)
    {
        m_signal = signal;
    }

    struct compare_less
    {
        bool operator()(const QueryTask *t1, const QueryTask *t2)
        {
            return t1->get_max(3) < t2->get_max(3);
        }
    };

    struct compare_greater
    {
        bool operator()(const QueryTask *t1, const QueryTask *t2)
        {
            return t1->get_min(3) > t2->get_min(3);
        }
    };

    void init() override;
    bool recycle() override;

private:
    friend class MemoryManager;
    friend class Query;
    friend class QueryExecutor;

    void query_with_ooo(std::vector<DataPointContainer*>& data);
    void query_without_ooo(std::vector<DataPointContainer*>& data);

    TimeRange m_time_range;
    Downsampler *m_downsampler;
    TimeSeries *m_ts;
    std::vector<Tsdb*> *m_tsdbs;
    DataPointVector m_dps;  // results before aggregation
    QueryResults m_results; // results after aggregation
    CountingSignal *m_signal;   // we don't own this, do not free it
    int m_errno;
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
    static size_t get_pending_task_count(std::vector<size_t> &counts);

private:
    friend class Query;

    QueryExecutor();
    static bool prepare_response(std::vector<QueryResults*>& results, HttpResponse& response, int error);

    std::mutex m_lock;
    TaskScheduler m_executors;

    static QueryExecutor *m_instance;
};


class DataPointContainer : public Recyclable
{
public:
/*
    void init(PageInfo *info)
    {
        m_dps.clear();
        m_dps.reserve(700);
        m_out_of_order = info->is_out_of_order();
        m_page_index = info->get_global_page_index();
        info->get_all_data_points(m_dps);
    }

    void init(struct page_info_on_disk *header)
    {
        ASSERT(header != nullptr);
        m_out_of_order = header->is_out_of_order();
        m_page_index = header->get_global_page_index();
    }
*/

    void init() override
    {
        m_dps.clear();
        m_dps.reserve(g_page_size / 4);
        m_out_of_order = false;
        m_page_index = 0;
    }

    bool recycle() override
    {
        m_dps.clear();
        m_dps.shrink_to_fit();
        return true;
    }

    inline size_t size() const { return m_dps.size(); }
    inline DataPointPair& get_data_point(int i) { return m_dps[i]; }
    inline DataPointPair& get_last_data_point() { return m_dps.back(); }
    inline PageIndex get_page_index() const { return m_page_index; }
    inline bool is_out_of_order() const { return m_out_of_order; }
    inline bool is_empty() const { return m_dps.empty(); }

    void set_out_of_order(bool ooo) { m_out_of_order = ooo; }
    void set_page_index(PageIndex idx) { m_page_index = idx; }

    void collect_data(PageInMemory *page);
    void collect_data(Timestamp from, struct tsdb_header *tsdb_header, struct page_info_on_disk *page_header, void *page);

private:
    bool m_out_of_order;
    PageIndex m_page_index;
    DataPointVector m_dps;
};


}
