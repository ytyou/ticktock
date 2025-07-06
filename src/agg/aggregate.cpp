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

#include <cstdlib>
#include <cstring>
#include <limits>
#include "aggregate.h"
#include "logger.h"
#include "memmgr.h"
#include "query.h"
#include "utils.h"


namespace tt
{


// Supported aggregators (Note that we support an p\d{2,3} percentile as aggregator);
// Should make it configurable. (TODO)
const char *SUPPORTED_AGGREGATORS =
    "[\"avg\",\"bottom1\",\"bottom3\",\"bottom5\",\"bottom9\",\"count\",\"dev\",\"first\",\"last\",\"max\",\"min\",\"none\",\"p50\",\"p90\",\"p95\",\"p98\",\"p99\",\"p999\",\"sum\",\"top1\",\"top3\",\"top5\",\"top9\"]";

Aggregator *
Aggregator::create(const char *aggregate)
{
    Aggregator *aggregator;

    if (aggregate == nullptr)
    {
        Logger::debug("aggregator not specified");
        aggregator =
            (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_NONE);
    }
    else
    {
        switch (aggregate[0])
        {
            case 'a':
                if (std::strcmp(aggregate, "avg") != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_AVG);
                break;

            case 'b':
                if (std::strncmp(aggregate, "bottom", 6) != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_BOTTOM);
                ((AggregatorBottom*)aggregator)->set_n(std::atoi(aggregate+6));
                break;

            case 'c':
                if (std::strcmp(aggregate, "count") != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_COUNT);
                break;

            case 'd':
                if (std::strcmp(aggregate, "dev") != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_DEV);
                break;

            case 'm':
                if (std::strcmp(aggregate, "max") == 0)
                {
                    aggregator =
                        (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_MAX);
                }
                else if (std::strcmp(aggregate, "min") == 0)
                {
                    aggregator =
                        (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_MIN);
                }
                else
                    throw std::runtime_error("unrecognized aggregator");
                break;

            case 'n':
                if (std::strcmp(aggregate, "none") != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_NONE);
                break;

            case 'p':
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_PT);
                ((AggregatorPercentile*)aggregator)->set_quantile(std::atoi(aggregate+1));
                break;

            case 's':
                if (std::strcmp(aggregate, "sum") != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_SUM);
                break;

            case 't':
                if (std::strncmp(aggregate, "top", 3) != 0)
                    throw std::runtime_error("unrecognized aggregator");
                aggregator =
                    (Aggregator*)MemoryManager::alloc_recyclable(RecyclableType::RT_AGGREGATOR_TOP);
                ((AggregatorTop*)aggregator)->set_n(std::atoi(aggregate+3));
                break;

            default:
                throw std::runtime_error("unrecognized aggregator");
        }
    }

    return aggregator;
}

bool
Aggregator::http_get_api_aggregators_handler(HttpRequest& request, HttpResponse& response)
{
    response.init(200, HttpContentType::JSON, strlen(SUPPORTED_AGGREGATORS), SUPPORTED_AGGREGATORS);
    return true;
}

// perform aggregation in the resultset passed in
void
Aggregator::aggregate(QueryResults *results)
{
    if (results == nullptr)
    {
        Logger::error("nullptr passed into Aggregator::aggregate()");
        return;
    }

    std::vector<std::reference_wrapper<DataPointVector>> vv;

    for (QueryTask *qt: results->get_query_tasks())
        vv.push_back(qt->get_dps());

    merge(vv, results->get_dps());
}

// data points in 'src' are sorted by the timestamp; they will be
// aggregated into 'dst';
void
Aggregator::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    int size = src.size();
    int idx[size];
    Timestamp ts = TT_INVALID_TIMESTAMP;

    for (int i = 0; i < size; i++)
        idx[i] = 0;

    for (DataPointVector& v: src)
    {
        if (! v.empty())
        {
            DataPointPair dp = v.front();
            if (ts > dp.first) ts = dp.first;
        }
    }

    while (ts != TT_INVALID_TIMESTAMP)
    {
        init();
        Timestamp next_ts = TT_INVALID_TIMESTAMP;

        for (int i = 0; i < size; i++)
        {
            DataPointVector& v = src[i];
            if (v.size() <= idx[i]) continue;

            DataPointPair& dp = v[idx[i]];

            if (dp.first == ts)
            {
                // accumulate data points for the current timestamp
                add_data_point(dp);
                idx[i]++;

                if (idx[i] < v.size())
                {
                    dp = v[idx[i]];
                    if (next_ts > dp.first)
                        next_ts = dp.first;
                }
            }
            else if (next_ts > dp.first)
                next_ts = dp.first;
        }

        if (! has_data()) break;

        // perform aggregation for the current timestamp;
        // it will generate one dp in the final resultset;
        add_aggregated(ts, dst);
        ts = next_ts;
    }
}


void
AggregatorNone::aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    for (QueryTask *qt: qtv)
    {
        QueryResults *result =
            (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
        result->set_metric(metric);
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->add_dps(dps);
        dps.clear();
    }
}


// keep bottom N resultsets
void
AggregatorBottom::aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::priority_queue<QueryTask*, std::vector<QueryTask*>, QueryTask::compare_greater> pq;

    for (QueryTask *qt: qtv)
        pq.push(qt);

    for (short i = 0; (i < m_n) && !pq.empty(); i++)
    {
        QueryTask *qt = pq.top();
        pq.pop();

        QueryResults *result =
            (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
        result->set_metric(metric);
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->add_dps(dps);
        dps.clear();
    }
}


void
AggregatorDev::add_data_point(DataPointPair& dp)
{
    if (!isnan(dp.second) && !isinf(dp.second))
        m_values.push_back(dp.second);
    m_has_data = true;
}

double
AggregatorDev::stddev(const std::vector<double>& values)
{
    if (values.empty())
        return std::nan("");
    else if (values.size() == 1)
        return 0.0;

    double old_mean = values[0];
    double new_mean = 0.0;
    double m2 = 0.0;
    int n;

    for (n = 1; n < values.size(); n++)
    {
        double x = values[n];

        new_mean = old_mean + (x - old_mean) / (double)(n+1);
        m2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
    }

    return std::sqrt(m2 / (double)n);
}


void
AggregatorMax::add_data_point(DataPointPair& dp)
{
    if (! m_has_data)
    {
        m_max = dp.second;
        m_has_data = true;
    }
    else
        m_max = std::max(m_max, dp.second);
}


void
AggregatorMin::add_data_point(DataPointPair& dp)
{
    if (! m_has_data)
    {
        m_min = dp.second;
        m_has_data = true;
    }
    else
        m_min = std::min(m_min, dp.second);
}


void
AggregatorPercentile::add_data_point(DataPointPair& dp)
{
    if (!isnan(dp.second) && !isinf(dp.second))
        m_values.push_back(dp.second);
    m_has_data = true;
}

void
AggregatorPercentile::set_quantile(double quantile)
{
    m_quantile = quantile;
    ASSERT(0 <= m_quantile);

    while (m_quantile > 100.0)
        m_quantile /= (double)10.0;
}

double
AggregatorPercentile::percentile(std::vector<double>& values) const
{
    int length = values.size();

    if (length == 0)
        return std::nan("");
    else if (length == 1)
        return values[0];
    else
    {
        double idx = index(length);
        double p;

        std::sort(values.begin(), values.end());

        if (idx < 1.0)
        {
            p = values[0];
        }
        else if (idx >= length)
        {
            p = values[length-1];
        }
        else
        {
            double fidx = std::floor(idx);
            int iidx = (int)idx;
            double diff = idx - fidx;

            double lower = values[iidx-1];
            double upper = values[iidx];
            p = lower + diff * (upper - lower);
        }

        return p;
    }
}

double
AggregatorPercentile::index(int length) const
{
    double p = m_quantile / 100.0;
    double minLimit = 0.0;
    double maxLimit = 1.0;
    return (p == minLimit) ? 0.0 : (p == maxLimit) ? length : p * (length + 1);
}


// keep top N resultsets
void
AggregatorTop::aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    std::priority_queue<QueryTask*, std::vector<QueryTask*>, QueryTask::compare_less> pq;

    for (QueryTask *qt: qtv)
        pq.push(qt);

    for (short i = 0; (i < m_n) && !pq.empty(); i++)
    {
        QueryTask *qt = pq.top();
        pq.pop();

        QueryResults *result =
            (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
        result->set_metric(metric);
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->add_dps(dps);
        dps.clear();
    }
}


}
