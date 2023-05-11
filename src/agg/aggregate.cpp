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

#include <cmath>
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

    // TODO: switch (aggregate[0])
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

void
Aggregator::aggregate(QueryResults *results)
{
    if (results == nullptr)
    {
        Logger::error("nullptr passed into Aggregator::aggregate()");
        return;
    }

    std::vector<std::reference_wrapper<DataPointVector>> vv;

    for (QueryTask *qt: results->m_qtv)
    {
        vv.push_back(qt->get_dps());
    }

    merge(vv, results->m_dps);
}


void
AggregatorNone::aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
{
    for (QueryTask *qt: qtv)
    {
        QueryResults *result =
            (QueryResults*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_RESULTS);
        result->m_metric = metric;
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->m_dps.insert(result->m_dps.end(), dps.begin(), dps.end());  // TODO: how to avoid copy?
        dps.clear();
    }
}


void
AggregatorAvg::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    double sum;
    int count = 0;
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp == curr_tstamp)
        {
            count++;
            sum += dp.second;
        }
        else
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                ASSERT(count > 0);
                dst.emplace_back(prev_tstamp, sum/(double)count);
            }

            count = 1;
            sum = dp.second;
            prev_tstamp = curr_tstamp;
        }
    }

    if (prev_tstamp != 0L)
    {
        ASSERT(count > 0);
        dst.emplace_back(prev_tstamp, sum/(double)count);
    }
}


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
        result->m_metric = metric;
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->m_dps.insert(result->m_dps.end(), dps.begin(), dps.end());  // TODO: how to avoid copy?
        dps.clear();
    }
}


void
AggregatorCount::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    int count = 0;
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp == curr_tstamp)
        {
            count++;
        }
        else
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                ASSERT(count > 0);
                dst.emplace_back(prev_tstamp, (double)count);
            }

            count = 1;
            prev_tstamp = curr_tstamp;
        }
    }

    if (prev_tstamp != 0L)
    {
        ASSERT(count > 0);
        dst.emplace_back(prev_tstamp, (double)count);
    }
}


void
AggregatorDev::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    std::vector<double> values;
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp != curr_tstamp)
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                dst.emplace_back(prev_tstamp, stddev(values));
            }

            prev_tstamp = curr_tstamp;
            values.clear();
        }

        if (! std::isnan(dp.second))
        {
            values.push_back(dp.second);
        }
    }

    if (prev_tstamp != 0L)
    {
        dst.emplace_back(prev_tstamp, stddev(values));
    }
}

double
AggregatorDev::stddev(const std::vector<double>& values)
{
    if (values.empty())
    {
        return std::nan("");
    }
    else if (values.size() == 1)
    {
        return 0.0;
    }

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
AggregatorMax::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    double max = std::numeric_limits<double>::min();
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp == curr_tstamp)
        {
            max = std::max(max, dp.second);
        }
        else
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                dst.emplace_back(prev_tstamp, max);
            }

            max = dp.second;
            prev_tstamp = curr_tstamp;
        }
    }

    if (prev_tstamp != 0L)
    {
        dst.emplace_back(prev_tstamp, max);
    }
}


void
AggregatorMin::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    double min = std::numeric_limits<double>::max();
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp == curr_tstamp)
        {
            min = std::min(min, dp.second);
        }
        else
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                dst.emplace_back(prev_tstamp, min);
            }

            min = dp.second;
            prev_tstamp = curr_tstamp;
        }
    }

    if (prev_tstamp != 0L)
    {
        dst.emplace_back(prev_tstamp, min);
    }
}


void
AggregatorPercentile::set_quantile(double quantile)
{
    m_quantile = quantile;
    ASSERT(0 <= m_quantile);

    while (m_quantile > 100.0)
    {
        m_quantile /= (double)10.0;
    }
}

void
AggregatorPercentile::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    std::vector<double> values;
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp != curr_tstamp)
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                dst.emplace_back(prev_tstamp, percentile(values));
            }

            prev_tstamp = curr_tstamp;
            values.clear();
        }

        if (! std::isnan(dp.second))
        {
            values.push_back(dp.second);
        }
    }

    if (prev_tstamp != 0L)
    {
        dst.emplace_back(prev_tstamp, percentile(values));
    }
}

double
AggregatorPercentile::percentile(const std::vector<double>& values) const
{
    int length = values.size();

    if (length == 0)
    {
        return std::nan("");
    }
    else if (length == 1)
    {
        return values[0];
    }
    else
    {
        double idx = index(length);
        double p;

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


void
AggregatorSum::merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst)
{
    PQ pq(src);
    double sum = 0;
    Timestamp prev_tstamp = 0L;

    while (! pq.empty())
    {
        DataPointPair& dp = pq.next();
        Timestamp curr_tstamp = dp.first;

        if (prev_tstamp == curr_tstamp)
        {
            sum += dp.second;
        }
        else
        {
            ASSERT(prev_tstamp < curr_tstamp);

            if (prev_tstamp != 0L)
            {
                dst.emplace_back(prev_tstamp, sum);
            }

            sum = dp.second;
            prev_tstamp = curr_tstamp;
        }
    }

    if (prev_tstamp != 0L)
    {
        dst.emplace_back(prev_tstamp, sum);
    }
}


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
        result->m_metric = metric;
        result->set_tags(qt->get_cloned_tags(strbuf));
        results.push_back(result);

        DataPointVector& dps = qt->get_dps();
        result->m_dps.insert(result->m_dps.end(), dps.begin(), dps.end());  // TODO: how to avoid copy?
        dps.clear();
    }
}


}
