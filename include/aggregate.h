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

#include <functional>
#include <queue>
#include "recycle.h"
#include "type.h"
#include "http.h"


namespace tt
{


class QueryResults;


typedef DataPointVector::iterator it_t;
typedef std::pair<it_t,it_t> it_pair_t;

// It compares the timestamp of the data point pointed to by the
// two iterators.
struct it_pair_greater
{
    inline bool
    operator()(const it_pair_t& lhs, const it_pair_t& rhs) const
    {
        return (*lhs.first > *rhs.first);
    }
};


// This class implements a priority queue that is used by Aggregators
// to merge multiple DataPointVectors. The priority queue stores a pair
// of DataPointVector::iterators; the first one is the current position
// and the second one is the end position.
class PQ
{
public:
    PQ(std::vector<std::reference_wrapper<DataPointVector>>& vov)
    {
        for (DataPointVector& v: vov)
        {
            auto p = std::make_pair(v.begin(), v.end());
            if (p.first != p.second) m_pq.push(p);
        }
    }

    inline bool empty() const
    {
        return m_pq.empty();
    }

    // NOTE: Caller is responsible for making sure the PQ is not empty
    //       before calling this method.
    DataPointPair& next()
    {
        auto top = m_pq.top();
        DataPointPair& data = *top.first;
        m_pq.pop();

        if (++top.first != top.second)
        {
            m_pq.push(top);
        }

        return data;
    }

private:
    std::priority_queue<it_pair_t, std::vector<it_pair_t>, it_pair_greater> m_pq;
};


// Base class of the Aggregators.
class Aggregator : public Recyclable
{
public:
    static Aggregator *create(const char *aggregate);

    inline virtual bool is_none() const { return false; }
    void aggregate(QueryResults *results);

    static bool http_get_api_aggregators_handler(HttpRequest& request, HttpResponse& response);

protected:
    Aggregator() {};

private:
    Aggregator(const Aggregator&) = delete;
    virtual void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst) = 0;
};


class AggregatorNone : public Aggregator
{
public:
    inline bool is_none() const { return true; }
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst) {};
};


class AggregatorAvg : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


class AggregatorCount : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


class AggregatorDev : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
    static double stddev(const std::vector<double>& values);
};


class AggregatorMax : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


class AggregatorMin : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


class AggregatorPercentile : public Aggregator
{
public:
    void set_quantile(double quantile);
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
    double percentile(const std::vector<double>& values) const;

private:
    double index(int length) const;

    double m_quantile;
};


class AggregatorSum : public Aggregator
{
public:
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


}
