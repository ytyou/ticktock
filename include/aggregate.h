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

#include <cmath>
#include <functional>
#include <queue>
#include "recycle.h"
#include "type.h"
#include "http.h"


namespace tt
{


class QueryResults;


#if 0
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
#endif


// Base class of the Aggregators.
class Aggregator : public Recyclable
{
public:
    static Aggregator *create(const char *aggregate);

    inline virtual bool is_none() const { return false; }
    void aggregate(QueryResults *results);
    virtual void aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf)
    { ASSERT(false); };     // should not call this

    static bool http_get_api_aggregators_handler(HttpRequest& request, HttpResponse& response);

protected:
    Aggregator() {};

    virtual void init() {}
    virtual void add_data_point(DataPointPair& dp) {}
    virtual bool has_data() { return false; }
    virtual void add_aggregated(Timestamp ts, DataPointVector& dps) {}

private:
    Aggregator(const Aggregator&) = delete;
    void merge(std::vector<std::reference_wrapper<DataPointVector>>& src, DataPointVector& dst);
};


class AggregatorNone : public Aggregator
{
public:
    inline bool is_none() const override { return true; }
    void aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf) override;
};


class AggregatorAvg : public Aggregator
{
protected:
    void init() override { m_count = 0; m_sum = 0.0; }
    void add_data_point(DataPointPair& dp) override { m_count++; m_sum += dp.second; }
    bool has_data() override { return m_count > 0; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, m_sum/(double)m_count); }

private:
    int m_count;
    double m_sum;
};


/* Select the smallest n values.
 *
 * This aggregator should not be used to perform downsampling.
 */
class AggregatorBottom : public Aggregator
{
public:
    inline void set_n(short n) { m_n = n; }
    inline bool is_none() const override { return true; }
    void aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf) override;

private:
    short m_n;
};


class AggregatorCount : public Aggregator
{
protected:
    void init() override { m_count = 0; }
    void add_data_point(DataPointPair& dp) override { m_count++; }
    bool has_data() override { return m_count > 0; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, (double)m_count); }

private:
    int m_count;
};


class AggregatorDev : public Aggregator
{
public:
    static double stddev(const std::vector<double>& values);

protected:
    void init() override { m_values.clear(); m_has_data = false; }
    void add_data_point(DataPointPair& dp) override
    { if (!std::isnan(dp.second)) m_values.push_back(dp.second); m_has_data = true; }
    bool has_data() override { return m_has_data; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, stddev(m_values)); }

private:
    std::vector<double> m_values;
    bool m_has_data;
};


class AggregatorMax : public Aggregator
{
protected:
    void init() override { m_max = std::numeric_limits<double>::min(); m_has_data = false; }
    void add_data_point(DataPointPair& dp) override
    { m_max = std::max(m_max, dp.second); m_has_data = true; }
    bool has_data() override { return m_has_data; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, m_max); }

private:
    double m_max;
    bool m_has_data;
};


class AggregatorMin : public Aggregator
{
protected:
    void init() override { m_min = std::numeric_limits<double>::max(); m_has_data = false; }
    void add_data_point(DataPointPair& dp) override
    { m_min = std::min(m_min, dp.second); m_has_data = true; }
    bool has_data() override { return m_has_data; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, m_min); }

private:
    double m_min;
    bool m_has_data;
};


class AggregatorPercentile : public Aggregator
{
public:
    void set_quantile(double quantile);
    double percentile(std::vector<double>& values) const;

protected:
    void init() override { m_values.clear(); m_has_data = false; }
    void add_data_point(DataPointPair& dp) override
    { if (!std::isnan(dp.second)) m_values.push_back(dp.second); m_has_data = true; }
    bool has_data() override { return m_has_data; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, percentile(m_values)); }

private:
    double index(int length) const;

    double m_quantile;
    std::vector<double> m_values;
    bool m_has_data;
};


class AggregatorSum : public Aggregator
{
protected:
    void init() override { m_sum = 0.0; m_has_data = false; }
    void add_data_point(DataPointPair& dp) override
    { m_sum += dp.second; m_has_data = true; }
    bool has_data() override { return m_has_data; }
    void add_aggregated(Timestamp ts, DataPointVector& dps) override
    { dps.emplace_back(ts, m_sum); }

private:
    double m_sum;
    bool m_has_data;
};


/* Select the biggest n values.
 *
 * This aggregator should not be used to perform downsampling.
 */
class AggregatorTop : public Aggregator
{
public:
    inline void set_n(short n) { m_n = n; }
    inline bool is_none() const override { return true; }
    void aggregate(const char *metric, std::vector<QueryTask*>& qtv, std::vector<QueryResults*>& results, StringBuffer& strbuf) override;

private:
    short m_n;
};


}
