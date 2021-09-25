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

#include "range.h"
#include "recycle.h"
#include "type.h"


namespace tt
{


class QueryResults;


enum class DownsampleFillPolicy
{
    DFP_NONE = 0,
    DFP_NAN  = 1,
    DFP_NULL = 2,
    DFP_ZERO = 3
};


// Downsampler interface.
class Downsampler : public Recyclable
{
public:
    Downsampler();
    static Downsampler *create(const char *downsample, TimeRange& range, bool ms);

    virtual void add_data_point(DataPointPair& dp, DataPointVector& dps) = 0;
    virtual void add_last_point(DataPointVector& dps) {};
    void fill_if_needed(DataPointVector& dps);

    inline virtual bool recycle()
    {
        m_last_tstamp = 0L;
        return true;
    }

    inline Timestamp step_down(Timestamp tstamp) const
    {
        return m_all ? m_start : (tstamp - (tstamp % m_interval));
    }

    static bool is_downsampler(const char *str);

protected:
    Downsampler(const Downsampler&) = delete;

    void init(char *interval, char *fill, TimeRange& range, bool ms);
    void fill_to(Timestamp to, DataPointVector& dps);

    inline Timestamp resolution(Timestamp tstamp)
    {
        return (m_ms ? to_ms(tstamp) : to_sec(tstamp));
    }

    Timestamp m_start;  // original query start, before stepping down
    TimeRange m_time_range;
    Timestamp m_interval;
    DownsampleFillPolicy m_fill;

    Timestamp m_last_tstamp;
    double m_fill_value;
    bool m_ms;  // output milli-second timestamp resolution?
    bool m_all; // interval is 'all'?
};


class DownsamplerAvg : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
    void add_last_point(DataPointVector& dps);

    bool recycle();
    double calc_avg() const;

private:
    std::vector<double> m_values;
};


class DownsamplerCount : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


class DownsamplerDev : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
    void add_last_point(DataPointVector& dps);

    bool recycle();

    inline double calc_dev() const
    {
        ASSERT(! m_values.empty());
        return AggregatorDev::stddev(m_values);
    }

private:
    std::vector<double> m_values;
};


class DownsamplerFirst : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


class DownsamplerLast : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


class DownsamplerMax : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


class DownsamplerMin : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


class DownsamplerPercentile : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
    void add_last_point(DataPointVector& dps);

    bool recycle();
    double calc_percentile();

    inline void set_quantile(double quantile)
    {
        m_aggregator.set_quantile(quantile);
    }

private:
    AggregatorPercentile m_aggregator;
    std::vector<double> m_values;
};


class DownsamplerSum : public Downsampler
{
public:
    void add_data_point(DataPointPair& dp, DataPointVector& dps);
};


}
