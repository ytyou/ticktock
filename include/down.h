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
#include "range.h"
#include "recycle.h"
#include "rollup.h"
#include "type.h"


namespace tt
{


class QueryResults;


enum class DownsampleFillPolicy : unsigned char
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

    virtual RollupType get_rollup_type() const = 0;
    virtual void add_data_point(DataPointPair& dp, DataPointVector& dps) = 0;
    virtual void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) {};
    virtual void add_last_point(DataPointVector& dps) {};
    virtual bool is_empty() const { return true; }  // any data collected so far?
    void fill_if_needed(DataPointVector& dps);

    inline virtual bool recycle()
    {
        m_last_tstamp = TT_INVALID_TIMESTAMP;
        return true;
    }

    inline Timestamp get_interval() const
    {
        return m_interval;
    }

    inline void set_interval(Timestamp interval)
    {
        m_interval = interval;
    }

    inline const TimeRange& get_time_range() const
    {
        return m_time_range;
    }

    void update_range(const TimeRange& range);

    // this range will include those trailing dps that should be
    // included AFTER stepping-down
    inline const TimeRange get_expanded_range() const
    {
        TimeRange range(m_time_range);
        if (range.get_to() % m_interval == 0)
            range.set_to(range.get_to() + m_interval - 1);
        return range;
    }

    static bool is_downsampler(const char *str);

protected:
    Downsampler(const Downsampler&) = delete;

    void initialize(char *interval, char *fill, TimeRange& range, bool ms);
    void fill_to(Timestamp to, DataPointVector& dps);
    Timestamp step_up(Timestamp tstamp) const;
    Timestamp step_down(Timestamp tstamp) const;

    inline Timestamp resolution(Timestamp tstamp)
    {
        return (m_ms ? to_ms(tstamp) : to_sec(tstamp));
    }

    // unit of all 3 Timestamps consistent with g_tstamp_resolution_ms
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
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) override;
    void add_last_point(DataPointVector& dps) override;

    inline bool is_empty() const override { return m_count == 0; }
    RollupType get_rollup_type() const override { return RollupType::RU_AVG; }

    inline virtual void init()
    {
        m_sum = 0;
        m_count = 0L;
    }

private:
    double m_sum;
    unsigned long m_count;
};


class DownsamplerCount : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_CNT; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) override;
};


class DownsamplerDev : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_NONE; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_last_point(DataPointVector& dps) override;
    inline bool is_empty() const override { return m_count == 0; }

    inline virtual void init()
    {
        m_count = 0L;
        m_mean = m_m2 = 0;
    }

    inline double calc_dev() const
    {
        ASSERT(m_count != 0L);
        return std::sqrt(m_m2 / (double)m_count);
    }

private:
    double m_mean, m_m2;
    unsigned long m_count;
};


class DownsamplerFirst : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_NONE; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
};


class DownsamplerLast : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_NONE; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
};


class DownsamplerMax : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_MAX; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) override;
};


class DownsamplerMin : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_MIN; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) override;
};


class DownsamplerPercentile : public Downsampler
{
public:
    RollupType get_rollup_type() const override { return RollupType::RU_NONE; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_last_point(DataPointVector& dps) override;
    inline bool is_empty() const override { return m_values.empty(); }

    bool recycle();
    double calc_percentile();

    inline virtual void init()
    {
        m_values.clear();
    }

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
    RollupType get_rollup_type() const override { return RollupType::RU_SUM; }
    void add_data_point(DataPointPair& dp, DataPointVector& dps) override;
    void add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps) override;
};


}
