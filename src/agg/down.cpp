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

#include <algorithm>
#include <cctype>
#include <cmath>
#include <vector>
#include "aggregate.h"
#include "down.h"
#include "memmgr.h"
#include "utils.h"
#include "logger.h"


namespace tt
{


Downsampler::Downsampler() :
    m_start(0L),
    m_interval(0L),
    m_fill(DownsampleFillPolicy::DFP_NONE),
    m_last_tstamp(0L),
    m_fill_value(0.0),
    m_ms(false)
{
}

void
Downsampler::init(char *interval, char *fill, TimeRange& range, bool ms)
{
    ASSERT(interval != nullptr);

    m_start = range.get_from();
    m_last_tstamp = 0L;
    m_time_range = range;
    m_fill = DownsampleFillPolicy::DFP_NONE;
    m_ms = ms;

    if (interval == nullptr)
    {
        m_interval = (g_tstamp_resolution_ms ? 60000 : 60); // default to 1 minute
        Logger::error("null interval passed into Downsampler::init()");
    }
    else
    {
        // interval
        long factor = 1;

        if (! ends_with(interval, "ms"))
        {
            char& unit = std::string(interval).back();

            switch (unit)
            {
                case 's':   factor = 1;         break;
                case 'm':   factor = 60;        break;
                case 'h':   factor = 3600;      break;
                case 'd':   factor = 86400;     break;
                case 'w':   factor = 604800;    break;
                default:    factor = 1;         break;
            };

            if (g_tstamp_resolution_ms) factor *= 1000;
        }

        m_interval = std::stol(interval) * factor;
    }

    ASSERT(m_interval > 0);

    m_time_range.set_from(m_time_range.get_from() - (m_time_range.get_from() % m_interval));
    m_time_range.set_to(m_time_range.get_to() - (m_time_range.get_to() % m_interval));

    // fill policy
    if (fill != nullptr)
    {
        if (std::strcmp("nan", fill) == 0)
        {
            m_fill = DownsampleFillPolicy::DFP_NAN;
            m_fill_value = NAN;
        }
        else if (std::strcmp("null", fill) == 0)
        {
            m_fill = DownsampleFillPolicy::DFP_NULL;
            m_fill_value = NAN;
        }
        else if (std::strcmp("zero", fill) == 0)
        {
            m_fill = DownsampleFillPolicy::DFP_ZERO;
            m_fill_value = 0.0;
        }
    }
}

Downsampler *
Downsampler::create(const char *downsample, TimeRange& range, bool ms)
{
    if (downsample == nullptr)
    {
        return nullptr;
    }

    char tmp[64];
    std::strncpy(tmp, downsample, sizeof(tmp));
    std::vector<char*> tokens;

    tokenize(tmp, '-', tokens);
    ASSERT(tokens.size() >= 2);

    char *interval = tokens[0];
    char *function = tokens[1];
    char *fill = (tokens.size() > 2) ? tokens[2] : nullptr;

    Downsampler *downsampler = nullptr;

    switch (function[0])
    {
        case 'a':
            ASSERT(strcmp(function, "avg") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_AVG);
            break;

        case 'c':
            ASSERT(strcmp(function, "count") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_COUNT);
            break;

        case 'd':
            ASSERT(strcmp(function, "dev") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_DEV);
            break;

        case 'f':
            ASSERT(strcmp(function, "first") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_FIRST);
            break;

        case 'l':
            ASSERT(strcmp(function, "last") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_LAST);
            break;

        case 'm':
            if (function[1] == 'a')
            {
                ASSERT(strcmp(function, "max") == 0);
                downsampler =
                    (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_MAX);
            }
            else
            {
                ASSERT(strcmp(function, "min") == 0);
                downsampler =
                    (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_MIN);
            }
            break;

        case 'p':
            ASSERT(std::isdigit(function[1]) != 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_PT);
            ((DownsamplerPercentile*)downsampler)->set_quantile(std::atoi(function+1));
            break;

        case 's':
            ASSERT(strcmp(function, "sum") == 0);
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_SUM);
            break;

        default:
            if (strcmp(function, "none") != 0)
                Logger::debug("Unknown downsampler ignored: %s", function);
            break;
    }

    if (downsampler != nullptr)
    {
        downsampler->init(interval, fill, range, ms);
    }

    return downsampler;
}

bool
Downsampler::is_downsampler(const char *str)
{
    if (str == nullptr)
    {
        return false;
    }
    else
    {
        return (std::strchr(str, '-') != nullptr);
    }
}

void
Downsampler::fill_to(Timestamp to, DataPointVector& dps)
{
    if (m_fill == DownsampleFillPolicy::DFP_NONE) return;

    Timestamp start;

    if (m_last_tstamp == 0L)
    {
        start = m_time_range.get_from();
        if (start < m_start) start += m_interval;
    }
    else
    {
        start = m_last_tstamp + m_interval;
    }

    for (Timestamp tstamp = start; tstamp < to; tstamp += m_interval)
    {
        dps.emplace_back(resolution(tstamp), m_fill_value);
    }
}

void
Downsampler::fill_if_needed(DataPointVector& dps)
{
    add_last_point(dps);

    if (m_fill != DownsampleFillPolicy::DFP_NONE)
    {
        Timestamp last_tstamp;

        if (dps.empty())
        {
            last_tstamp = m_time_range.get_from();
        }
        else
        {
            DataPointPair& last_dp = dps.back();
            last_tstamp = last_dp.first;
        }

        for (Timestamp tstamp = last_tstamp + m_interval; tstamp <= m_time_range.get_to(); tstamp += m_interval)
        {
            dps.emplace_back(resolution(tstamp), m_fill_value);
        }
    }
}


bool
DownsamplerAvg::recycle()
{
    m_values.clear();
    m_values.shrink_to_fit();
    return Downsampler::recycle();
}

double
DownsamplerAvg::calc_avg() const
{
    ASSERT(! m_values.empty());

    double sum = 0.0;

    for (double v: m_values)
    {
        sum += v;
    }

    return sum / (double)m_values.size();
}

void
DownsamplerAvg::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        m_values.push_back(dp.second);
    }
    else
    {
        if (m_last_tstamp != 0L)
        {
            dps.emplace_back(resolution(m_last_tstamp), calc_avg());
            m_values.clear();
        }

        fill_to(curr_tstamp, dps);
        m_values.push_back(dp.second);
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerAvg::add_last_point(DataPointVector& dps)
{
    if (! m_values.empty())
    {
        dps.emplace_back(resolution(m_last_tstamp), calc_avg());
        m_values.clear();
    }
}


void
DownsamplerCount::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(! dps.empty());
        dps.back().second++;
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), 1);
        m_last_tstamp = curr_tstamp;
    }
}


bool
DownsamplerDev::recycle()
{
    m_values.clear();
    m_values.shrink_to_fit();
    return Downsampler::recycle();
}

void
DownsamplerDev::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        m_values.push_back(dp.second);
    }
    else
    {
        if (m_last_tstamp != 0L)
        {
            dps.emplace_back(resolution(m_last_tstamp), calc_dev());
            m_values.clear();
        }

        fill_to(curr_tstamp, dps);
        m_values.push_back(dp.second);
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerDev::add_last_point(DataPointVector& dps)
{
    if (! m_values.empty())
    {
        dps.emplace_back(resolution(m_last_tstamp), calc_dev());
        m_values.clear();
    }
}


void
DownsamplerFirst::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp != m_last_tstamp)
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), dp.second);
        m_last_tstamp = curr_tstamp;
    }
}


void
DownsamplerLast::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second = dp.second;
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), dp.second);
        m_last_tstamp = curr_tstamp;
    }
}


void
DownsamplerMax::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second = std::max(dps.back().second, dp.second);
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), dp.second);
        m_last_tstamp = curr_tstamp;
    }
}


void
DownsamplerMin::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second = std::min(dps.back().second, dp.second);
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), dp.second);
        m_last_tstamp = curr_tstamp;
    }
}


bool
DownsamplerPercentile::recycle()
{
    m_values.clear();
    m_values.shrink_to_fit();
    return Downsampler::recycle();
}

double
DownsamplerPercentile::calc_percentile()
{
    if (m_values.size() > 1)
    {
        // TODO: optimize for bigger size vectors
        //       consider using std::nth_element()
        std::sort(m_values.begin(), m_values.end());
    }

    return m_aggregator.percentile(m_values);
}

void
DownsamplerPercentile::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        m_values.push_back(dp.second);
    }
    else
    {
        if (m_last_tstamp != 0L)
        {
            dps.emplace_back(resolution(m_last_tstamp), calc_percentile());
            m_values.clear();
        }

        fill_to(curr_tstamp, dps);
        m_values.push_back(dp.second);
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerPercentile::add_last_point(DataPointVector& dps)
{
    if (! m_values.empty())
    {
        dps.emplace_back(resolution(m_last_tstamp), calc_percentile());
        m_values.clear();
    }
}


void
DownsamplerSum::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT(m_last_tstamp <= curr_tstamp);

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second += dp.second;
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(resolution(curr_tstamp), dp.second);
        m_last_tstamp = curr_tstamp;
    }
}


}
