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

#include <algorithm>
#include <cctype>
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
    m_last_tstamp(TT_INVALID_TIMESTAMP),
    m_fill_value(0.0),
    m_ms(false),
    m_all(false)
{
}

void
Downsampler::initialize(char *interval, char *fill, TimeRange& range, bool ms)
{
    ASSERT(interval != nullptr);

    m_start = range.get_from();
    m_last_tstamp = TT_INVALID_TIMESTAMP;
    m_time_range = range;
    m_fill = DownsampleFillPolicy::DFP_NONE;
    m_ms = ms;
    m_all = false;

    if (interval == nullptr)
    {
        m_interval = (g_tstamp_resolution_ms ? 60000 : 60); // default to 1 minute
        Logger::error("null interval passed into Downsampler::init()");
    }
    else
    {
        // interval
        double factor = 1;

        if (ends_with(interval, "ms"))
            factor = g_tstamp_resolution_ms ? 1 : 0.001;
        else
        {
            //char& unit = std::string(interval).back();
            char unit = interval[std::strlen(interval)-1];  // last char

            switch (unit)
            {
                case 's':   factor = 1;         break;  // second
                case 'm':   factor = 60;        break;  // minute
                case 'h':   factor = 3600;      break;  // hour
                case 'd':   factor = 86400;     break;  // day
                case 'w':   factor = 604800;    break;  // week
                case 'l':   m_all = true;       break;  // all
                default:    throw std::runtime_error("unrecognized downsampler");
            };

            if (g_tstamp_resolution_ms) factor *= 1000;
        }

        m_interval = (Timestamp)((double)std::atoll(interval) * factor);
        if (m_interval == 0) m_interval = 1;
    }

    ASSERT(m_interval > 0);

    m_time_range.set_from(step_down(m_time_range.get_from()));
    if (m_all) m_interval = m_time_range.get_duration() + 1;

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

    init(); // initialize child downsamplers
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
    if (tokens.size() < 2) return nullptr;

    char *interval = tokens[0];
    char *function = tokens[1];
    char *fill = (tokens.size() > 2) ? tokens[2] : nullptr;

    Downsampler *downsampler = nullptr;

    switch (function[0])
    {
        case 'a':
            if (strcmp(function, "avg") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_AVG);
            break;

        case 'c':
            if (strcmp(function, "count") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_COUNT);
            break;

        case 'd':
            if (strcmp(function, "dev") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_DEV);
            break;

        case 'f':
            if (strcmp(function, "first") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_FIRST);
            break;

        case 'l':
            if (strcmp(function, "last") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_LAST);
            break;

        case 'm':
            if (function[1] == 'a')
            {
                if (strcmp(function, "max") != 0)
                    throw std::runtime_error("unrecognized downsampler");
                downsampler =
                    (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_MAX);
            }
            else
            {
                if (strcmp(function, "min") != 0)
                    throw std::runtime_error("unrecognized downsampler");
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
            if (strcmp(function, "sum") != 0)
                throw std::runtime_error("unrecognized downsampler");
            downsampler =
                (Downsampler*)MemoryManager::alloc_recyclable(RecyclableType::RT_DOWNSAMPLER_SUM);
            break;

        default:
            if (strcmp(function, "none") != 0)
                throw std::runtime_error("unrecognized downsampler");
            break;
    }

    if (downsampler != nullptr)
    {
        downsampler->initialize(interval, fill, range, ms);
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

    if (m_last_tstamp == TT_INVALID_TIMESTAMP)
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


void
DownsamplerAvg::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        // accumulate in current interval
        m_count++;
        m_sum += dp.second;
    }
    else
    {
        // start a new interval
        if (m_last_tstamp != TT_INVALID_TIMESTAMP)
        {
            ASSERT(m_count != 0L);
            dps.emplace_back(resolution(m_last_tstamp), m_sum/(double)m_count);
        }

        fill_to(curr_tstamp, dps);
        m_count = 1L;
        m_sum = dp.second;
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerAvg::add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps)
{
    ASSERT(entry != nullptr);
    Timestamp curr_tstamp = step_down(validate_resolution(entry->tstamp));
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        // accumulate in current interval
        m_count += entry->cnt;
        m_sum += entry->sum;
    }
    else
    {
        // start a new interval
        if (m_last_tstamp != TT_INVALID_TIMESTAMP)
        {
            ASSERT(m_count != 0L);
            dps.emplace_back(m_last_tstamp, m_sum/(double)m_count);
        }

        fill_to(curr_tstamp, dps);
        m_count = entry->cnt;
        m_sum = entry->sum;
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerAvg::add_last_point(DataPointVector& dps)
{
    if (m_count != 0L)
        dps.emplace_back(validate_resolution(m_last_tstamp), m_sum/(double)m_count);

    m_sum = 0;
    m_count = 0L;
}


void
DownsamplerCount::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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

void
DownsamplerCount::add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps)
{
    ASSERT(entry != nullptr);
    Timestamp curr_tstamp = step_down(validate_resolution(entry->tstamp));
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(! dps.empty());
        dps.back().second += entry->cnt;
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(curr_tstamp, (double)entry->cnt);
        m_last_tstamp = curr_tstamp;
    }
}


void
DownsamplerDev::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        m_count++;
        double new_value = dp.second;
        double new_mean = m_mean + (new_value - m_mean) / (double)m_count;
        m_m2 += (new_value - m_mean) * (new_value - new_mean);
        m_mean = new_mean;
    }
    else
    {
        if (m_last_tstamp != TT_INVALID_TIMESTAMP)
            dps.emplace_back(resolution(m_last_tstamp), calc_dev());

        fill_to(curr_tstamp, dps);
        m_m2 = 0.0;
        m_count = 1;
        m_mean = dp.second;
        m_last_tstamp = curr_tstamp;
    }
}

void
DownsamplerDev::add_last_point(DataPointVector& dps)
{
    if (m_count != 0L)
        dps.emplace_back(resolution(m_last_tstamp), calc_dev());

    m_count = 0L;
    m_mean = m_m2 = 0;
}


void
DownsamplerFirst::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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
DownsamplerMax::add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps)
{
    ASSERT(entry != nullptr);
    Timestamp curr_tstamp = step_down(validate_resolution(entry->tstamp));
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second = std::max(dps.back().second, entry->max);
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(curr_tstamp, (double)entry->max);
        m_last_tstamp = curr_tstamp;
    }
}


void
DownsamplerMin::add_data_point(DataPointPair& dp, DataPointVector& dps)
{
    Timestamp curr_tstamp = step_down(dp.first);
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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

void
DownsamplerMin::add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps)
{
    ASSERT(entry != nullptr);
    Timestamp curr_tstamp = step_down(validate_resolution(entry->tstamp));
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second = std::min(dps.back().second, entry->min);
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(curr_tstamp, (double)entry->min);
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
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        m_values.push_back(dp.second);
    }
    else
    {
        if (m_last_tstamp != TT_INVALID_TIMESTAMP)
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
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

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

void
DownsamplerSum::add_data_point(struct rollup_entry_ext *entry, RollupType rollup, DataPointVector& dps)
{
    ASSERT(entry != nullptr);
    Timestamp curr_tstamp = step_down(validate_resolution(entry->tstamp));
    ASSERT((m_last_tstamp <= curr_tstamp) || (m_last_tstamp == TT_INVALID_TIMESTAMP));

    if (curr_tstamp < m_start) return;

    if (curr_tstamp == m_last_tstamp)
    {
        ASSERT(!dps.empty());
        dps.back().second += entry->sum;
    }
    else
    {
        fill_to(curr_tstamp, dps);
        dps.emplace_back(curr_tstamp, (double)entry->sum);
        m_last_tstamp = curr_tstamp;
    }
}


}
