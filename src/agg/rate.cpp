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

#include <cstdint>
#include "global.h"
#include "rate.h"
#include "utils.h"


namespace tt
{


void
RateCalculator::init(bool counter, bool drop_resets, uint64_t counter_max, uint64_t reset_value)
{
    m_counter = counter;
    m_drop_resets = drop_resets;
    m_counter_max = counter_max;
    m_reset_value = reset_value;
}

void
RateCalculator::calculate(DataPointVector& dps)
{
    if (dps.empty()) return;

    Timestamp t0 = dps[0].first;
    double v0 = dps[0].second;
    int j = 0;

    dps[0].second = 0.0;

    for (int i = 1; i < dps.size(); i++)
    {
        Timestamp t1 = dps[i].first;
        double v1 = dps[i].second;

        ASSERT(t0 < t1);
        double ts_delta_secs = (double)(t1 - t0);
        double val_delta = v1 - v0;

        if (g_tstamp_resolution_ms) ts_delta_secs /= 1000.0;

        if (m_counter && (val_delta < 0))
        {
            if (m_drop_resets) goto cont;

            val_delta = (double)m_counter_max - v0 + v1;
            double rate = val_delta / ts_delta_secs;

            if ((m_reset_value != 0) && (rate > m_reset_value))
            {
                dps[j].first = t1;
                dps[j].second = 0.0;
            }
            else
            {
                dps[j].first = t1;
                dps[j].second = rate;
            }
        }
        else
        {
            dps[j].first = t1;
            dps[j].second = val_delta / ts_delta_secs;
        }

        j++;

    cont:
        t0 = t1;
        v0 = v1;
    }

    dps.resize(j);
}


}
