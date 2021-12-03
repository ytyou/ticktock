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

#include <stdint.h>
#include "recycle.h"
#include "type.h"


namespace tt
{


class RateCalculator : public Recyclable
{
public:
    void init(bool counter, bool drop_resets, uint64_t counter_max = UINT64_MAX, uint64_t reset_value = 0);
    void calculate(DataPointVector& dps);   // calc in-place

private:
    bool m_counter;
    bool m_drop_resets;

    uint64_t m_counter_max;
    uint64_t m_reset_value;
};


}
