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

#include <mutex>
#include <vector>
#include "utils.h"


namespace tt
{


class Calendar
{
public:
    static Timestamp begin_month_of(Timestamp ts);
    static Timestamp end_month_of(Timestamp ts);

private:
    Calendar() = default;
    static std::size_t add_month(Timestamp ts, int n);

    // cache for known (beginning of) months
    static std::mutex m_lock;
    static std::vector<Timestamp> m_months;
};


}
