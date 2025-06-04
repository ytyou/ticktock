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
#include "cal.h"
#include "utils.h"
#include "cal_test.h"


using namespace tt;

namespace tt_test
{


void
CalendarTests::run()
{
    test1();
}

void
CalendarTests::test1()
{
    log("Running %s...", m_name);

    Timestamp ts1 = ts_now_sec();
    Timestamp begin1 = verify(ts1);

    // try previous month
    Timestamp ts2 = begin1 - 1;
    Timestamp begin2 = verify(ts2);

    // try next month
    Timestamp ts3 = begin1 + 30 * 24 * 3600;
    verify(ts3);

    for (int m = 5; m <= 200; m += 5)
    {
        // try a few month ago
        Timestamp ts4 = begin1 - m * 30 * 24 * 3600 - 10;
        verify(ts4);

        // try a few month in the future
        Timestamp ts5 = begin1 + m * 30 * 24 * 3600 + 10;
        verify(ts5);
    }

    // again
    verify(ts1);
    verify(ts2);
    verify(ts3);

    verify(begin1 - 3 * 30 * 24 * 3600 - 10);
    verify(begin1 + 3 * 30 * 24 * 3600 + 10);

    m_stats.add_passed(1);
}

Timestamp
CalendarTests::verify(Timestamp ts)
{
    Timestamp expected_begin = begin_month(ts);
    Timestamp expected_end = end_month(ts);
    Timestamp actual_begin = Calendar::begin_month_of(ts);
    Timestamp actual_end = Calendar::end_month_of(ts);

    CONFIRM(expected_begin == actual_begin);
    CONFIRM(expected_end == actual_end);
    CONFIRM(expected_begin == Calendar::begin_month_of(actual_begin));
    CONFIRM(expected_end == Calendar::end_month_of(actual_begin));
    CONFIRM(expected_begin == Calendar::begin_month_of(actual_end-1));
    CONFIRM(expected_end == Calendar::end_month_of(actual_end-1));

    return expected_begin;
}


}
