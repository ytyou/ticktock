/*
    TickTockDB is an open-source Time Series Database, maintained by
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
#include "dp.h"
#include "dp_test.h"


using namespace tt;

namespace tt_test
{


void
DataPointTests::run()
{
    parse_nan();
}

void
DataPointTests::parse_nan()
{
    log("Running %s...", m_name);

    char buff[1024];
    char *p = buff;
    DataPoint dp;

    snprintf(buff, sizeof(buff), "dp.test.metric 1606091337 NaN host=dev\ndp.test.metric 1606091337 2.3 host=suse\n");

    CONFIRM(dp.from_plain(p));
    CONFIRM(dp.get_timestamp() == 1606091337);
    CONFIRM(std::isnan(NAN));
    log("value = %f", dp.get_value());

    CONFIRM(dp.from_plain(p));
    CONFIRM(dp.get_timestamp() == 1606091337);
    CONFIRM(dp.get_value() == 2.3);
    log("value = %f", dp.get_value());

    m_stats.add_passed(1);
}


}
