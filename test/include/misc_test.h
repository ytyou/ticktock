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

#include "test.h"


namespace tt_test
{


class MiscTests : public TestCase
{
public:
    MiscTests() { m_name = "misc_tests"; }
    void run();

private:
    void dynamic_array_tests();
    void memmgr_tests();
    void off_hour_tests();
    void random_tests();
    void strbuf_tests();
    void url_decode_tests();
    void time_conv_tests();
    void string_util_tests();
};


}
