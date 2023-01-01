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

#include "tag.h"
#include "test.h"


namespace tt_test
{


class TagTests : public TestCase
{
public:
    TagTests() { m_name = "tag_tests"; }
    void run();

private:
    void parsed_tests();
    void raw_tests();

    int m_device_count;
    int m_sensor_count;

    std::vector<tt::Tag*> m_tags;
    std::vector<const char*> m_raws;
};


}
