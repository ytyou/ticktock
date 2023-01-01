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

#include <cstdlib>
#include <string>
#include <cstring>
#include "tag_test.h"


using namespace tt;

namespace tt_test
{


void
TagTests::run()
{
    log("Running %s...", m_name);

    // prepare data
    m_device_count = 30000;
    m_sensor_count = 1000;
    char buff[128];

    m_tags.reserve(m_device_count * m_sensor_count);
    m_raws.reserve(m_device_count * m_sensor_count);

    for (int d = 0; d < m_device_count; d++)
    {
        for (int s = 0; s < m_sensor_count; s++)
        {
            snprintf(buff, sizeof(buff), "sensor=s_%d;device=d_%d;", s, d);
            m_raws.push_back(strdup(buff));
            std::string str(buff);
            m_tags.push_back(Tag::parse_multiple(str));
        }
    }

    log("Generated %d tags", m_device_count * m_sensor_count);

    parsed_tests();
    raw_tests();

    log("Finished %s", m_name);
}

void
TagTests::parsed_tests()
{
    tt::Timestamp ts;
    char buff[128];
    int match_cnt;

    // exact match
    ts = ts_now_ms();
    match_cnt = 0;
    for (tt::Tag *tags: m_tags)
    {
        if (Tag::match_value(tags, "device", "d_2"))
            match_cnt++;
    }
    CONFIRM(match_cnt == m_sensor_count);

    match_cnt = 0;
    for (tt::Tag *tags: m_tags)
    {
        if (Tag::match_value(tags, "sensor", "s_21"))
            match_cnt++;
    }
    CONFIRM(match_cnt == m_device_count);

    log("parsed: exact match took: %" PRIu64 " ms", ts_now_ms() - ts);

    m_stats.add_passed(1);
}

void
TagTests::raw_tests()
{
    tt::Timestamp ts;
    char buff[128];
    int match_cnt;

    // exact match
    ts = ts_now_ms();
    match_cnt = 0;
    for (const char *tags: m_raws)
    {
        const char *match = std::strstr(tags, "device=d_2;");
        if (match == nullptr) continue;
        if ((tags != match) && (*(match-1) != ';')) continue;
        //if (std::strncmp(match+7, "d_2;", 4) != 0) continue;
        match_cnt++;
    }
    CONFIRM(match_cnt == m_sensor_count);

    match_cnt = 0;
    for (const char *tags: m_raws)
    {
        const char *match = std::strstr(tags, "sensor=s_21;");
        if (match == nullptr) continue;
        if ((tags != match) && (*(match-1) != ';')) continue;
        //if (std::strncmp(match+7, "s_21;", 5) != 0) continue;
        match_cnt++;
    }
    CONFIRM(match_cnt == m_device_count);

    log("raws: exact match took: %" PRIu64 " ms", ts_now_ms() - ts);

    m_stats.add_passed(1);
}


}
