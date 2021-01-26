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

#include <cstdlib>
#include "misc_test.h"


using namespace tt;

namespace tt_test
{


void
MiscTests::run()
{
    log("Running %s...", m_name);

    memmgr_tests();
    off_hour_tests();
    random_tests();
    strbuf_tests();
    url_decode_tests();

    log("Finished %s", m_name);
}

void
MiscTests::memmgr_tests()
{
    for (int i = 0; i < 4096; i++)
    {
        for (int t = 0; t < (int)RecyclableType::RT_COUNT; t++)
        {
            Recyclable *r = MemoryManager::alloc_recyclable((RecyclableType)t);
            confirm(r != nullptr);
            confirm(r->recyclable_type() == t);
            MemoryManager::free_recyclable(r);
        }
    }

    m_stats.add_passed(1);
}

void
MiscTests::off_hour_tests()
{
    std::time_t sec = std::time(nullptr);
    struct tm *now = localtime(&sec);
    int cur_hour = now->tm_hour;
    std::string begin, end;

    begin = std::to_string((cur_hour - 1) % 24);
    end = std::to_string((cur_hour + 1) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    confirm(is_off_hour());

    begin = std::to_string((cur_hour + 2) % 24);
    end = std::to_string((cur_hour + 3) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    confirm(! is_off_hour());

    begin = std::to_string((cur_hour + 2) % 24);
    end = std::to_string((cur_hour + 22) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    confirm(! is_off_hour());

    begin = std::to_string((cur_hour - 22) % 24);
    end = std::to_string((cur_hour - 1) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    confirm(! is_off_hour());

    begin = std::to_string((cur_hour - 20) % 24);
    end = std::to_string((cur_hour + 1) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    confirm(is_off_hour());

    m_stats.add_passed(1);
}

void
MiscTests::random_tests()
{
    for (int i = 0; i < 1000; i++)
    {
        int from = std::rand();
        int to = std::rand();

        if (from > to)
        {
            int tmp = to;
            to = from;
            from = tmp;
        }

        int n = random(from, to);

        confirm(from <= n);
        confirm(n <= to);
    }

    m_stats.add_passed(1);
}

void
MiscTests::strbuf_tests()
{
    StringBuffer strbuf;

    for (int i = 0; i < 4096; i++)
    {
        std::string s = std::to_string(i);
        char *c = strbuf.strdup(s.c_str());
        confirm(c != nullptr);
        confirm(i == std::atoi(c));
    }

    m_stats.add_passed(1);
}

void
MiscTests::url_decode_tests()
{
    const char *url1 = "start=1562483040&end=1562483385&m=avg%3A1m-avg%3Amysql.innodb_row_lock_time%7Bhost%3D*%7D";
    const char *expected1 = "start=1562483040&end=1562483385&m=avg:1m-avg:mysql.innodb_row_lock_time{host=*}";
    char actual1[128];

    confirm(url_unescape(url1, actual1, sizeof(actual1)));
    confirm(strcmp(expected1, actual1) == 0);

    m_stats.add_passed(1);
}


}
