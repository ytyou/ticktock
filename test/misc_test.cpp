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

    dynamic_array_tests();
    memmgr_tests();
    off_hour_tests();
    random_tests();
    strbuf_tests();
    url_decode_tests();
    time_conv_tests();

    log("Finished %s", m_name);
}

void
MiscTests::dynamic_array_tests()
{
    size_t rows = 3;
    size_t cols = 4097;
    int arr[rows][cols];
    DynamicArray2D<int> dyn_arr(rows, cols);

    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
        {
            arr[i][j] = tt::random(0, 100);
            dyn_arr.elem(i, j) = arr[i][j];
        }
    }

    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < cols; j++)
            CONFIRM(arr[i][j] == dyn_arr.elem(i,j));
    }

    m_stats.add_passed(1);
}

void
MiscTests::memmgr_tests()
{
    MemoryManager::init();

    for (int i = 0; i < 4096; i++)
    {
        for (int t = 0; t < (int)RecyclableType::RT_COUNT; t++)
        {
            Recyclable *r = MemoryManager::alloc_recyclable((RecyclableType)t);
            CONFIRM(r != nullptr);
            CONFIRM(r->recyclable_type() == t);
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
    CONFIRM(is_off_hour());

    begin = std::to_string((cur_hour + 2) % 24);
    end = std::to_string((cur_hour + 3) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    CONFIRM(! is_off_hour());

    begin = std::to_string((cur_hour + 2) % 24);
    end = std::to_string((cur_hour + 22) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    CONFIRM(! is_off_hour());

    begin = std::to_string((cur_hour - 22) % 24);
    end = std::to_string((cur_hour - 1) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    CONFIRM(! is_off_hour());

    begin = std::to_string((cur_hour - 20) % 24);
    end = std::to_string((cur_hour + 1) % 24);
    Config::set_value(CFG_TSDB_OFF_HOUR_BEGIN, begin);
    Config::set_value(CFG_TSDB_OFF_HOUR_END, end);
    CONFIRM(is_off_hour());

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

        CONFIRM(from <= n);
        CONFIRM(n <= to);
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
        CONFIRM(c != nullptr);
        CONFIRM(i == std::atoi(c));
    }

    m_stats.add_passed(1);
}

void
MiscTests::url_decode_tests()
{
    const char *url1 = "start=1562483040&end=1562483385&m=avg%3A1m-avg%3Amysql.innodb_row_lock_time%7Bhost%3D*%7D";
    const char *expected1 = "start=1562483040&end=1562483385&m=avg:1m-avg:mysql.innodb_row_lock_time{host=*}";
    char actual1[128];

    CONFIRM(url_unescape(url1, actual1, sizeof(actual1)));
    CONFIRM(strcmp(expected1, actual1) == 0);

    m_stats.add_passed(1);
}

void
MiscTests::time_conv_tests()
{
    // parse time unit
    CONFIRM(to_time_unit("1m",2) == TimeUnit::MIN);
    CONFIRM(to_time_unit("2min",4) == TimeUnit::MIN);
    CONFIRM(to_time_unit("3s",2) == TimeUnit::SEC);
    CONFIRM(to_time_unit("4ms",3) == TimeUnit::MS);
    CONFIRM(to_time_unit("5h",2) == TimeUnit::HOUR);
    CONFIRM(to_time_unit("10w",3) == TimeUnit::WEEK);
    CONFIRM(to_time_unit("100n",4) == TimeUnit::MONTH);
    CONFIRM(to_time_unit("20month",7) == TimeUnit::MONTH);
    CONFIRM(to_time_unit("90y",3) == TimeUnit::YEAR);

    // time conversion
    CONFIRM(convert_time((Timestamp)4*365*24*3600*1000L, TimeUnit::MS, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time((Timestamp)3*30*24*3600*1000L, TimeUnit::MS, TimeUnit::MONTH) == 3);
    CONFIRM(convert_time((Timestamp)5*7*24*3600*1000L, TimeUnit::MS, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8*24*3600*1000, TimeUnit::MS, TimeUnit::DAY) == 8);
    CONFIRM(convert_time(27*3600*1000, TimeUnit::MS, TimeUnit::HOUR) == 27);
    CONFIRM(convert_time(207*60*1000, TimeUnit::MS, TimeUnit::MIN) == 207);
    CONFIRM(convert_time(2*1000, TimeUnit::MS, TimeUnit::SEC) == 2);
    CONFIRM(convert_time(2345, TimeUnit::MS, TimeUnit::MS) == 2345);

    CONFIRM(convert_time(4*365*24*3600, TimeUnit::SEC, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(3*30*24*3600, TimeUnit::SEC, TimeUnit::MONTH) == 3);
    CONFIRM(convert_time(5*7*24*3600, TimeUnit::SEC, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8*24*3600, TimeUnit::SEC, TimeUnit::DAY) == 8);
    CONFIRM(convert_time(27*3600, TimeUnit::SEC, TimeUnit::HOUR) == 27);
    CONFIRM(convert_time(207*60, TimeUnit::SEC, TimeUnit::MIN) == 207);
    CONFIRM(convert_time(2345, TimeUnit::SEC, TimeUnit::SEC) == 2345);
    CONFIRM(convert_time(2345, TimeUnit::SEC, TimeUnit::MS) == 2345000);

    CONFIRM(convert_time(4*365*24*60, TimeUnit::MIN, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(3*30*24*60, TimeUnit::MIN, TimeUnit::MONTH) == 3);
    CONFIRM(convert_time(5*7*24*60, TimeUnit::MIN, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8*24*60, TimeUnit::MIN, TimeUnit::DAY) == 8);
    CONFIRM(convert_time(27*60, TimeUnit::MIN, TimeUnit::HOUR) == 27);
    CONFIRM(convert_time(2345, TimeUnit::MIN, TimeUnit::MIN) == 2345);
    CONFIRM(convert_time(23, TimeUnit::MIN, TimeUnit::SEC) == 23*60);
    CONFIRM(convert_time(23, TimeUnit::MIN, TimeUnit::MS) == 23*60000);

    CONFIRM(convert_time(4*365*24, TimeUnit::HOUR, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(3*30*24, TimeUnit::HOUR, TimeUnit::MONTH) == 3);
    CONFIRM(convert_time(5*7*24, TimeUnit::HOUR, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8*24, TimeUnit::HOUR, TimeUnit::DAY) == 8);
    CONFIRM(convert_time(27, TimeUnit::HOUR, TimeUnit::HOUR) == 27);
    CONFIRM(convert_time(23, TimeUnit::HOUR, TimeUnit::MIN) == 23*60);
    CONFIRM(convert_time(23, TimeUnit::HOUR, TimeUnit::SEC) == 23*3600);
    CONFIRM(convert_time(23, TimeUnit::HOUR, TimeUnit::MS) == 23*3600000);

    CONFIRM(convert_time(4*365, TimeUnit::DAY, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(3*30, TimeUnit::DAY, TimeUnit::MONTH) == 3);
    CONFIRM(convert_time(5*7, TimeUnit::DAY, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8, TimeUnit::DAY, TimeUnit::DAY) == 8);
    CONFIRM(convert_time(27, TimeUnit::DAY, TimeUnit::HOUR) == 27*24);
    CONFIRM(convert_time(13, TimeUnit::DAY, TimeUnit::MIN) == 13*24*60);
    CONFIRM(convert_time(13, TimeUnit::DAY, TimeUnit::SEC) == 13*24*3600);
    CONFIRM(convert_time(13, TimeUnit::DAY, TimeUnit::MS) == 13*24*3600000);

    CONFIRM(convert_time(4*53, TimeUnit::WEEK, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(9, TimeUnit::WEEK, TimeUnit::MONTH) == 2);
    CONFIRM(convert_time(5, TimeUnit::WEEK, TimeUnit::WEEK) == 5);
    CONFIRM(convert_time(8, TimeUnit::WEEK, TimeUnit::DAY) == 8*7);
    CONFIRM(convert_time(27, TimeUnit::WEEK, TimeUnit::HOUR) == 27*7*24);
    CONFIRM(convert_time(3, TimeUnit::WEEK, TimeUnit::MIN) == 3*7*24*60);
    CONFIRM(convert_time(3, TimeUnit::WEEK, TimeUnit::SEC) == 3*7*24*3600);
    CONFIRM(convert_time(3, TimeUnit::WEEK, TimeUnit::MS) == 3*7*24*3600000);

    CONFIRM(convert_time(4*12+1, TimeUnit::MONTH, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(31, TimeUnit::MONTH, TimeUnit::MONTH) == 31);
    CONFIRM(convert_time(5, TimeUnit::MONTH, TimeUnit::WEEK) == (5*30)/7);
    CONFIRM(convert_time(8, TimeUnit::MONTH, TimeUnit::DAY) == 8*30);
    CONFIRM(convert_time(27, TimeUnit::MONTH, TimeUnit::HOUR) == 27*30*24);
    CONFIRM(convert_time(2, TimeUnit::MONTH, TimeUnit::MIN) == 2*30*24*60);
    CONFIRM(convert_time(2, TimeUnit::MONTH, TimeUnit::SEC) == 2*30*24*3600);
    CONFIRM(convert_time(2, TimeUnit::MONTH, TimeUnit::MS) == (Timestamp)2*30*24*3600000L);

    CONFIRM(convert_time(4, TimeUnit::YEAR, TimeUnit::YEAR) == 4);
    CONFIRM(convert_time(3, TimeUnit::YEAR, TimeUnit::MONTH) == (3*365)/30);
    CONFIRM(convert_time(5, TimeUnit::YEAR, TimeUnit::WEEK) == (5*365)/7);
    CONFIRM(convert_time(8, TimeUnit::YEAR, TimeUnit::DAY) == 8*365);
    CONFIRM(convert_time(27, TimeUnit::YEAR, TimeUnit::HOUR) == 27*365*24);
    CONFIRM(convert_time(2, TimeUnit::YEAR, TimeUnit::MIN) == 2*365*24*60);
    CONFIRM(convert_time(2, TimeUnit::YEAR, TimeUnit::SEC) == 2*365*24*3600L);
    CONFIRM(convert_time(2, TimeUnit::YEAR, TimeUnit::MS) == (Timestamp)2*365*24*3600000L);
}


}
