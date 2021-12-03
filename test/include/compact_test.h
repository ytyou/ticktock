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


class CompactTests : public TestCase
{
public:
    CompactTests() :
        start(946684800000),
        metric("compact.test.metric")
    {
        m_name = "compact_tests";
    }

    void run();

private:
    void two_partial_with_ooo();
    void two_partial_with_ooo1(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps);
    void two_partial_with_ooo2(int dps_cnt, int ooo_cnt);
    void two_partial_with_ooo3();
    void two_partial_with_ooo4(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps, tt::Timestamp now);

    void one_full_two_partial_with_ooo();
    void one_full_two_partial_with_ooo1(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps);
    void one_full_two_partial_with_ooo2(int dps_cnt, int ooo_cnt);
    void one_full_two_partial_with_ooo3();
    void one_full_two_partial_with_ooo4(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps, tt::Timestamp now);

    void three_partial_with_ooo();
    void three_partial_with_ooo1(int dps_cnt, int ooo_cnt, tt::DataPointVector dps1, tt::DataPointVector dps2, tt::DataPointVector ooo_dps);
    void three_partial_with_ooo2(int dps_cnt, int ooo_cnt);
    void three_partial_with_ooo3();
    void three_partial_with_ooo4(int dps_cnt, int ooo_cnt, tt::DataPointVector dps1, tt::DataPointVector dps2, tt::DataPointVector ooo_dps, tt::Timestamp now);

    void need_to_fill_empty_page();
    void need_to_fill_empty_page1(int dps_cnt1, int dps_cnt2, int dps_cnt3, tt::DataPointVector dps1, tt::DataPointVector dps2, tt::DataPointVector dps3);
    void need_to_fill_empty_page2(int dps_cnt1, int dps_cnt2, int dps_cnt3);
    void need_to_fill_empty_page3();
    void need_to_fill_empty_page4(int dps_cnt1, int dps_cnt2, int dps_cnt3, tt::DataPointVector dps1, tt::DataPointVector dps2, tt::DataPointVector dps3, tt::Timestamp now);

    void need_to_fill_empty_page_again(int compressor);
    void need_to_fill_empty_page_again1(int compressor, int dps_cnts[6], tt::DataPointVector dps[6], tt::StringBuffer *strbuf);
    void need_to_fill_empty_page_again2(int compressor, int dps_cnt);
    void need_to_fill_empty_page_again3(int compressor, int dps_cnt, tt::DataPointVector dps[6]);
    void need_to_fill_empty_page_again4(int compressor, int dps_cnt, tt::DataPointVector dps[6], tt::Timestamp now);

    void remove_duplicates();
    void remove_duplicates1(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps);
    void remove_duplicates2(int dps_cnt, int ooo_cnt);
    void remove_duplicates3();
    void remove_duplicates4(int dps_cnt, int ooo_cnt, tt::DataPointVector dps, tt::DataPointVector ooo_dps, tt::Timestamp now);

    void update_config(tt::Timestamp archive_ms, int compressor = 1);

    tt::Timestamp start;
    const char *metric;
};


}
