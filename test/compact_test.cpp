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

#include "compact_test.h"


using namespace tt;

namespace tt_test
{


void
CompactTests::run()
{
    log("Running %s...", m_name);

    two_partial_with_ooo();
    one_full_two_partial_with_ooo();
    three_partial_with_ooo();
    need_to_fill_empty_page();
    need_to_fill_empty_page_again(0);
    need_to_fill_empty_page_again(1);
    need_to_fill_empty_page_again(2);
    remove_duplicates();

    log("Finished %s", m_name);
}

void
CompactTests::update_config(Timestamp archive_ms, int compressor)
{
    std::vector<std::pair<const char*, const char*> > configs;
    std::string archive = std::to_string(archive_ms) + "ms";

    configs.emplace_back(CFG_APPEND_LOG_ENABLED, "false");
    configs.emplace_back(CFG_LOG_FILE, str_join(TEST_ROOT, "test.log"));
    configs.emplace_back(CFG_LOG_LEVEL, "TRACE");
    configs.emplace_back(CFG_TSDB_DATA_DIR, str_join(TEST_ROOT, "data"));
    configs.emplace_back(CFG_TSDB_ARCHIVE_THRESHOLD, archive.c_str());
    configs.emplace_back(CFG_TSDB_READ_ONLY_THRESHOLD, archive.c_str());
    configs.emplace_back(CFG_TSDB_TIMESTAMP_RESOLUTION, "millisecond");
    configs.emplace_back(CFG_QUERY_EXECUTOR_PARALLEL, "false");
    configs.emplace_back(CFG_TCP_BUFFER_SIZE, "1mb");
    configs.emplace_back(CFG_TSDB_COMPRESSOR_VERSION, (compressor==0)?"0":((compressor==1)?"1":"2"));

    create_config(configs);
    Config::init();
}

void
CompactTests::two_partial_with_ooo1(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps)
{
    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 1);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::two_partial_with_ooo2(int dps_cnt, int ooo_cnt)
{
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    Tsdb::shutdown();
}

void
CompactTests::two_partial_with_ooo3()
{
    log("perform compaction...");
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    Tsdb::shutdown();
    log("compaction done");
}

void
CompactTests::two_partial_with_ooo4(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps, Timestamp now)
{
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));
}

void
CompactTests::two_partial_with_ooo()
{
    log("Running two_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 10;
    DataPointVector dps, ooo_dps;

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    /* Running tests in different threads to around the issue of
     * contention_free_shared_mutex stops working once you take a
     * shared lock/unlock of it and then release it (calling destructor).
     */
    std::thread t1(&CompactTests::two_partial_with_ooo1, this, dps_cnt, ooo_cnt, dps, ooo_dps);
    t1.join();

    std::thread t2(&CompactTests::two_partial_with_ooo2, this, dps_cnt, ooo_cnt);
    t2.join();

    std::thread t3(&CompactTests::two_partial_with_ooo3, this);
    t3.join();

    std::thread t4(&CompactTests::two_partial_with_ooo4, this, dps_cnt, ooo_cnt, dps, ooo_dps, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::one_full_two_partial_with_ooo1(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps)
{
    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    CONFIRM(! Tsdb::inst(start)->is_archived());

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("query returned %d data points", results.size());

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 2);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::one_full_two_partial_with_ooo2(int dps_cnt, int ooo_cnt)
{
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    Tsdb::shutdown();
}

void
CompactTests::one_full_two_partial_with_ooo3()
{
    log("perform compaction...");
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    Tsdb::shutdown();
    log("compaction done");
}

void
CompactTests::one_full_two_partial_with_ooo4(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps, Timestamp now)
{
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 2);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));
}

void
CompactTests::one_full_two_partial_with_ooo()
{
    log("Running one_full_two_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 1000, ooo_cnt = 10;
    DataPointVector dps, ooo_dps;

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    std::thread t1(&CompactTests::one_full_two_partial_with_ooo1, this, dps_cnt, ooo_cnt, dps, ooo_dps);
    t1.join();

    std::thread t2(&CompactTests::one_full_two_partial_with_ooo2, this, dps_cnt, ooo_cnt);
    t2.join();

    std::thread t3(&CompactTests::one_full_two_partial_with_ooo3, this);
    t3.join();

    std::thread t4(&CompactTests::one_full_two_partial_with_ooo4, this, dps_cnt, ooo_cnt, dps, ooo_dps, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::three_partial_with_ooo1(int dps_cnt, int ooo_cnt, DataPointVector dps1, DataPointVector dps2, DataPointVector ooo_dps)
{
    for (DataPointPair& dpp: dps1)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "1");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: dps2)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "2");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "1");
        tsdb->add(dp);
    }

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));

    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 2);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::three_partial_with_ooo2(int dps_cnt, int ooo_cnt)
{
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));
    Tsdb::shutdown();
}

void
CompactTests::three_partial_with_ooo3()
{
    log("perform compaction...");
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    Tsdb::shutdown();
    log("compaction done");
}

void
CompactTests::three_partial_with_ooo4(int dps_cnt, int ooo_cnt, DataPointVector dps1, DataPointVector dps2, DataPointVector ooo_dps, Timestamp now)
{
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));
    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));
}

void
CompactTests::three_partial_with_ooo()
{
    log("Running three_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 10;
    DataPointVector dps1, dps2, ooo_dps;

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps1, dps_cnt, start+3600000);
    generate_data_points(dps2, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    std::thread t1(&CompactTests::three_partial_with_ooo1, this, dps_cnt, ooo_cnt, dps1, dps2, ooo_dps);
    t1.join();

    std::thread t2(&CompactTests::three_partial_with_ooo2, this, dps_cnt, ooo_cnt);
    t2.join();

    std::thread t3(&CompactTests::three_partial_with_ooo3, this);
    t3.join();

    std::thread t4(&CompactTests::three_partial_with_ooo4, this, dps_cnt, ooo_cnt, dps1, dps2, ooo_dps, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::need_to_fill_empty_page1(int dps_cnt1, int dps_cnt2, int dps_cnt3, DataPointVector dps1, DataPointVector dps2, DataPointVector dps3)
{
    for (DataPointPair& dpp: dps1)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "1");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: dps2)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "2");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: dps3)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        dp.add_tag("tag", "3");
        tsdb->add(dp);
    }

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));

    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: dps3) CONFIRM(contains(results, dp));

    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 4);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::need_to_fill_empty_page2(int dps_cnt1, int dps_cnt2, int dps_cnt3)
{
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));
    Tsdb::shutdown();
}

void
CompactTests::need_to_fill_empty_page3()
{
    log("perform compaction...");
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    Tsdb::shutdown();
    log("compaction done");
}

void
CompactTests::need_to_fill_empty_page4(int dps_cnt1, int dps_cnt2, int dps_cnt3, DataPointVector dps1, DataPointVector dps2, DataPointVector dps3, Timestamp now)
{
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 2);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));
    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: dps3) CONFIRM(contains(results, dp));
}

void
CompactTests::need_to_fill_empty_page()
{
    log("Running need_to_fill_empty_page()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt1 = 10, dps_cnt2 = 10, dps_cnt3 = 1000;
    DataPointVector dps1, dps2, dps3;

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps1, dps_cnt1, start);
    generate_data_points(dps2, dps_cnt2, start);
    generate_data_points(dps3, dps_cnt3, start);

    std::thread t1(&CompactTests::need_to_fill_empty_page1, this, dps_cnt1, dps_cnt2, dps_cnt3, dps1, dps2, dps3);
    t1.join();

    std::thread t2(&CompactTests::need_to_fill_empty_page2, this, dps_cnt1, dps_cnt2, dps_cnt3);
    t2.join();

    std::thread t3(&CompactTests::need_to_fill_empty_page3, this);
    t3.join();

    std::thread t4(&CompactTests::need_to_fill_empty_page4, this, dps_cnt1, dps_cnt2, dps_cnt3, dps1, dps2, dps3, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::need_to_fill_empty_page_again1(int compressor, int dps_cnts[6], DataPointVector dps[6], StringBuffer *strbuf)
{
    int dps_cnt = 0;

    for (int i = 0; i < 6; i++)
    {
        for (DataPointPair& dpp: dps[i])
        {
            Tsdb *tsdb = Tsdb::inst(dpp.first);
            DataPoint dp(dpp.first, dpp.second);
            dp.set_metric(metric);
            dp.add_tag("tag", strbuf->strdup(std::to_string(i).c_str()));
            tsdb->add(dp);
        }

        dps_cnt += dps_cnts[i];
    }

    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);

    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }

    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == ((compressor==0)?9:7));
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::need_to_fill_empty_page_again2(int compressor, int dps_cnt)
{
    update_config(3600000, compressor); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);
    Tsdb::shutdown();
}

void
CompactTests::need_to_fill_empty_page_again3(int compressor, int dps_cnt, DataPointVector dps[6])
{
    log("perform compaction...");
    update_config(3600000, compressor); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    log("compaction done");
    DataPointVector results;
    query_raw(metric, 0, results);
    log("results.size() = %d", results.size());
    CONFIRM(results.size() == dps_cnt);
    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }
    Tsdb::shutdown();
}

void
CompactTests::need_to_fill_empty_page_again4(int compressor, int dps_cnt, DataPointVector dps[6], Timestamp now)
{
    update_config(now, compressor);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == ((compressor==0)?4:2));
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    DataPointVector results;
    query_raw(metric, 0, results);
    log("results.size() = %d", results.size());
    CONFIRM(results.size() == dps_cnt);
    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }
}

void
CompactTests::need_to_fill_empty_page_again(int compressor)
{
    log("Running need_to_fill_empty_page_again(%d)...", compressor);

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnts[] = { 2, 2, 2, 2, 1000, 2 };
    DataPointVector dps[6];
    int dps_cnt = 0;

    // make sure archive mode is NOT on
    update_config(now, compressor);
    clean_start(true);

    StringBuffer strbuf;    // must be declared after clean_start()!

    for (int i = 0; i < 6; i++)
    {
        generate_data_points(dps[i], dps_cnts[i], start);
        dps_cnt += dps_cnts[i];
    }

    std::thread t1(&CompactTests::need_to_fill_empty_page_again1, this, compressor, dps_cnts, dps, &strbuf);
    t1.join();

    std::thread t2(&CompactTests::need_to_fill_empty_page_again2, this, compressor, dps_cnt);
    t2.join();

    std::thread t3(&CompactTests::need_to_fill_empty_page_again3, this, compressor, dps_cnt, dps);
    t3.join();

    std::thread t4(&CompactTests::need_to_fill_empty_page_again4, this, compressor, dps_cnt, dps, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::remove_duplicates1(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps)
{
    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    // insert them again as duplicates
    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    // retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    // query will remove duplicates
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("results.size() = %d", results.size());

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    // make sure they occupy 2 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 1);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    Tsdb::shutdown();
}

void
CompactTests::remove_duplicates2(int dps_cnt, int ooo_cnt)
{
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    DataPointVector results;
    query_raw(metric, 0, results);
    // query will remove duplicates
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("results.size() = %d", results.size());
    Tsdb::shutdown();
}

void
CompactTests::remove_duplicates3()
{
    log("perform compaction...");
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    Tsdb::shutdown();
    log("compaction done");
}

void
CompactTests::remove_duplicates4(int dps_cnt, int ooo_cnt, DataPointVector dps, DataPointVector ooo_dps, Timestamp now)
{
    // make sure tsdb has no out-of-order dps anymore;
    // and all dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // make sure all dps are still there and correct, with duplicates removed
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));
    log("results.size() = %d", results.size());
}

void
CompactTests::remove_duplicates()
{
    log("Running remove_duplicates()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    //Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 15;
    DataPointVector dps, ooo_dps;

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    std::thread t1(&CompactTests::remove_duplicates1, this, dps_cnt, ooo_cnt, dps, ooo_dps);
    t1.join();

    std::thread t2(&CompactTests::remove_duplicates2, this, dps_cnt, ooo_cnt);
    t2.join();

    std::thread t3(&CompactTests::remove_duplicates3, this);
    t3.join();

    std::thread t4(&CompactTests::remove_duplicates4, this, dps_cnt, ooo_cnt, dps, ooo_dps, now);
    t4.join();

    clean_shutdown();
    m_stats.add_passed(1);
}


}
