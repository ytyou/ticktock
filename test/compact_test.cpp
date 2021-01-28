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
    need_to_fill_empty_page2(0);
    need_to_fill_empty_page2(1);
    need_to_fill_empty_page2(2);
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
CompactTests::two_partial_with_ooo()
{
    log("Running two_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 10;
    DataPointVector dps, ooo_dps;
    const char *metric = "compact.test.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        tsdb->add(dp);
    }

    // 2. retrieve all 20 dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    // 3. make sure they occupy 2 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 1);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
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

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all 20 dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all 20 dps are still there and correct;
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::one_full_two_partial_with_ooo()
{
    log("Running one_full_two_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 1000, ooo_cnt = 10;
    DataPointVector dps, ooo_dps;
    const char *metric = "compact.test.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        tsdb->add(dp);
    }

    CONFIRM(! Tsdb::inst(start)->is_archived());

    for (DataPointPair& dpp: ooo_dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        tsdb->add(dp);
    }

    // 2. retrieve all 20 dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("query returned %d data points", results.size());

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    // 3. make sure they occupy 3 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 2);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
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

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all 20 dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 2);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all 20 dps are still there and correct;
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::three_partial_with_ooo()
{
    log("Running three_partial_with_ooo()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 10;
    DataPointVector dps1, dps2, ooo_dps;
    const char *metric = "compact.test.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps1, dps_cnt, start+3600000);
    generate_data_points(dps2, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

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

    // 2. retrieve all 30 dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));

    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    // 3. make sure they occupy 3 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 2);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
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

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all 20 dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all 30 dps are still there and correct;
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (2*dps_cnt + ooo_cnt));
    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::need_to_fill_empty_page()
{
    log("Running need_to_fill_empty_page()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt1 = 10, dps_cnt2 = 10, dps_cnt3 = 1000;
    DataPointVector dps1, dps2, dps3;
    const char *metric = "compact.test.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps1, dps_cnt1, start);
    generate_data_points(dps2, dps_cnt2, start);
    generate_data_points(dps3, dps_cnt3, start);

    for (DataPointPair& dpp: dps1)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        dp.add_tag("tag", "1");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: dps2)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        dp.add_tag("tag", "2");
        tsdb->add(dp);
    }

    for (DataPointPair& dpp: dps3)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        //dp.add_tag(METRIC_TAG_NAME, metric);
        dp.add_tag("tag", "3");
        tsdb->add(dp);
    }

    // 2. retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));

    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: dps3) CONFIRM(contains(results, dp));

    // 3. make sure they occupy 4 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 4);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
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

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all 20 dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 2);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all dps are still there and correct;
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt1 + dps_cnt2 + dps_cnt3));
    for (auto& dp: dps1) CONFIRM(contains(results, dp));
    for (auto& dp: dps2) CONFIRM(contains(results, dp));
    for (auto& dp: dps3) CONFIRM(contains(results, dp));

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::need_to_fill_empty_page2(int compressor)
{
    log("Running need_to_fill_empty_page2(%d)...", compressor);

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnts[] = { 2, 2, 2, 2, 1000, 2 };
    DataPointVector dps[6];
    const char *metric = "compact.test.metric";
    int dps_cnt = 0;

    // make sure archive mode is NOT on
    update_config(now, compressor);
    clean_start(true);

    StringBuffer strbuf;    // must be declared after clean_start()!

    for (int i = 0; i < 6; i++)
    {
        generate_data_points(dps[i], dps_cnts[i], start);

        for (DataPointPair& dpp: dps[i])
        {
            Tsdb *tsdb = Tsdb::inst(dpp.first);
            DataPoint dp(dpp.first, dpp.second);
            dp.set_metric(metric);
            //dp.add_tag(METRIC_TAG_NAME, metric);
            dp.add_tag("tag", strbuf.strdup(std::to_string(i).c_str()));
            tsdb->add(dp);
        }

        dps_cnt += dps_cnts[i];
    }

    // 2. retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);

    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }

    // 3. make sure they occupy 7 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == ((compressor==0)?9:7));
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000, compressor); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
    update_config(3600000, compressor); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    TaskData data;
    data.integer = 1;
    Tsdb::compact(data);
    CONFIRM(Tsdb::inst(start)->is_compacted());
    log("compaction done");
    results.clear();        // query after compaction, before shutting down
    query_raw(metric, 0, results);
    log("results.size() = %d", results.size());
    CONFIRM(results.size() == dps_cnt);
    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }
    Tsdb::shutdown();

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all 20 dps reside on a single page;
    update_config(now, compressor);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == ((compressor==0)?4:2));
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all dps are still there and correct;
    results.clear();
    query_raw(metric, 0, results);
    log("results.size() = %d", results.size());
    CONFIRM(results.size() == dps_cnt);
    for (int i = 0; i < 6; i++)
    {
        for (auto& dp: dps[i]) CONFIRM(contains(results, dp));
    }

    clean_shutdown();
    m_stats.add_passed(1);
}

void
CompactTests::remove_duplicates()
{
    log("Running remove_duplicates()...");

    // 1. write 10 in order dps; then 10 out-of-order dps;
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 10, ooo_cnt = 15;
    DataPointVector dps, ooo_dps;
    const char *metric = "compact.test.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start+3600000);
    generate_data_points(ooo_dps, ooo_cnt, start);  // out-of-order dps

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
        //dp.add_tag(METRIC_TAG_NAME, metric);
        tsdb->add(dp);
    }

    // 2. retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    // query will remove duplicates
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("results.size() = %d", results.size());

    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));

    // 3. make sure they occupy 2 pages;
    log("page count = %d", Tsdb::get_page_count(false));
    log("ooo page count = %d", Tsdb::get_page_count(true));
    CONFIRM(Tsdb::get_page_count(false) == 1);
    CONFIRM(Tsdb::get_page_count(true) == 1);
    CONFIRM(! Tsdb::inst(start)->is_read_only());

    // 4. reload and check no data loss
    Tsdb::shutdown();
    update_config(3600000); // make sure pages are loaded in archive mode
    Config::init();
    Tsdb::init();
    CONFIRM(Tsdb::inst(start)->is_archived());
    results.clear();
    query_raw(metric, 0, results);
    // query will remove duplicates
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    log("results.size() = %d", results.size());

    // 5. perform compaction;
    log("perform compaction...");
    Tsdb::shutdown();
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

    // 6. make sure tsdb has no out-of-order dps anymore;
    //    and all dps reside on a single page;
    update_config(now);
    Config::init();
    Tsdb::init();
    CONFIRM(! Tsdb::inst(start)->is_archived());
    CONFIRM(Tsdb::inst(start)->is_compacted());
    CONFIRM(Tsdb::get_data_page_count() == 1);
    log("numer of pages after compaction: %d", Tsdb::get_data_page_count());

    // 7. make sure all dps are still there and correct, with duplicates removed
    results.clear();
    query_raw(metric, 0, results);
    CONFIRM(results.size() == (dps_cnt + ooo_cnt));
    for (auto& dp: dps) CONFIRM(contains(results, dp));
    for (auto& dp: ooo_dps) CONFIRM(contains(results, dp));
    log("results.size() = %d", results.size());

    clean_shutdown();
    m_stats.add_passed(1);
}


}
