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

#include "query_test.h"


using namespace tt;

namespace tt_test
{


void
QueryTests::run()
{
    log("Running %s...", m_name);

    std::thread t1(&QueryTests::basic_query_tests, this);
    t1.join();

    std::thread t2(&QueryTests::duplicate_dp_tests, this);
    t2.join();
    //basic_query_tests();
    //duplicate_dp_tests();

    log("Finished %s", m_name);
}

void
QueryTests::update_config(Timestamp archive_ms)
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

    create_config(configs);
    Config::init();
}

void
QueryTests::basic_query_tests()
{
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 20;
    DataPointVector dps;
    const char *metric = "query.test.basic.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start);

    // insert original data points
    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    CONFIRM(Tsdb::get_dp_count() == dps_cnt);
    log("dp count = %d", Tsdb::get_dp_count());

    // retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);

    for (auto& dp: dps) CONFIRM(contains(results, dp));

    clean_shutdown();
    m_stats.add_passed(1);
}

void
QueryTests::duplicate_dp_tests()
{
    Timestamp start = 946684800000; // 2020-01-01
    Timestamp now = ts_now_ms();
    int dps_cnt = 256;
    DataPointVector dps;
    const char *metric = "query.test.dedup.metric";

    // make sure archive mode is NOT on
    update_config(now);
    clean_start(true);
    generate_data_points(dps, dps_cnt, start);

    // insert original data points
    for (DataPointPair& dpp: dps)
    {
        Tsdb *tsdb = Tsdb::inst(dpp.first);
        DataPoint dp(dpp.first, dpp.second);
        dp.set_metric(metric);
        tsdb->add(dp);
    }

    CONFIRM(Tsdb::get_dp_count() == dps_cnt);
    log("dp count = %d", Tsdb::get_dp_count());

    // retrieve all dps and make sure they are correct;
    DataPointVector results;
    query_raw(metric, 0, results);
    CONFIRM(results.size() == dps_cnt);

    log("no duplicate cases...");
    for (auto& dp: dps) CONFIRM(contains(results, dp));

    // create duplicate data points a few times
    for (int i = 0; i < 10; i++)
    {
        for (auto& dp: dps)
            dp.second += tt::random(1.0, 10.0);
        for (auto& dp: dps) CONFIRM(! contains(results, dp));

        for (DataPointPair& dpp: dps)
        {
            Tsdb *tsdb = Tsdb::inst(dpp.first);
            DataPoint dp(dpp.first, dpp.second);
            dp.set_metric(metric);
            tsdb->add(dp);
        }

        results.clear();
        query_raw(metric, 0, results);
        CONFIRM(results.size() == dps_cnt);

        log("duplicate cases, iteration %d...", i);
        for (auto& dp: dps) CONFIRM(contains(results, dp));
    }

    clean_shutdown();
    m_stats.add_passed(1);
}


}
