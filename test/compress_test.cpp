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

#include "utils.h"
#include "compress_test.h"


using namespace tt;

namespace tt_test
{


void
CompressTests::run()
{
    log("Running compress tests with millisecond resolution...");
    Compressor_v3::initialize();
    run_with(true);
    log("Running compress tests with second resolution...");
    run_with(false);
    log("Running best scenario case with millisecond resolution...");
    best_scenario(true);
    log("Running best scenario case with second resolution...");
    best_scenario(false);
}

void
CompressTests::run_with(bool ms)
{
    log("Running %s...", m_name);

    uint8_t buff[131072];
    Compressor *compressor;

    tt::g_tstamp_resolution_ms = ms;

    Timestamp ts = ts_now();

    for (int v = 0; v <= 3; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, false, false);
        delete compressor;
    }

    for (int v = 0; v <= 3; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, false, true);
        delete compressor;
    }

    for (int v = 0; v <= 3; v++)
    {
        log("Testing save/restore for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        save_restore(compressor, ts);
        delete compressor;
    }

    for (int v = 0; v <= 3; v++)
    {
        log("Testing save/restore again for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        save_restore2(compressor, ts);
        delete compressor;
    }

    for (int v = 0; v <= 3; v++)
    {
        log("Stress testing for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        stress_test(compressor, ts);
        delete compressor;
    }

    log("Finished %s", m_name);
    m_stats.add_passed(1);
}

void
CompressTests::compress_uncompress(Compressor *compressor, Timestamp ts, bool best, bool floating)
{
    int dp_cnt = 5000;
    DataPointVector dps;
    double val = 123.456;
    Timestamp interval = (tt::g_tstamp_resolution_ms) ? 30000 : 30;

    if (best)
    {
        dps.emplace_back(ts, val);
        for (int i = 1; i < dp_cnt; i++)
            dps.emplace_back(dps[i-1].first+interval, val);
    }
    else if (floating)
        generate_data_points_float(dps, dp_cnt, ts);
    else
        generate_data_points(dps, dp_cnt, ts);

    for (int i = 0; i < dp_cnt; i++)
        CONFIRM(compressor->compress(dps[i].first, dps[i].second));

    log("compressor.size() = %d", compressor->size());
    std::vector<std::pair<Timestamp,double>> uncompressed;
    compressor->uncompress(uncompressed);

    log("uncompressed.size() = %d", uncompressed.size());
    CONFIRM(dp_cnt == uncompressed.size());

    for (int i = 0; i < dp_cnt; i++)
    {
        //log("t: exp=%" PRIu64 ", act=%" PRIu64 "; v: exp=%f, act=%f",
            //dps[i].first, uncompressed[i].first, dps[i].second, uncompressed[i].second);
        CONFIRM(dps[i].first == uncompressed[i].first);
        CONFIRM(std::fabs(dps[i].second - uncompressed[i].second) < 0.001);
    }

    log("compression ratio = %f", (16.0*dp_cnt)/compressor->size());
    log("average #bytes per dp = %f", (double)compressor->size()/dp_cnt);

    m_stats.add_passed(1);
}

void
CompressTests::save_restore(Compressor *compressor, Timestamp ts)
{
    uint8_t buff2[131072], buff3[131072];
    int dp_cnt = 5000;
    DataPointVector dps;

    generate_data_points(dps, dp_cnt, ts);

    for (DataPointPair& dp: dps)
    {
        CONFIRM(compressor->compress(dp.first, dp.second));
    }

    CONFIRM(compressor->get_dp_count() == dp_cnt);

    CompressorPosition position;

    compressor->save(position);
    compressor->save(buff2);

    std::vector<std::pair<Timestamp,double>> uncompressed;
    compressor->init(ts, buff3, sizeof(buff3));
    compressor->restore(uncompressed, position, buff2);

    std::vector<std::pair<Timestamp,double>> uncompressed2;
    compressor->uncompress(uncompressed2);

    CONFIRM(uncompressed.size() == dp_cnt);
    CONFIRM(uncompressed2.size() == dp_cnt);

    for (int j = 0; j < dp_cnt; j++)
    {
        CONFIRM(dps[j].first == uncompressed[j].first);
        CONFIRM(dps[j].second == uncompressed[j].second);
        CONFIRM(dps[j].first == uncompressed2[j].first);
        CONFIRM(dps[j].second == uncompressed2[j].second);
    }

    m_stats.add_passed(1);
}

void
CompressTests::save_restore2(Compressor *compressor, Timestamp ts)
{
    uint8_t buff2[131072], buff3[131072];
    CompressorPosition position;
    DataPointVector dps;
    DataPointVector uncompressed, uncompressed2;
    int dps_cnt = 5001;

    generate_data_points(dps, dps_cnt, ts);

    for (int i = 0; i < 1000; i++)
    {
        CONFIRM(compressor->compress(dps[i].first, dps[i].second));
    }

    CONFIRM(compressor->get_dp_count() == 1000);

    compressor->save(position);
    compressor->save(buff2);

    uncompressed.clear();
    compressor->init(ts, buff3, sizeof(buff3));
    compressor->restore(uncompressed, position, buff2);

    uncompressed2.clear();
    compressor->uncompress(uncompressed2);

    CONFIRM(uncompressed.size() == 1000);
    CONFIRM(uncompressed2.size() == 1000);

    for (int j = 0; j < 1000; j++)
    {
        CONFIRM(dps[j].first == uncompressed[j].first);
        CONFIRM(dps[j].second == uncompressed[j].second);
        CONFIRM(dps[j].first == uncompressed2[j].first);
        CONFIRM(dps[j].second == uncompressed2[j].second);
    }

    for (int i = 1000; i < 2000; i++)
    {
        CONFIRM(compressor->compress(dps[i].first, dps[i].second));
    }

    CONFIRM(compressor->get_dp_count() == 2000);

    compressor->save(position);
    compressor->save(buff2);

    uncompressed.clear();
    compressor->init(ts, buff3, sizeof(buff3));
    compressor->restore(uncompressed, position, buff2);

    uncompressed2.clear();
    compressor->uncompress(uncompressed2);

    CONFIRM(uncompressed.size() == 2000);
    CONFIRM(uncompressed2.size() == 2000);

    for (int j = 0; j < 2000; j++)
    {
        CONFIRM(dps[j].first == uncompressed[j].first);
        CONFIRM(dps[j].second == uncompressed[j].second);
        CONFIRM(dps[j].first == uncompressed2[j].first);
        CONFIRM(dps[j].second == uncompressed2[j].second);
    }

    for (int i = 2000; i < dps.size(); i++)
    {
        CONFIRM(compressor->compress(dps[i].first, dps[i].second));
    }

    CONFIRM(compressor->get_dp_count() == dps.size());

    compressor->save(position);
    compressor->save(buff2);

    uncompressed.clear();
    compressor->init(ts, buff3, sizeof(buff3));
    compressor->restore(uncompressed, position, buff2);

    uncompressed2.clear();
    compressor->uncompress(uncompressed2);

    CONFIRM(uncompressed.size() == dps.size());
    CONFIRM(uncompressed2.size() == dps.size());

    for (int j = 0; j < dps.size(); j++)
    {
        CONFIRM(dps[j].first == uncompressed[j].first);
        CONFIRM(dps[j].second == uncompressed[j].second);
        CONFIRM(dps[j].first == uncompressed2[j].first);
        CONFIRM(dps[j].second == uncompressed2[j].second);
    }

    m_stats.add_passed(1);
}

void
CompressTests::stress_test(Compressor *compressor, Timestamp ts)
{
    DataPointVector dps;
    uint8_t page[4096];
    int n;

    generate_data_points(dps, 5000, ts);

    auto start = std::chrono::system_clock::now();

    for (int i = 0; i < 5000; i++)
    {
        n = 0;

        compressor->init(ts, page, sizeof(page));

        for (DataPointPair& dp: dps)
        {
            if (! compressor->compress(dp.first, dp.second)) break;
            n++;
        }

        CONFIRM(compressor->get_dp_count() == n);

        DataPointVector uncompressed;
        compressor->uncompress(uncompressed);

        CONFIRM(uncompressed.size() == n);

        for (int j = 0; j < n; j++)
        {
            CONFIRM(dps[j].first == uncompressed[j].first);
            CONFIRM(dps[j].second == uncompressed[j].second);
        }
    }

    using namespace std::chrono;
    system_clock::time_point end = system_clock::now();
    auto ms = duration_cast<milliseconds>(end - start).count();
    log("compress_stress_test(): %d dps in %lf ms", n, (double)ms);

    m_stats.add_passed(1);
}

void
CompressTests::best_scenario(bool ms)
{
    uint8_t buff[131072];
    Compressor *compressor;

    tt::g_tstamp_resolution_ms = ms;

    Timestamp ts = ts_now();

    for (int v = 0; v <= 3; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, true, false);
        delete compressor;
    }
}


}
