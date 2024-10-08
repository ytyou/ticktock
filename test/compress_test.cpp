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

#include <algorithm>
#include <cstring>
#include "utils.h"
#include "compress_test.h"


using namespace tt;

namespace tt_test
{


void
CompressTests::run()
{
    Compressor::initialize();
    log("Running compress tests with millisecond resolution...");
    run_with(true);
    log("Running compress tests with second resolution...");
    run_with(false);
    log("Running best scenario case with millisecond resolution...");
    best_scenario(true);
    log("Running best scenario case with second resolution...");
    best_scenario(false);
    log("Running compress_v4 tests...");
    compress_v4_tests();
    log("Running rollup compression tests...");
    rollup_compress1();
    for (int i = 0; i < 10000; i++) rollup_compress2();
    rollup_compress3();
}

void
CompressTests::run_with(bool ms)
{
    log("Running %s...", m_name);

    uint8_t buff[131072];
    Compressor *compressor;

    tt::g_tstamp_resolution_ms = ms;

    Timestamp ts = ts_now();

    for (int v = 0; v <= 4; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, false, false);
        delete compressor;
    }

    for (int v = 0; v <= 4; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, false, true);
        delete compressor;
    }

    for (int v = 0; v <= 4; v++)
    {
        log("Testing save/restore for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        save_restore(compressor, ts);
        delete compressor;
    }

    for (int v = 0; v <= 4; v++)
    {
        log("Testing save/restore again for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        save_restore2(compressor, ts);
        delete compressor;
    }

    for (int v = 0; v <= 4; v++)
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
    int dp_cnt = 4500;
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

    log("uncompressed.size() = %d, dp_cnt = %d", uncompressed.size(), dp_cnt);
    CONFIRM(dp_cnt == uncompressed.size());

    for (int i = 0; i < dp_cnt; i++)
    {
        if ((std::fabs(dps[i].second - uncompressed[i].second)) >= 0.001)
        {
            log("t: exp=%" PRIu64 ", act=%" PRIu64 "; v: exp=%f, act=%f, diff=%f",
                dps[i].first, uncompressed[i].first, dps[i].second, uncompressed[i].second,
                std::fabs(dps[i].second - uncompressed[i].second));
        }

        CONFIRM(dps[i].first == uncompressed[i].first);
        CONFIRM(std::fabs(dps[i].second - uncompressed[i].second) < 0.0012);
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

    compressor->save(buff2);
    compressor->save(position);

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

    compressor->save(buff2);
    compressor->save(position);

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

    compressor->save(buff2);
    compressor->save(position);

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

    compressor->save(buff2);
    compressor->save(position);

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
        CONFIRM(compressor->get_dp_count() == n);

        for (DataPointPair& dp: dps)
        {
            if (! compressor->compress(dp.first, dp.second)) break;
            n++;
            CONFIRM(compressor->get_dp_count() == n);
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

    for (int v = 0; v <= 4; v++)
    {
        log("Testing compress/uncompress for Compressor_v%d...", v);
        compressor = Compressor::create(v);
        compressor->init(ts, buff, sizeof(buff));
        compress_uncompress(compressor, ts, true, false);
        delete compressor;
    }

    m_stats.add_passed(1);
}

void
CompressTests::compress_v4_tests()
{
    uint8_t buff[131072];
    DataPointVector dps;
    Compressor *compressor;
    Timestamp ts = ts_now();
    Timestamp ts_inc = 5000;
    double value = 123.456;
    double value_inc = 1.1;
    int cnt;

    tt::g_tstamp_resolution_ms = true;

    // 1 dp => 12 bytes
    compressor = Compressor::create(4);
    compressor->init(ts, buff, sizeof(buff));
    ts += ts_inc;
    compressor->compress(ts, value);
    log("compress4(): ts=%" PRIu64 ", val=%f", ts, value);
    CONFIRM(compressor->size() == 12);
    compressor->uncompress(dps);
    CONFIRM(dps.size() == 1);
    CONFIRM(dps[0].first == ts);
    CONFIRM(dps[0].second == value);
    log("uncompress4(): ts=%" PRIu64 ", val=%f", dps[0].first, dps[0].second);
    delete compressor;

    // 3 dp => 12 bytes + 6 bits
    cnt = 3;
    compressor = Compressor::create(4);
    compressor->init(ts, buff, sizeof(buff));
    for (int i = 0; i < cnt; i++)
    {
        ts += ts_inc;
        compressor->compress(ts, value);
    }
    log("compressor->size() == %u", compressor->size());
    CONFIRM(compressor->size() == 13);
    delete compressor;

    // 128 dp => 12 bytes + 2 bytes
    cnt = 128;
    compressor = Compressor::create(4);
    compressor->init(ts, buff, sizeof(buff));
    for (int i = 0; i < cnt; i++)
    {
        ts += ts_inc;
        compressor->compress(ts, value);
    }
    log("compressor->size() == %u", compressor->size());
    CONFIRM(compressor->size() == 14);
    delete compressor;

    // 256 dp => 12 bytes + 3 bytes
    cnt = 130;
    compressor = Compressor::create(4);
    compressor->init(ts, buff, sizeof(buff));
    for (int i = 0; i < cnt; i++)
    {
        ts += ts_inc;
        compressor->compress(ts, value);
    }
    log("compressor->size() == %u", compressor->size());
    CONFIRM(compressor->size() == 14);
    delete compressor;

    // 256 dp => 12 bytes + 3 bytes
    cnt = 258;
    compressor = Compressor::create(4);
    compressor->init(ts, buff, sizeof(buff));
    log("compressor->size() == %u", compressor->size());
    for (int i = 0; i < cnt; i++)
    {
        ts += ts_inc;
        value += value_inc;
        compressor->compress(ts, value);
    }
    log("compressor->size() == %u", compressor->size());
    CONFIRM(compressor->size() == 24);
    delete compressor;
}

void
CompressTests::rollup_compress1()
{
    uint8_t buff[4096];
    struct rollup_entry entry;
    uint32_t tid = 0;
    uint32_t cnt = 1;
    double min = 0.0;
    double max = 100.0;
    double sum = 84155849.918796;
    double precision = std::pow(10, 3);
    int len = 0;

    // compress
    int m = RollupCompressor_v1::compress(&buff[0], tid, cnt, min, max, sum, precision);
    CONFIRM(m >= 14);

    // uncompress
    int n = RollupCompressor_v1::uncompress(&buff[0], m, &entry, precision);
    CONFIRM(m == n);
    CONFIRM(tid == entry.tid);
    CONFIRM(cnt == entry.cnt);
    CONFIRM(min == entry.min);
    CONFIRM(max == entry.max);
    CONFIRM(std::abs(sum - entry.sum) < 0.001);

    m_stats.add_passed(1);
}

void
CompressTests::rollup_compress2()
{
    uint8_t buff[4096];
    struct rollup_entry entries[100];
    double precision = std::pow(10, 3);
    int len = 0;

    // generate data
    for (int i = 0; i < sizeof(entries)/sizeof(entries[0]); i++)
    {
        entries[i].tid = tt::random(0, 1000000);
        entries[i].cnt = tt::random(0, 3600);
        entries[i].min = tt::random(-10000.0, 10000.0);
        entries[i].max = tt::random(-1000000.0, 1000000.0);
        entries[i].sum = tt::random(-100000000.0, 100000000.0);
    }

    // compress
    for (int i = 0; i < sizeof(entries)/sizeof(entries[0]); i++)
    {
        CONFIRM((sizeof(buff) - len) >= 33);
        int n = RollupCompressor_v1::compress(&buff[len], entries[i].tid, entries[i].cnt, entries[i].min, entries[i].max, entries[i].sum, precision);
        CONFIRM(n >= 14 || entries[i].cnt == 0);
        len += n;
    }

    // uncompress
    int idx = 0;

    for (int i = 0; i < sizeof(entries)/sizeof(entries[0]); i++)
    {
        struct rollup_entry entry;
        int n = RollupCompressor_v1::uncompress(&buff[idx], len-idx, &entry, precision);
        CONFIRM(n >= 14 || entries[i].cnt == 0);
        CONFIRM(entries[i].tid == entry.tid);
        CONFIRM(entries[i].cnt == entry.cnt);

        if (entry.cnt != 0)
        {
            CONFIRM(std::abs(entries[i].min - entry.min) < 0.001);
            CONFIRM(std::abs(entries[i].max - entry.max) < 0.001);
            CONFIRM(std::abs(entries[i].sum - entry.sum) < 0.001);
        }

        idx += n;
    }

    m_stats.add_passed(1);
}

void
CompressTests::rollup_compress3()
{
    struct rollup_entry entries[22000];

    // generate data
    log("generating data...");
    for (int i = 0; i < sizeof(entries)/sizeof(entries[0]); i++)
    {
        entries[i].tid = tt::random(0, 1000000);
        entries[i].cnt = tt::random(0, 36000);
        entries[i].min = tt::random(-10000.0, 10000.0);
        entries[i].max = tt::random(-1000000.0, 1000000.0);
        entries[i].sum = tt::random(-100000000.0, 100000000.0);
    }

    m_buff_offset = m_disk_offset = m_disk_size = 0;
    m_precision = std::pow(10, 5);

    // compress
    log("compress data...");
    for (int i = 0; i < sizeof(entries)/sizeof(entries[0]); i++)
        add_data_point(entries[i]);

    // flush
    if (m_buff_offset > 0)
    {
        std::memcpy(m_disk+m_disk_offset, m_buff, m_buff_offset);
        m_disk_offset += m_buff_offset;
        m_disk_size += m_buff_offset;
        m_buff_offset = 0;
    }

    log("disk size: %d", m_disk_size);

    // uncompress: simulates RollupDataFile::query()
    log("uncompress data...");
    uint8_t buff[4096];
    std::size_t n;
    int len, offset = 0;
    int entry_idx = 0;

    m_disk_offset = 0;  // rewind

    while ((n = read_disk(&buff[offset], sizeof(buff)-offset)) > 0)
    {
        ASSERT((n + offset) <= sizeof(buff));

        n += offset;
        ASSERT(n <= sizeof(buff));
        offset = 0;

        for (int i = 0; i < n; i += len)
        {
            ASSERT(i < sizeof(buff));
            ASSERT(n <= sizeof(buff));
            ASSERT((n-i) <= sizeof(buff));

            struct rollup_entry entry;
            len = RollupCompressor_v1::uncompress((uint8_t*)(buff+i), n-i, &entry, m_precision);

            if (len == 0)
            {
                ASSERT(i > 0);
                // copy remaining unprocessed data to the beginning of buff
                for (int j = 0; (i+j) < n; j++)
                    buff[j] = buff[i+j];
                offset = n - i;
                break;
            }

            int failed = m_stats.get_failed();

            CONFIRM(entries[entry_idx].tid == entry.tid);
            CONFIRM(entries[entry_idx].cnt == entry.cnt);

            if (entry.cnt != 0)
            {
                CONFIRM(std::abs(entries[entry_idx].min - entry.min) < 0.0005);
                CONFIRM(std::abs(entries[entry_idx].max - entry.max) < 0.0005);
                CONFIRM(std::abs(entries[entry_idx].sum - entry.sum) < 0.014);
            }

            if (failed < m_stats.get_failed())
            {
                log("entry_idx = %d", entry_idx);
                log("entries[entry_idx].tid=%u, entry.tid=%u", entries[entry_idx].tid, entry.tid);
                log("entries[entry_idx].cnt=%u, entry.cnt=%u", entries[entry_idx].cnt, entry.cnt);
                log("entries[entry_idx].min=%f, entry.min=%f", entries[entry_idx].min, entry.min);
                log("entries[entry_idx].max=%f, entry.max=%f", entries[entry_idx].max, entry.max);
                log("entries[entry_idx].sum=%f, entry.sum=%f", entries[entry_idx].sum, entry.sum);
            }

            entry_idx++;
        }
    }

    m_stats.add_passed(1);
}

// This simulates RollupDataFile::add_data_point()
void
CompressTests::add_data_point(struct rollup_entry& entry)
{
    int size;
    uint8_t buff[128];

    size = RollupCompressor_v1::compress(buff, entry.tid, entry.cnt, entry.min, entry.max, entry.sum, m_precision);

    if (sizeof(m_buff) < (m_buff_offset + size))
    {
        ASSERT((m_disk_offset + m_buff_offset) < sizeof(m_disk));
        std::memcpy(m_disk+m_disk_offset, m_buff, m_buff_offset);
        m_disk_offset += m_buff_offset;
        m_disk_size += m_buff_offset;
        m_buff_offset = 0;
    }

    std::memcpy(m_buff+m_buff_offset, buff, size);
    m_buff_offset += size;
}

// simualtes std::fread()
std::size_t
CompressTests::read_disk(uint8_t *buff, std::size_t size)
{
    ASSERT((m_disk_offset+size) < sizeof(m_disk));
    int len = std::min((int)size, (int)m_disk_size-m_disk_offset);
    if (len > 0)
        std::memcpy(buff, m_disk+m_disk_offset, len);
    m_disk_offset += len;
    return len;
}


}
