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
#include <memory>
#include <string>
#include "hash_test.h"
#include "tsl/robin_map.h"


using namespace tt;

namespace tt_test
{


void
HashTests::run()
{
    log("Running %s...", m_name);

    // prepare data
    //std::size_t buff_size = 100000000;
    //m_buff = (char*)malloc(buff_size);
    std::vector<tt::perfect_entry> entries;
    HashBuffer hash_buff(1048576);
    char str_buff[130];
    m_num_keys = 0;

    int min = 1, max = 128;
    std::unordered_set<std::string> keys;

    for (uint32_t i = 0; i < 2000000; i++)
    {
        int len = gen_random_string(str_buff, min, max);
        std::string s(str_buff);
        if (keys.find(s) == keys.end())
        {
            keys.insert(str_buff);
            entries.emplace_back(hash_buff.strdup(str_buff), m_num_keys++);
        }
    }

    CONFIRM(entries.size() == m_num_keys);
    keys.clear();
    log("Generated %d random strings", m_num_keys);

    in_memory_map_tests(entries);
    perfect_hash_tests(entries);
    robin_hash_tests(entries);

    log("Finished %s", m_name);
}

void
HashTests::perfect_hash_tests(std::vector<tt::perfect_entry>& entries)
{
    tt::Timestamp ts = ts_now_ms();
    tt::PerfectHash ph(entries);

    log("PerfectHash build time: %" PRIu64 " ms", ts_now_ms()-ts);

    uint8_t indices[m_num_keys+1];

    for (uint32_t i = 0; i <= m_num_keys; i++)
        indices[i] = 0;

    ts = ts_now_ms();

    for (uint32_t i = 0; i < m_num_keys; i++)
    {
        uint64_t idx = ph.lookup_internal(entries[i].key, PerfectHash::hash(entries[i].key));
        CONFIRM(idx <= m_num_keys);
        CONFIRM(indices[idx] == 0);
        indices[idx] = 1;
    }

    log("PerfectHash lookup_internal avg time: %lf ms", ((double)(ts_now_ms()-ts))/(double)m_num_keys);

    CONFIRM(indices[0] == 0);
    for (uint32_t i = 1; i <= m_num_keys; i++)
        CONFIRM(indices[i] == 1);

    ts = ts_now_ms();

    for (uint32_t i = 0; i < entries.size(); i++)
    {
        TimeSeriesId id = ph.lookup(entries[i].key, PerfectHash::hash(entries[i].key));
        CONFIRM(id <= entries[i].id);
    }

    log("PerfectHash lookup avg time: %lf ms", ((double)(ts_now_ms()-ts))/(double)m_num_keys);

    m_stats.add_passed(1);
}

void
HashTests::in_memory_map_tests(std::vector<tt::perfect_entry>& entries)
{
    InMemoryMap map;
    tt::Timestamp ts = ts_now_ms();

    // insert
    for (int i = 0; i < entries.size(); i++)
    {
        uint64_t hash = PerfectHash::hash(entries[i].key);
        CONFIRM(map.set(entries[i].key, hash, entries[i].id));
    }

    log("InMemoryMap build time: %" PRIu64 " ms", ts_now_ms()-ts);
    log("map size = %" PRIu64 "; m_num_keys = %" PRIu64, map.size(), m_num_keys);
    CONFIRM(map.size() == m_num_keys);

    // lookup...
    ts = ts_now_ms();

    for (int i = 0; i < entries.size(); i++)
    {
        uint64_t hash = PerfectHash::hash(entries[i].key);
        TimeSeriesId id = map.get(entries[i].key, hash);
        if (id != entries[i].id)
            log("id = %u; entries[i].id = %u, i = %d", id, entries[i].id, i);
        CONFIRM(id == entries[i].id);
    }

    log("InMemoryMap lookup avg time: %lf ms", ((double)(ts_now_ms()-ts))/(double)m_num_keys);

    m_stats.add_passed(1);
}

void
HashTests::robin_hash_tests(std::vector<tt::perfect_entry>& entries)
{
    tsl::robin_map<const char*,TimeSeriesId,hash_func,eq_func> map;
    tt::Timestamp ts = ts_now_ms();

    // insert
    for (int i = 0; i < entries.size(); i++)
        map[entries[i].key] = entries[i].id;

    log("robin hash build time: %" PRIu64 " ms", ts_now_ms()-ts);
    log("map size = %" PRIu64 "; m_num_keys = %" PRIu64, map.size(), m_num_keys);
    CONFIRM(map.size() == m_num_keys);

    // lookup...
    ts = ts_now_ms();

    for (int i = 0; i < entries.size(); i++)
    {
        auto search = map.find(entries[i].key);
        CONFIRM(search != map.end());
        CONFIRM(search->second == entries[i].id);
    }

    log("robin hash lookup avg time: %lf ms", ((double)(ts_now_ms()-ts))/(double)m_num_keys);

    m_stats.add_passed(1);
}


}
