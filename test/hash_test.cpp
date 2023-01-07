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
#include "mmap.h"
#include "hash_test.h"


using namespace tt;

namespace tt_test
{


void
HashTests::run()
{
    log("Running %s...", m_name);

    // prepare data
    std::size_t buff_size = 100000000;
    m_buff = (char*)malloc(buff_size);
    m_num_keys = 0;

    int curr = 0;
    int min = 1, max = 128;
    std::unordered_set<std::string> keys;

    while ((buff_size - curr - 1) > max)
    {
        int len = gen_random_string(&m_buff[curr], min, max);
        std::string s(&m_buff[curr]);
        if (keys.find(s) == keys.end())
        {
            curr += len;
            m_num_keys++;
            keys.insert(s);
        }
    }

    log("Generated %d random strings", m_num_keys);

    index_tests();

    std::free(m_buff);

    log("Finished %s", m_name);
}

void
HashTests::index_tests()
{
    tt::Timestamp ts = ts_now_ms();
    tt::PerfectHash pf(m_buff, m_num_keys);

    log("PerfectHash build time: %" PRIu64 " ms", ts_now_ms()-ts);

    std::unordered_set<uint64_t> indices;
    char *key = m_buff;
    int cnt;

    ts = ts_now_ms();

    for (cnt = 0; cnt < m_num_keys; cnt++, key = std::strchr(key, '\0')+1)
    {
        uint64_t i = pf.lookup(key);
        CONFIRM(i <= m_num_keys);
        CONFIRM(indices.find(i) == indices.end());
        indices.insert(i);
    }

    log("PerfectHash lookup avg time: %lf ms", ((double)(ts_now_ms()-ts))/(double)m_num_keys);

    m_stats.add_passed(1);
}


}
