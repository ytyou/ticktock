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

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "dp.h"
#include "recycle.h"
#include "task.h"


namespace tt
{


#define MAX_USAGE_SIZE  12

class QueryTask;


// this is a per thread singleton
class MemoryManager
{
public:
    static void init();
    static void cleanup();

    static void log_stats();
    static void collect_stats(Timestamp ts, std::vector<DataPoint> &dps);
    static int get_recyclable_total();

    // network buffer
    static char *alloc_network_buffer();
    static void free_network_buffer(char *buff);

    inline static uint64_t get_network_buffer_size()
    {
        return m_network_buffer_len;
    }

    // tsdb buffer
    static uint8_t *alloc_tsdb_buffer();
    static void free_tsdb_buffer(uint8_t *buff);

    inline static uint64_t get_tsdb_buffer_size()
    {
        return m_tsdb_buffer_len;
    }

    static Recyclable *alloc_recyclable(RecyclableType type);
    static void free_recyclable(Recyclable *r);
    static void free_recyclables(Recyclable *r);
    static void assert_recyclable(Recyclable *r);   // for debugging only
    static bool collect_garbage(TaskData& data);

private:
    MemoryManager();

    static bool m_initialized;      // must initialize before using
    static uint64_t m_network_buffer_len;
    static uint64_t m_tsdb_buffer_len;

    static std::mutex m_network_lock;
    static char *m_network_buffer_free_list;

    static std::mutex m_tsdb_lock;
    static uint8_t *m_tsdb_buffer_free_list;

    static std::mutex m_page_lock;
    static void *m_page_free_list;

    static std::mutex m_locks[RecyclableType::RT_COUNT];
    static Recyclable *m_free_lists[RecyclableType::RT_COUNT];

    // keep track of number of reusable objects,
    // both free and in total
    static std::atomic<int> m_free[RecyclableType::RT_COUNT+2];
    static std::atomic<int> m_total[RecyclableType::RT_COUNT+2];

    // garbage collector
    static std::mutex m_garbage_lock;   // to prevent multi-invoke of collect_garbage()
    static int m_max_usage[RecyclableType::RT_COUNT+1][MAX_USAGE_SIZE];
    static int m_max_usage_idx;

#ifdef _DEBUG
    // for debugging only
    static std::unordered_map<Recyclable*,bool> m_maps[RecyclableType::RT_COUNT];
#endif
};


}
