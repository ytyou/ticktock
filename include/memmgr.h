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

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "recycle.h"


namespace tt
{


class QueryTask;


// this is a per thread singleton
class MemoryManager
{
public:
    static MemoryManager* inst();

    static void init();
    static void cleanup();

    static void log_stats();
    static int get_recyclable_total();

    // network buffer
    static char* alloc_network_buffer();
    static void free_network_buffer(char *buff);

    inline static uint64_t get_network_buffer_size()
    {
        return m_network_buffer_len;
    }

    static Recyclable *alloc_recyclable(RecyclableType type);
    static void free_recyclable(Recyclable *r);
    static void free_recyclables(Recyclable *r);
    static void assert_recyclable(Recyclable *r);   // for debugging only

private:
    MemoryManager();

    static bool m_initialized;      // must initialize before using
    static uint64_t m_network_buffer_len;

    char *m_network_buffer_free_list;

    static std::mutex m_page_lock;
    static void *m_page_free_list;

    static std::mutex m_locks[RecyclableType::RT_COUNT];
    static Recyclable *m_free_lists[RecyclableType::RT_COUNT];
#ifdef _DEBUG
    // for debugging only
    static std::unordered_map<Recyclable*,bool> m_maps[RecyclableType::RT_COUNT];
#endif
};


}
