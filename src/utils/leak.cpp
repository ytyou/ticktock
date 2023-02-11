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

#include <mutex>
#include <unordered_map>
#include "logger.h"
#include "leak.h"


namespace tt
{


#ifdef _LEAK_DETECTION

static std::mutex mem_lock;
static std::unordered_map<void *, mem_info *> mem_map;


void
ld_add(void *p, size_t size, const char *file, int line)
{
    ASSERT(p != nullptr);
    ASSERT(file != nullptr);

    mem_info *info = new mem_info;
    info->size = size;
    info->line = line;
    strncpy(info->file, file, sizeof(info->file));
    strncpy(info->thread, g_thread_id.c_str(), sizeof(info->thread));

    std::lock_guard<std::mutex> guard(mem_lock);
    ASSERT(mem_map.find(p) == mem_map.end());

    mem_map[p] = info;
    Logger::trace("Memory %p added at file %s, line %d", p, file, line);
}

void
ld_del(void *p, const char *file, int line)
{
    ASSERT(p != nullptr);

    std::lock_guard<std::mutex> guard(mem_lock);
    auto search = mem_map.find(p);

    if (search == mem_map.end())
    {
        Logger::error("Trying to free %p that's not allocated by us (file=%s, line=%d)", p, file, line);
    }
    else
    {
        mem_info *info = search->second;
        ASSERT(info != nullptr);
        free(info);
        mem_map.erase(search);
    }
}

unsigned long
ld_stats(const char *msg)
{
    if (msg != nullptr)
    {
        Logger::info("mem-leak: %s", msg);
    }

    uint64_t total = 0;
    std::lock_guard<std::mutex> guard(mem_lock);

    for (const auto& mem: mem_map)
    {
        void *p = mem.first;
        mem_info *info = mem.second;

        ASSERT(p != nullptr);
        ASSERT(info != nullptr);

        total += (unsigned long)info->size;

        if (msg != nullptr)
        {
            Logger::info("mem-leak: p=%p, size=%d, thread=%s, file=%s, line=%d",
                p, info->size, info->thread, info->file, info->line);
        }
    }

    Logger::info("mem-leak: Total of %" PRIu64 " bytes allocated", total);
    return total;
}

void
ld_free(void *p, const char *file, int line)
{
    ld_del(p, file, line);
    free(p);
}

void *
ld_malloc(size_t size, const char *file, int line)
{
    ASSERT(size > 0);
    ASSERT(file != nullptr);

    void *p = malloc(size);
    ASSERT(p != nullptr);
    ld_add(p, size, file, line);
    return p;
}

char *
ld_strdup(const char *str, const char *file, int line)
{
    ASSERT(str != nullptr);
    ASSERT(file != nullptr);

    char *dup = strdup(str);
    ld_add(dup, strlen(dup), file, line);
    return dup;
}

#endif


}
