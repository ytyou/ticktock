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

#include <cstring>
#include "config.h"
#include "hash.h"
#include "logger.h"
#include "meta.h"
#include "mmap.h"
#include "timer.h"
#include "ts.h"
#include "type.h"
#include "utils.h"


namespace tt
{


//std::atomic<uint64_t> cache_hit{0};
//std::atomic<uint64_t> cache_miss{0};
SuperMap * SuperMap::m_instance;


InMemoryMap::InMemoryMap() :
    m_read_only(false)
{
}

InMemoryMap::~InMemoryMap()
{
    Logger::info("InMemoryMap::~InMemoryMap(%p) called", this);
}

std::shared_ptr<InMemoryMap>
InMemoryMap::create()
{
    Logger::info("Creating InMemoryMap...");
    return std::shared_ptr<InMemoryMap>(new InMemoryMap());
}

void
InMemoryMap::set_read_only()
{
    WriteLock guard(m_lock);
    m_read_only = true;
}

TimeSeriesId
InMemoryMap::get(const char *key, uint64_t hash)
{
    ReadLock guard(m_lock);
    SuperKey super_key(key, hash);
    auto search = m_map.find(super_key);
    if (search == m_map.end())
        return TT_INVALID_TIME_SERIES_ID;
    return search->second;
}

bool
InMemoryMap::set(const char *key, uint64_t hash, TimeSeriesId id)
{
    WriteLock guard(m_lock);
    if (is_read_only()) return false;
    SuperKey super_key(key, hash);
    m_map[super_key] = id;
    return true;
}

void
InMemoryMap::collect(std::vector<struct perfect_entry>& entries)
{
    for (auto it: m_map)
        entries.emplace_back(it.first.m_key, it.second);
}


PerfectHash::PerfectHash(std::vector<struct perfect_entry>& entries)
{
    m_count = entries.size();
    m_buckets = (struct perfect_entry*) calloc(m_count+1, sizeof(struct perfect_entry));
    construct(entries);
}

PerfectHash::~PerfectHash()
{
    Logger::info("PerfectHash::~PerfectHash() called");

    if (m_buckets != nullptr)
        std::free(m_buckets);

    // delete the InMemoryMap...
    SuperMap::instance()->erase();
}

std::shared_ptr<PerfectHash>
PerfectHash::create(MetaFile *meta_file)
{
    ASSERT(meta_file != nullptr);
    std::vector<struct perfect_entry> entries;
    return std::shared_ptr<PerfectHash>(new PerfectHash(entries));
}

std::shared_ptr<PerfectHash>
PerfectHash::create(std::shared_ptr<PerfectHash> ph, std::shared_ptr<InMemoryMap> map)
{
    std::vector<struct perfect_entry> entries;

    if (ph != nullptr)
    {
        for (uint32_t i = 1; i <= ph->m_count; i++)
            entries.emplace_back(ph->m_buckets[i]);
    }

    if (map != nullptr)
        map->collect(entries);

    return std::shared_ptr<PerfectHash>(new PerfectHash(entries));
}

TimeSeriesId
PerfectHash::lookup(const char *key, uint64_t h)
{
    uint64_t idx = lookup_internal(key, h);
    if ((idx != 0) && (std::strcmp(key, m_buckets[idx].key) != 0))
        idx = 0;
    if (idx == 0)
    {
        return TT_INVALID_TIME_SERIES_ID;
    }
    else
    {
        return m_buckets[idx].id;
    }
}

uint64_t
PerfectHash::lookup_internal(const char *key, uint64_t h)
{
    ASSERT(key != nullptr);
    ASSERT(hash(key) == h);

    uint32_t h1 = (uint32_t)(h & 0xFFFFFFFF);
    uint32_t h2 = (uint32_t)(h >> 32);
    uint32_t level = 0;

    for (BitSet64& b: m_bits)
    {
        ASSERT(b.capacity64() > 0);
        uint32_t size_in_bits = b.capacity64() * 64;
        uint64_t idx = (h1 ^ rotl(h2, level)) % size_in_bits;

        if (! b.test(idx))
        {
            level++;
            continue;
        }

        uint64_t rank = m_ranks[level][idx/512];

        for (std::size_t i = (idx / 64) & ~7; i < idx/64; i++)
            rank += b.pop64(i);

        uint64_t n = b.get64(idx/64);
        uint64_t p = ((idx % 64) == 0) ? 0 : __builtin_popcountl((uint64_t)n << (64 - (idx % 64)));
        rank += p + 1;

        return rank;
    }

    return 0;
}

uint64_t
PerfectHash::hash(uint64_t x)
{
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    return x * 2685821657736338717;
}

uint64_t
PerfectHash::hash(const char *s)
{
    uint64_t h = 0;
    long len = std::strlen(s);

    if (len < 8)
    {
        //std::memcpy(((char*)(&h))+8-len, s, len);
        std::memcpy(((char*)(&h)), s, len);
        return hash(h);
    }
    else
    {
        for ( ; ; )
        {
            h = rotl(h,11) ^ hash(*((uint64_t*)(s)));
            if (len <= 0) break;
            len -= 8;
            if (len < 8)
            {
                s += len;
                len = 0;
            }
            else
                s += 8;
        }

        return h;
    }
}

uint32_t
PerfectHash::rotl(uint32_t x, uint32_t y)
{
    //y &= 63;
    return (x << y) | (x >> (32 - y));
}

uint32_t
PerfectHash::calc_index(const char *key, uint32_t level, uint32_t size)
{
    ASSERT(key != nullptr);
    ASSERT(size > 0);

    uint64_t h = hash(key);
    uint32_t h1 = (uint32_t)(h & 0xFFFFFFFF);
    uint32_t h2 = (uint32_t)(h >> 32);
    return (h1 ^ rotl(h2, level)) % size;
}

void
PerfectHash::construct(std::vector<struct perfect_entry>& entries)
{
    uint32_t count = entries.size();
    uint32_t level = 0;
    uint32_t size = 2 * count;
    size = (size + 63) & ~63;
    ASSERT((size % 64) == 0);

    BitSet64 exists(size);
    BitSet64 collide(size);
    std::vector<const char*> redo;

    for (auto entry : entries)
    {
        ASSERT(std::strlen(entry.key) > 0);
        uint32_t idx = calc_index(entry.key, level, size);

        if (collide.test(idx))
            continue;

        if (exists.test(idx))
        {
            collide.set(idx);
            continue;
        }

        exists.set(idx);
    }

    m_bits.emplace_back(size);
    BitSet64 &back = m_bits.back();

    for (auto entry : entries)
    {
        uint32_t idx = calc_index(entry.key, level, size);

        if (collide.test(idx))
            redo.push_back(entry.key);
        else
            back.set(idx);
    }

    while (! redo.empty())
    {
        // TODO!!!
        //size = 2 * redo.size();
        size = 2 * redo.size() * (level + 2);
        size = (size + 63) & ~63;
        exists.reset();
        collide.reset();
        level++;

        if (level > 10)
        {
            Logger::warn("PerfectHash::level = %u, redo.size = %" PRIu64, level, redo.size());
            for (auto key : redo) Logger::warn("key = %s", key);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        for (auto key : redo)
        {
            uint32_t idx = calc_index(key, level, size);

            if (collide.test(idx))
                continue;

            if (exists.test(idx))
            {
                collide.set(idx);
                continue;
            }

            exists.set(idx);
        }

        m_bits.emplace_back(size);
        BitSet64& back = m_bits.back();

        for (auto it = redo.begin(); it != redo.end(); )
        {
            const char *key = *it;
            ASSERT(std::strlen(key) > 0);
            uint32_t idx = calc_index(key, level, size);

            if (collide.test(idx))
                it++;
            else
            {
                back.set(idx);
                it = redo.erase(it);
            }
        }
    }

    calc_ranks();
    fill_buckets(entries);
}

void
PerfectHash::calc_ranks()
{
    uint64_t pop = 0;

    for (BitSet64& b : m_bits)
    {
        m_ranks.emplace_back();
        std::vector<uint64_t>& back = m_ranks.back();

        for (std::size_t i = 0; i < b.capacity64(); i++)
        {
            if ((i % 8) == 0)
                back.push_back(pop);
            pop += b.pop64(i);
        }
    }
}

void
PerfectHash::fill_buckets(std::vector<struct perfect_entry>& entries)
{
    ASSERT(m_buckets != nullptr);

    for (auto entry : entries)
    {
        uint64_t idx = lookup_internal(entry.key, PerfectHash::hash(entry.key));
//printf("fill_buckets: idx = %lu, key = %s\n", idx, key);
        ASSERT((0 < idx) && (idx <= m_count));
        //if (! ((0 < idx) && (idx <= m_count)))
            //printf("idx = %lu, m_count = %u, key = %s\n", idx, m_count, key);
        m_buckets[idx].key = entry.key;
        m_buckets[idx].id = entry.id;
    }
}


SuperMap::SuperMap() :
    m_buff(1048576)
{
}

SuperMap::~SuperMap()
{
    if (m_perfect_hash != nullptr)
        m_perfect_hash.reset();
}

void
SuperMap::init()
{
    m_instance = new SuperMap();

    Task task;
    task.doit = &SuperMap::rotate;
    int freq_sec = Config::get_time(CFG_HASH_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_HASH_ROTATION_FREQUENCY_DEF);
    if (freq_sec < 1) freq_sec = 1;
    Timer::inst()->add_task(task, freq_sec, "hash_rotate");
    Logger::info("Will try to rotate super hash every %d secs.", freq_sec);
}

std::shared_ptr<PerfectHash>
SuperMap::get_perfect_hash()
{
    return std::atomic_load(&m_perfect_hash);
}

void
SuperMap::set_perfect_hash(std::shared_ptr<PerfectHash> ph)
{
    std::atomic_store(&m_perfect_hash, ph);
}

TimeSeriesId
SuperMap::get(const char *key)
{
    uint64_t h = PerfectHash::hash(key);
    return get_internal(key, h);
}

TimeSeriesId
SuperMap::get_internal(const char *key, uint64_t h)
{
    ASSERT(key != nullptr);

    std::shared_ptr<PerfectHash> ph = get_perfect_hash();

    // try PerfectHash first
    if (ph != nullptr)
    {
        TimeSeriesId id = ph->lookup(key, h);
        if (id != TT_INVALID_TIME_SERIES_ID)
        {
            //cache_hit++;
            return id;
        }
    }

    ReadLock guard(m_lock);

    for (auto map : m_maps)
    {
        TimeSeriesId id = map->get(key, h);
        if (id != TT_INVALID_TIME_SERIES_ID)
        {
            //cache_miss++;
            return id;
        }
    }

    return TT_INVALID_TIME_SERIES_ID;
}

TimeSeries *
SuperMap::set(const char *key, TagOwner& owner)
{
    ASSERT(key != nullptr);

    uint64_t h = PerfectHash::hash(key);
    TimeSeriesId id = get_internal(key, h);

    if (id != TT_INVALID_TIME_SERIES_ID)
        return TimeSeries::get_ts(id);

    WriteLock guard(m_lock);

    for (auto map : m_maps)
    {
        id = map->get(key, h);
        if (id != TT_INVALID_TIME_SERIES_ID)
            return TimeSeries::get_ts(id);
    }

    TimeSeries *ts = TimeSeries::create(owner.get_cloned_tags());
    id = ts->get_id();
    MetaFile::instance()->add_entry(key, id);

    key = m_buff.strdup(key);

    for (auto it = m_maps.rbegin(); it != m_maps.rend(); it++)
    {
        auto map = *it;
        if (map->set(key, h, id))
            return ts;
    }

    std::shared_ptr<InMemoryMap> map = InMemoryMap::create();
    map->set(key, h, id);
    m_maps.push_back(map);

    return ts;
}

void
SuperMap::set_raw(const char *key, TimeSeriesId id)
{
    uint64_t h = PerfectHash::hash(key);
    TimeSeriesId id2 = get_internal(key, h);

    if (id2 != TT_INVALID_TIME_SERIES_ID)
    {
        ASSERT(id == id2);
        return;
    }

    WriteLock guard(m_lock);

    for (auto map : m_maps)
    {
        id2 = map->get(key, h);
        if (id2 != TT_INVALID_TIME_SERIES_ID)
        {
            ASSERT(id == id2);
            return;
        }
    }

    MetaFile::instance()->add_entry(key, id);
    key = m_buff.strdup(key);

    for (auto it = m_maps.rbegin(); it != m_maps.rend(); it++)
    {
        auto map = *it;
        if (map->set(key, h, id))
            return;
    }

    auto map = InMemoryMap::create();
    map->set(key, h, id);
    m_maps.push_back(map);
}

void
SuperMap::erase()
{
    WriteLock guard(m_lock);

    for (auto it = m_maps.begin(); it != m_maps.end(); it++)
    {
        if ((*it)->is_read_only())
        {
            m_maps.erase(it);
            break;
        }
    }
}

bool
SuperMap::rotate(TaskData& data)
{
    //Logger::info("cache-hit = %lu, cache-miss = %lu", cache_hit.load(), cache_miss.load());

    if (g_shutdown_requested) return false;
    if (m_instance->m_maps.empty()) return false;

    Logger::info("[hash-rotate] Start");
    std::shared_ptr<InMemoryMap> map = m_instance->m_maps.back();
    if (map->is_empty()) return false;
    if (map->is_read_only()) return false;

    // move what's in map into the perfect hash
    map->set_read_only();
    //map->write_meta_file();

    std::shared_ptr<PerfectHash> old = m_instance->get_perfect_hash();
    std::shared_ptr<PerfectHash> ph = PerfectHash::create(old, map);

    {
        WriteLock guard(m_instance->m_lock);
        m_instance->set_perfect_hash(ph);
    }

    if (old == nullptr)
        m_instance->erase();
    else
        old.reset();

    Logger::info("[hash-rotate] Done");
    return false;
}


}
