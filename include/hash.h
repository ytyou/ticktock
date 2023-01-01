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

#include <deque>
#include <memory>
#include <unordered_map>
#include <vector>
#include "bitset.h"
#include "rw.h"
#include "strbuf.h"
#include "tag.h"
#include "task.h"
#include "ts.h"
#include "utils.h"


namespace tt_test
{
    class HashTests;
}

namespace tt
{


class MetaFile;


struct SuperKey
{
    SuperKey(const char *key, uint64_t hash) :
        m_key(key), m_hash(hash)
    {
    }

    uint64_t m_hash;    // pre-calculated hash value of m_key
    const char *m_key;
};


struct SuperHash
{
    std::size_t operator()(const SuperKey& key) const
    {
        return key.m_hash;
    }
};


struct SuperEqual
{
    bool operator()(const SuperKey& lhs, const SuperKey& rhs) const
    {
        ASSERT(lhs.m_hash == rhs.m_hash);
        return (std::strcmp(lhs.m_key, rhs.m_key) == 0);
    }
};


class InMemoryMap : public std::enable_shared_from_this<InMemoryMap>
{
public:
    [[nodiscard]] static std::shared_ptr<InMemoryMap> create();
    ~InMemoryMap();

    TimeSeriesId get(const char *key, uint64_t hash);
    bool set(const char *key, uint64_t hash, TimeSeriesId id);
    std::size_t size() const { return m_map.size(); }
    inline bool is_read_only() const { return m_read_only; }
    void set_read_only();
    inline bool is_empty() const { return m_map.empty(); }
    void collect(std::vector<struct perfect_entry>& entries);

private:
    friend class tt_test::HashTests;

    InMemoryMap();

    bool m_read_only;
    std::unordered_map<SuperKey, TimeSeriesId, SuperHash, SuperEqual> m_map;
    default_contention_free_shared_mutex m_lock;

    //StringBuffer m_buff;    // for all the keys in this map
};

struct __attribute__ ((__packed__)) perfect_entry
{
    perfect_entry(const char *k, TimeSeriesId i) : key(k), id(i) { }
    perfect_entry(struct perfect_entry& entry)
    {
        key = entry.key;
        id = entry.id;
    }
    perfect_entry(struct perfect_entry&& entry)
    {
        key = entry.key;
        id = entry.id;
    }

    const char *key;
    TimeSeriesId id;
};

// See https://github.com/dgryski/go-boomphf
class PerfectHash : public std::enable_shared_from_this<PerfectHash>
{
public:
    // count: number of keys in the file
    [[nodiscard]] static std::shared_ptr<PerfectHash>
        create(std::shared_ptr<PerfectHash> ph, std::shared_ptr<InMemoryMap> map);
    [[nodiscard]] static std::shared_ptr<PerfectHash> create(MetaFile *meta_file);
    ~PerfectHash();

    static uint64_t hash(const char *s);

    // hash: pre-calculated hash of the 'key'
    TimeSeriesId lookup(const char *key, uint64_t hash);

private:
    friend class tt_test::HashTests;

    static uint64_t hash(uint64_t x);
    static uint32_t rotl(uint32_t x, uint32_t y);
    static uint32_t calc_index(const char *key, uint32_t level, uint32_t size);

    // count: number of keys in the file
    PerfectHash(std::vector<struct perfect_entry>& entries);
    void construct(std::vector<struct perfect_entry>& entries);
    void calc_ranks();
    void fill_buckets(std::vector<struct perfect_entry>& entries);
    uint64_t lookup_internal(const char *key, uint64_t hash);

    std::vector<BitSet64> m_bits;
    std::vector<std::vector<uint64_t>> m_ranks;

    uint32_t m_count;   // number of keys
    struct perfect_entry *m_buckets;    // array of 'm_count+1' in size
};


// Singleton
class SuperMap
{
public:
    SuperMap();
    ~SuperMap();
    static SuperMap *instance() { return m_instance; }

    static void init();

    TimeSeriesId get(const char *key);

    // get() and then set()
    TimeSeries *set(const char *key, TagOwner& owner);
    void set_raw(const char *key, TimeSeriesId id);

    void erase();

private:
    std::shared_ptr<PerfectHash> get_perfect_hash();
    void set_perfect_hash(std::shared_ptr<PerfectHash> ph);
    TimeSeriesId get_internal(const char *key, uint64_t h);

    static bool rotate(TaskData& data);

    default_contention_free_shared_mutex m_lock;
    std::shared_ptr<PerfectHash> m_perfect_hash;
    std::deque<std::shared_ptr<InMemoryMap>> m_maps;
    HashBuffer m_buff;

    static SuperMap *m_instance;
};


}
