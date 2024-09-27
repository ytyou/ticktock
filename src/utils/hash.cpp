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

#include <cstdio>
#include <stdio.h>
#include <cstring>
#include "hash.h"
#include "utils.h"


namespace tt
{


PerfectHash::PerfectHash(const char *keys, uint32_t count) :
    m_keys(keys),
    m_count(count)
{
    construct(keys, count);
}

uint64_t
PerfectHash::lookup(const char *key)
{
    ASSERT(key != nullptr);
    uint64_t h = hash(key);
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
PerfectHash::construct(const char *keys, uint32_t count)
{
    ASSERT(keys != nullptr);
    ASSERT(count > 0);

    uint32_t level = 0;
    uint32_t size = 2 * count;
    size = (size + 63) & ~63;
    ASSERT((size % 64) == 0);

    BitSet64 exists(size);
    BitSet64 collide(size);
    std::vector<const char*> redo;
    uint32_t cnt;
    const char *key = keys;

    for (cnt = 0; cnt < count; cnt++, key = std::strchr(key, '\0')+1)
    {
        ASSERT(std::strlen(key) > 0);
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
    BitSet64 &back = m_bits.back();

    for (cnt = 0, key = keys; cnt < count; cnt++, key = std::strchr(key, '\0')+1)
    {
        uint32_t idx = calc_index(key, level, size);

        if (collide.test(idx))
            redo.push_back(key);
        else
            back.set(idx);
    }

    while (! redo.empty())
    {
        size = 2 * redo.size();
        size = (size + 63) & ~63;
        exists.reset();
        collide.reset();
        level++;

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
            key = *it;
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


}
