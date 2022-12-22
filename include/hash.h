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

#include <vector>
#include "bitset.h"


namespace tt
{


// See https://github.com/dgryski/go-boomphf
class PerfectHash
{
public:
    // count: number of keys in the file
    PerfectHash(const char *keys, uint32_t count);

    uint64_t lookup(const char *key);

private:
    static uint64_t hash(uint64_t x);
    static uint64_t hash(const char *s);
    static uint32_t rotl(uint32_t x, uint32_t y);
    static uint32_t calc_index(const char *key, uint32_t level, uint32_t size);

    // count: number of keys in the file
    void construct(const char *keys, uint32_t count);
    void calc_ranks();

    std::vector<BitSet64> m_bits;
    std::vector<std::vector<uint64_t>> m_ranks;
    const char *m_keys;
    uint32_t m_count;   // number of keys
};


}
