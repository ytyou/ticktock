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

#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include "bitset.h"
#include "memmgr.h"
#include "utils.h"


namespace tt
{


static uint8_t mask_0011[8] = { 0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01 };
static uint8_t mask_1100[8] = { 0x00, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE };


BitSet::BitSet() :
    m_bits(nullptr),
    m_capacity_in_bytes(0),
    m_cursor(nullptr),
    m_end(nullptr),
    m_start(0),
    m_cp_cursor(nullptr),
    m_cp_start(0),
    m_own_memory(false)
{
}

BitSet::BitSet(size_t size_in_bits) :
    BitSet()
{
    size_t size_in_bytes = (size_in_bits + 7) / 8;
    uint8_t *base = (uint8_t*) std::malloc(size_in_bytes);
    std::memset(base, 0, size_in_bytes);
    m_own_memory = true;
    init(base, size_in_bytes);
}

BitSet::~BitSet()
{
    if (m_own_memory && (m_bits != nullptr))
        std::free(m_bits);
}

void
BitSet::init(uint8_t *base, size_t capacity_in_bytes, bool full)
{
    ASSERT(base != nullptr);

    m_bits = base;
    m_capacity_in_bytes = capacity_in_bytes;
    m_start = 0;

    m_cp_cursor = nullptr;
    m_cp_start = 0;

    m_end = m_bits + capacity_in_bytes;
    m_cursor = full ? m_end : base;
}

size_t
BitSet::capacity_in_bytes() const
{
    if ((m_bits == nullptr) || (m_end == nullptr))
        return 0;
    else
    {
        ASSERT(m_bits <= m_end);
        ASSERT(m_cursor <= m_end);
        return m_end - m_bits;
    }
}

void
BitSet::recycle()
{
    m_cursor = m_bits;
    m_start = 0;
    m_cp_cursor = nullptr;
    m_cp_start = 0;
}

void
BitSet::reset()
{
    recycle();
    size_t size_in_bytes = capacity_in_bytes();
    if ((size_in_bytes > 0) && (m_bits != nullptr))
        std::memset(m_bits, 0, size_in_bytes);
    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
}

void
BitSet::rebase(uint8_t *base)
{
    ASSERT(base != nullptr);
    ASSERT(base != m_bits);

    if (m_bits != nullptr)
    {
        if (m_cursor != nullptr) m_cursor = base + (m_cursor - m_bits);
        if (m_cp_cursor != nullptr) m_cp_cursor = base + (m_cp_cursor - m_bits);
        if (m_end != nullptr) m_end = base + (m_end - m_bits);
    }

    m_bits = base;
    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
}

BitSetCursor *
BitSet::new_cursor()
{
    BitSetCursor *cursor =
        (BitSetCursor*)MemoryManager::alloc_recyclable(RecyclableType::RT_BITSET_CURSOR);
    cursor->init(this);
    return cursor;
}

void
BitSet::set(size_t idx)
{
    if (m_bits == nullptr)
        throw std::out_of_range("bitset not initialized yet");
    size_t idx_byte = idx / 8;
    unsigned int off = idx % 8;
    uint8_t *cursor = m_bits + idx_byte;
    if (cursor > m_end)
        throw std::out_of_range("index out of range");
    *cursor |= 1 << (7 - off);
}

bool
BitSet::test(size_t idx)
{
    if (m_bits == nullptr)
        throw std::out_of_range("bitset not initialized yet");
    size_t idx_byte = idx / 8;
    unsigned int off = idx % 8;
    uint8_t *cursor = m_bits + idx_byte;
    if (cursor > m_end)
        throw std::out_of_range("index out of range");
    return ((*cursor) & 1 << (7 - off)) != 0;
}

void
BitSet::append(uint8_t *bits, uint8_t len, uint8_t start)
{
    ASSERT(m_start < 8);
    ASSERT(bits != nullptr);
    ASSERT(len > 0);

    while (start >= 8)
    {
        bits++;
        start -= 8;
    }

    while (len > 0)
    {
        if (m_cursor == m_end)
        {
            throw std::out_of_range("bitset is full");
        }

        append(*bits, len, start);
        if (start == 0) bits++;
    }

    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
}

void
BitSet::append(uint8_t bits, uint8_t& len, uint8_t& start)
{
    if (start < m_start)
    {
        // we have more bits to copy than the current byte can take
        bits >>= (m_start - start);

        *m_cursor &= mask_1100[m_start];
        bits &= mask_0011[m_start];
        *m_cursor |= bits;

        uint8_t l = 8 - m_start;

        if (len >= l)
        {
            start += l;
            len -= l;
            m_start = 0;
            m_cursor++;
        }
        else
        {
            m_start += len;
            len = 0;
        }
    }
    else if (m_start < start)
    {
        // we have more space than what we need to copy
        bits <<= (start - m_start);

        *m_cursor &= mask_1100[m_start];
        bits &= mask_0011[m_start];
        *m_cursor |= bits;

        uint8_t l = 8 - start;

        if (len >= l)
        {
            m_start += l;
            len -= l;
            start = 0;
        }
        else
        {
            m_start += len;
            len = 0;
        }
    }
    else // start == m_start
    {
        *m_cursor &= mask_1100[m_start];
        bits &= mask_0011[m_start];
        *m_cursor |= bits;

        uint8_t l = 8 - m_start;

        if (len >= l)
        {
            len -= l;
            start = 0;
            m_start = 0;
            m_cursor++;
        }
        else
        {
            m_start += len;
            len = 0;
        }
    }

    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
}

int
BitSet::append(FILE *file)
{
    ASSERT(file != nullptr);
    return fwrite(m_bits, 1, size_in_bytes(), file);
}

void
BitSet::copy_to(uint8_t *base) const
{
    ASSERT(base != nullptr);
    if (base != m_bits)
        memcpy(base, m_bits, size_in_bytes());
}

void
BitSet::copy_from(uint8_t *base, int bytes, uint8_t start)
{
    ASSERT(bytes > 0);
    //ASSERT(base != m_bits);

    m_cursor = m_bits + bytes;
    m_start = start;

    if ((base != nullptr) && (base != m_bits))
    {
        if (start != 0) bytes++;
        memcpy(m_bits, base, bytes);
    }
}

/* @param bits  buffer where retrieved bits will be returned;
 * @param len   number of bits to retrieve;
 * @param start starting position in 'bits' to store retrieved bits;
 */
void
BitSet::retrieve(BitSetCursor *cursor, uint8_t *bits, uint8_t len, uint8_t start)
{
    ASSERT(cursor != nullptr);
    ASSERT(bits != nullptr);
    ASSERT(len > 0);
    ASSERT(m_bits <= cursor->m_cursor);
    ASSERT(cursor->m_cursor <= m_end);
    ASSERT(cursor->m_start < 8);

    if ((cursor->m_cursor > m_cursor) ||
        ((cursor->m_cursor == m_cursor) && (cursor->m_start >= m_start)))
    {
        throw std::out_of_range("end of bitset reached");
    }

    while (start >= 8)
    {
        bits++;
        start -= 8;
    }

    while (len > 0)
    {
        ASSERT(m_cursor <= m_end);

        if (bits_left(cursor) < len)
            throw std::out_of_range("end of bitset reached");

        ASSERT(cursor->m_cursor <= m_cursor);
        retrieve(cursor, *bits, len, start);
        if (start == 0) bits++;
        ASSERT(cursor->m_cursor <= m_cursor);
    }

    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
    ASSERT(cursor->m_cursor <= m_cursor);
}

void
BitSet::retrieve(BitSetCursor *cursor, uint8_t& bits, uint8_t& len, uint8_t& start)
{
    uint8_t src = *cursor->m_cursor;

    if (start < cursor->m_start)
    {
        // we have more space than what we need to copy
        src <<= (cursor->m_start - start);

        bits &= mask_1100[start];
        src &= mask_0011[start];
        bits |= src;

        uint8_t l = 8 - cursor->m_start;

        if (len >= l)
        {
            start += l;
            len -= l;
            cursor->m_start = 0;
            cursor->m_cursor++;
            ASSERT(cursor->m_cursor <= m_cursor);
        }
        else
        {
            cursor->m_start += len;
            len = 0;
        }
    }
    else if (cursor->m_start < start)
    {
        // we have more bits to copy than the current byte can take
        src >>= (start - cursor->m_start);

        bits &= mask_1100[start];
        src &= mask_0011[start];
        bits |= src;

        uint8_t l = 8 - start;

        if (len >= l)
        {
            cursor->m_start += l;
            len -= l;
            start = 0;
        }
        else
        {
            cursor->m_start += len;
            len = 0;
        }
    }
    else // start == cursor->m_start
    {
        bits &= mask_1100[start];
        src &= mask_0011[start];
        bits |= src;

        uint8_t l = 8 - start;

        if (len >= l)
        {
            len -= l;
            start = 0;
            cursor->m_start = 0;
            cursor->m_cursor++;
            ASSERT(cursor->m_cursor <= m_cursor);
        }
        else
        {
            cursor->m_start += len;
            len = 0;
        }
    }

    ASSERT(m_bits <= m_end);
    ASSERT(m_bits <= m_cursor);
    ASSERT(m_cursor <= m_end);
}

/*
const char *
BitSet::c_str(char *buff) const
{
    std::snprintf(buff, c_size(), "bits=%p cap=%d cur=%p end=%p start=%d",
        m_bits, (int)m_capacity_in_bytes, m_cursor, m_end, (int)m_start);
    return buff;
}
*/


void
BitSetCursor::init()
{
    m_cursor = nullptr;
    m_start = 0;
}

void
BitSetCursor::init(BitSet *bitset)
{
    m_cursor = bitset->m_bits;
    m_start = 0;
}

void
BitSetCursor::ignore_rest_of_byte()
{
    if (m_start != 0)
    {
        m_cursor++;
        m_start = 0;
    }
}


BitSet64::BitSet64(std::size_t size)
{
    m_capacity = (size + 63) / 64;
    m_bits = (uint64_t*) calloc(size, sizeof(uint64_t));
}

BitSet64::BitSet64(BitSet64&& src) :
    m_bits(src.m_bits),
    m_capacity(src.m_capacity)
{
    src.m_bits = nullptr;
}

BitSet64::~BitSet64()
{
    if (m_bits != nullptr)
        std::free(m_bits);
}

uint64_t
BitSet64::get64(std::size_t idx)
{
    ASSERT(idx < m_capacity);
    return m_bits[idx];
}

uint64_t
BitSet64::pop64(std::size_t idx)
{
    ASSERT(idx < m_capacity);
    return __builtin_popcountll(m_bits[idx]);
}

void
BitSet64::reset()
{
    std::memset((void*)m_bits, 0, m_capacity*8);
}


}
