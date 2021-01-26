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

#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include "bitset.h"
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
    m_cp_start(0)
{
}

void
BitSet::init(uint8_t *base, size_t capacity_in_bytes)
{
    ASSERT(base != nullptr);

    m_bits = base;
    m_cursor = base;
    m_capacity_in_bytes = capacity_in_bytes;
    m_start = 0;

    m_cp_cursor = nullptr;
    m_cp_start = 0;

    m_end = m_bits + capacity_in_bytes;
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
    ASSERT(base != m_bits);

    m_cursor = m_bits + bytes;
    m_start = start;

    if (base != nullptr)
    {
        if (start != 0) bytes++;
        memcpy(m_bits, base, bytes);
    }
}

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
        if (cursor->m_cursor == m_end)
        {
            throw std::out_of_range("end of bitset reached");
        }

        retrieve(cursor, *bits, len, start);
        if (start == 0) bits++;
    }
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
        }
        else
        {
            cursor->m_start += len;
            len = 0;
        }
    }
}

const char *
BitSet::c_str(char *buff, size_t size) const
{
    std::snprintf(buff, size, "bits=%p cap=%d cur=%p end=%p start=%d",
        m_bits, (int)m_capacity_in_bytes, m_cursor, m_end, (int)m_start);
    return buff;
}


BitSetCursor::BitSetCursor(BitSet *bitset) :
    m_cursor(bitset->m_bits),
    m_start(0)
{
}


}
