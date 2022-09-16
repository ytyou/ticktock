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

#include <algorithm>
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
    m_cp_start(0),
    m_buffer(nullptr),
    m_bound(nullptr)
{
}

void
BitSet::init(uint8_t *base, size_t capacity_in_bytes, size_t buff_size)
{
    ASSERT(base != nullptr);

    m_bits = base;
    m_capacity_in_bytes = capacity_in_bytes;

    if (buff_size == 0)
    {
        m_buffer = nullptr;
        m_cursor = base;
        m_end = m_bound = m_bits + capacity_in_bytes;
    }
    else
    {
        m_buffer = (uint8_t*)malloc(buff_size);
        m_bound = base;
        m_cursor = m_buffer;
        m_end = m_buffer + buff_size;
    }

    m_start = 0;
    m_cp_cursor = nullptr;
    m_cp_start = 0;
}

void
BitSet::recycle()
{
    m_cursor = m_bits;
    m_start = 0;
    m_cp_cursor = nullptr;
    m_cp_start = 0;

    if (m_buffer != nullptr)
    {
        free(m_buffer);
        m_buffer = nullptr;
        m_bound = nullptr;
    }
}

void
BitSet::rebase(uint8_t *base)
{
    ASSERT(base != nullptr);
    ASSERT(base != m_bits);

    if (m_bits != nullptr)
    {
        if (m_buffer == nullptr)
        {
            if (m_cursor != nullptr) m_cursor = base + (m_cursor - m_bits);
            if (m_cp_cursor != nullptr) m_cp_cursor = base + (m_cp_cursor - m_bits);
            if (m_end != nullptr) m_end = m_bound = base + (m_end - m_bits);
        }
        else if (m_bound != nullptr)
        {
            m_bound = base + (m_bound - m_bits);
        }
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
            flush();

        if (m_cursor == m_end)
            throw std::out_of_range("bitset is full");

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
BitSet::flush()
{
    if (m_buffer != nullptr)
    {
        // copy
        size_t size = m_cursor - m_buffer;
        size_t size1 = (m_start == 0) ? size : (size + 1);
        memcpy(m_bound, m_buffer, size1);
        m_bound += size;

        // re-initialize m_buffer
        size_t left = m_capacity_in_bytes - (m_bound - m_bits);
        m_end = m_buffer + std::min(size1, left);
        m_cursor = m_buffer;
        if (m_start != 0) m_buffer[0] = *m_bound;
    }
}

void
BitSet::copy_to(uint8_t *base)
{
    ASSERT(base != nullptr);
    flush();
    if (base != m_bits)
        memcpy(base, m_bits, size_in_bytes());
}

void
BitSet::copy_from(uint8_t *base, int bytes, uint8_t start)
{
    ASSERT(bytes > 0);
    ASSERT(base != m_bits);

    int bytes1 = (start == 0) ? bytes : (bytes + 1);
    m_start = start;

    if ((base != nullptr) && (base != m_bits))
        memcpy(m_bits, base, bytes1);

    if (m_buffer != nullptr)
    {
        m_bound = m_bits + bytes;
        m_cursor = m_buffer;

        if (start != 0)
            m_buffer[0] = *m_bound;
    }
    else
    {
        m_cursor = m_bits + bytes;
    }
}

bool
BitSet::end_reached(BitSetCursor *cursor) const
{
    if (m_buffer != nullptr)
    {
        if (cursor->m_in_buffer)
        {
            return ((cursor->m_cursor > m_cursor) ||
                ((cursor->m_cursor == m_cursor) && (cursor->m_start >= m_start)));
        }
        else
        {
            if (cursor->m_cursor >= m_bound)
            {
                // switch to m_buffer
                ASSERT(cursor->m_cursor == m_bound);
                cursor->m_cursor = m_buffer;
                cursor->m_in_buffer = true;
                return ((cursor->m_cursor > m_cursor) ||
                    ((cursor->m_cursor == m_cursor) && (cursor->m_start >= m_start)));
            }
            else
                return false;
        }
    }
    else
    {
        return ((cursor->m_cursor > m_cursor) ||
            ((cursor->m_cursor == m_cursor) && (cursor->m_start >= m_start)));
    }
}

void
BitSet::retrieve(BitSetCursor *cursor, uint8_t *bits, uint8_t len, uint8_t start)
{
    ASSERT(cursor != nullptr);
    ASSERT(bits != nullptr);
    ASSERT(len > 0);
    ASSERT(cursor->m_start < 8);

    if (end_reached(cursor))
        throw std::out_of_range("end of bitset reached");

    while (start >= 8)
    {
        bits++;
        start -= 8;
    }

    while (len > 0)
    {
        if (end_reached(cursor))
            throw std::out_of_range("end of bitset reached");

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
BitSet::c_str(char *buff) const
{
    std::snprintf(buff, c_size(), "bits=%p cap=%d cur=%p end=%p start=%d",
        m_bits, (int)m_capacity_in_bytes, m_cursor, m_end, (int)m_start);
    return buff;
}


BitSetCursor::BitSetCursor(BitSet *bitset) :
    m_cursor(bitset->m_bits),
    m_start(0),
    m_in_buffer(false)
{
}


}
