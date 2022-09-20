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
#include <cstdlib>
#include <cmath>
#include <endian.h>
#include <limits.h>
#include <inttypes.h>
#include <stdexcept>
#include "compress.h"
#include "logger.h"
#include "memmgr.h"
#include "utils.h"


namespace tt
{


static uint64_t TRAILING_ZEROS[64] =
{
    0xFFFFFFFFFFFFFFFF, 0xFEFFFFFFFFFFFFFF, 0xFCFFFFFFFFFFFFFF, 0xF8FFFFFFFFFFFFFF,
    0xF0FFFFFFFFFFFFFF, 0xE0FFFFFFFFFFFFFF, 0xC0FFFFFFFFFFFFFF, 0x80FFFFFFFFFFFFFF,
    0x00FFFFFFFFFFFFFF, 0x00FEFFFFFFFFFFFF, 0x00FCFFFFFFFFFFFF, 0x00F8FFFFFFFFFFFF,
    0x00F0FFFFFFFFFFFF, 0x00E0FFFFFFFFFFFF, 0x00C0FFFFFFFFFFFF, 0x0080FFFFFFFFFFFF,
    0x0000FFFFFFFFFFFF, 0x0000FEFFFFFFFFFF, 0x0000FCFFFFFFFFFF, 0x0000F8FFFFFFFFFF,
    0x0000F0FFFFFFFFFF, 0x0000E0FFFFFFFFFF, 0x0000C0FFFFFFFFFF, 0x000080FFFFFFFFFF,
    0x000000FFFFFFFFFF, 0x000000FEFFFFFFFF, 0x000000FCFFFFFFFF, 0x000000F8FFFFFFFF,
    0x000000F0FFFFFFFF, 0x000000E0FFFFFFFF, 0x000000C0FFFFFFFF, 0x00000080FFFFFFFF,
    0x00000000FFFFFFFF, 0x00000000FEFFFFFF, 0x00000000FCFFFFFF, 0x00000000F8FFFFFF,
    0x00000000F0FFFFFF, 0x00000000E0FFFFFF, 0x00000000C0FFFFFF, 0x0000000080FFFFFF,
    0x0000000000FFFFFF, 0x0000000000FEFFFF, 0x0000000000FCFFFF, 0x0000000000F8FFFF,
    0x0000000000F0FFFF, 0x0000000000E0FFFF, 0x0000000000C0FFFF, 0x000000000080FFFF,
    0x000000000000FFFF, 0x000000000000FEFF, 0x000000000000FCFF, 0x000000000000F8FF,
    0x000000000000F0FF, 0x000000000000E0FF, 0x000000000000C0FF, 0x00000000000080FF,
    0x00000000000000FF, 0x00000000000000FE, 0x00000000000000FC, 0x00000000000000F8,
    0x00000000000000F0, 0x00000000000000E0, 0x00000000000000C0, 0x0000000000000080
};


Compressor::Compressor()
{
    get_start_tstamp() = 0L;
}

void
Compressor::init(Timestamp start, uint8_t *base, size_t size)
{
    get_start_tstamp() = start;
}

Compressor *
Compressor::create(int version)
{
    Compressor *compressor = nullptr;

    switch (version)
    {
        case 0:
            compressor = new Compressor_v0();
            break;

        case 1:
            compressor = new Compressor_v1();
            break;

        case 2:
            compressor = new Compressor_v2();
            break;

        default:
            Logger::warn("Unknown compressor version %d", version);
            break;
    }

    return compressor;
}

Timestamp &
Compressor::get_start_tstamp()
{
#ifdef __x86_64__
    return (Timestamp&)Recyclable::next();
#else
    return m_start_tstamp;
#endif
}

Timestamp
Compressor::get_start_tstamp_const() const
{
#ifdef __x86_64__
    return (Timestamp)Recyclable::next_const();
#else
    return m_start_tstamp;
#endif
}


// Implementation of Gorilla compression algorithm.
Compressor_v2::Compressor_v2()
{
    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = 0L;
    m_prev_value = 0.0;
    m_prev_leading_zeros = 65;
    m_prev_trailing_zeros = 65;
    m_prev_none_zeros = 64;
    m_is_full = false;

    ASSERT(m_start_tstamp <= m_prev_tstamp);
}

void
Compressor_v2::init(Timestamp start, uint8_t *base, size_t size)
{
    ASSERT(base != nullptr);

    Compressor::init(start, base, size);

    m_bitset.init(base, size);

    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = start;
    m_prev_value = 0.0;
    m_prev_leading_zeros = 65;
    m_prev_trailing_zeros = 65;
    m_prev_none_zeros = 64;
    m_is_full = false;

    ASSERT(m_start_tstamp <= m_prev_tstamp);
}

void
Compressor_v2::restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base)
{
    Logger::debug("cv2: restoring from position: offset=%d, start=%d",
        position.m_offset, position.m_start);

    m_bitset.copy_from(base, position.m_offset, position.m_start);
    uncompress(dps, true);

    Logger::debug("cv2: restored %d data-points", m_dp_count);
}

void
Compressor_v2::save(CompressorPosition& position)
{
    size_t bit_cnt = m_bitset.size_in_bits();

    position.m_offset = bit_cnt / 8;
    position.m_start = bit_cnt % 8;

    Logger::debug("cv2: saved position: offset=%d, start=%d, #dp=%d",
        position.m_offset, position.m_start, m_dp_count);
}

void
Compressor_v2::compress1(Timestamp timestamp, double value)
{
    ASSERT(m_dp_count == 0);
    ASSERT(m_start_tstamp <= timestamp);
    ASSERT((timestamp - m_start_tstamp) <= INT_MAX);

    uint32_t delta = timestamp - get_start_tstamp();

    // TODO: make these lengths constants
    m_bitset.append(reinterpret_cast<uint8_t*>(&delta), 8*sizeof(uint32_t), 0);
    m_bitset.append(reinterpret_cast<uint8_t*>(&value), 8*sizeof(double), 0);

    m_prev_tstamp = timestamp;
    m_prev_value = value;
    m_prev_delta = delta;
    m_dp_count++;
}

bool
Compressor_v2::compress(Timestamp timestamp, double value)
{
    ASSERT(m_start_tstamp <= timestamp);

    m_bitset.save_check_point();

    try
    {
        if (m_dp_count == 0)
        {
            compress1(timestamp, value);
            return true;
        }

        if (m_prev_tstamp > timestamp)
        {
            Logger::debug("out-of-order dp dropped, timestamp = %" PRIu64, timestamp);
            return true;    // drop it
        }

        ASSERT(m_dp_count > 0);

        // Timestamp first
        Timestamp delta = timestamp - m_prev_tstamp;
        uint64_t delta_of_delta = (uint64_t)delta - (uint64_t)m_prev_delta;

        if (delta_of_delta == 0)
        {
            // store a single '0' bit
            uint8_t zero = 0x00;
            m_bitset.append(reinterpret_cast<uint8_t*>(&zero), 1, 0);
        }
        else if ((-8192 <= (int64_t)delta_of_delta) && ((int64_t)delta_of_delta <= 8191))
        {
            // store '10' followed by value in 14 bits
            uint8_t one_zero = 0x80;
            m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
            uint16_t dod_be = htobe16((uint16_t)delta_of_delta);
            m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 14, 16-14);
        }
        else if ((-65536 <= (int64_t)delta_of_delta) && ((int64_t)delta_of_delta <= 65535))
        {
            // store '110' followed by value in 17 bits
            uint8_t one_one_zero = 0xC0;
            m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_zero), 3, 0);
            uint32_t dod_be = htobe32((uint32_t)delta_of_delta);
            m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);
        }
        else
        {
            // store '111' followed by value in 33 bits
            uint8_t one_one_one = 0xE0;
            m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_one), 3, 0);
            uint64_t dod_be = htobe64((uint64_t)delta_of_delta);
            m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 33, 64-33);
        }

        m_prev_tstamp = timestamp;
        m_prev_delta = delta;

        // Value next
        uint64_t prev = htobe64(*reinterpret_cast<uint64_t*>(&m_prev_value));
        uint64_t curr = htobe64(*reinterpret_cast<uint64_t*>(&value));
        uint64_t x = curr xor prev;

        if (x == 0)
        {
            // store a single '0' bit
            uint8_t zero = 0x00;
            m_bitset.append(reinterpret_cast<uint8_t*>(&zero), 1, 0);
        }
        else
        {
            uint32_t l = reinterpret_cast<uint32_t*>(&x)[0] | 0x01000000;
            uint32_t t = reinterpret_cast<uint32_t*>(&x)[1];

            l = htobe32(l);
            t = htobe32(t);

            uint8_t leading_zeros = __builtin_clz(l);
            uint8_t trailing_zeros = (t == 0) ? 32 : __builtin_ctz(t);

            if ((m_prev_leading_zeros > 0) &&
                (m_prev_leading_zeros <= leading_zeros) && (m_prev_trailing_zeros <= trailing_zeros))
            {
                uint8_t one_zero = 0x80;
                m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
                m_bitset.append(reinterpret_cast<uint8_t*>(&x), m_prev_none_zeros, m_prev_leading_zeros);
            }
            else
            {
                uint8_t one_one = 0xC0;
                m_bitset.append(reinterpret_cast<uint8_t*>(&one_one), 2, 0);
                m_bitset.append(reinterpret_cast<uint8_t*>(&leading_zeros), 5, 8-5);
                uint8_t none_zeros = 64 - leading_zeros - trailing_zeros;
                m_bitset.append(reinterpret_cast<uint8_t*>(&none_zeros), 6, 8-6);
                m_bitset.append(reinterpret_cast<uint8_t*>(&x), none_zeros, leading_zeros);

                m_prev_leading_zeros = leading_zeros;
                m_prev_trailing_zeros = trailing_zeros;
                m_prev_none_zeros = none_zeros;
            }
        }

        m_dp_count++;
        m_prev_value = value;
    }
    catch (const std::out_of_range& ex)
    {
        m_bitset.restore_from_check_point();
        m_is_full = true;
        return false;
    }

    return true;
}

void
Compressor_v2::uncompress(DataPointVector& dps, bool restore)
{
    Timestamp timestamp;
    double value = 0.0;

    uint64_t value_be;

    uint8_t leading_zeros = 0;
    uint8_t trailing_zeros = 0;
    uint8_t none_zeros = 0;

    BitSetCursor *cursor = m_bitset.new_cursor();

    // TODO: check for m_bitset being empty
    // 1st data-point
    uint32_t delta32 = 0;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&delta32), 32, 0);
    timestamp = get_start_tstamp() + delta32;
    uint64_t delta = delta32;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&value), 8*sizeof(double), 0);
    ASSERT(m_start_tstamp <= timestamp);
    dps.emplace_back(timestamp, value);

    value_be = htobe64(*reinterpret_cast<uint64_t*>(&value));

    uint64_t y = *reinterpret_cast<uint64_t*>(&value);

    while (true)
    {
        // timestamp
        uint8_t byte = 0;
        try
        {
            m_bitset.retrieve(cursor, &byte, 1, 0);
        }
        catch (const std::out_of_range& ex)
        {
            break;  // reached the end of the bit-stream
        }

        if ((byte & 0x80) == 0)
        {
            ASSERT(delta >= 0);
            timestamp += delta;
        }
        else
        {
            uint64_t delta_of_delta = 0;

            m_bitset.retrieve(cursor, &byte, 1, 0);

            if ((byte & 0x80) == 0)
            {
                // 14-bit delta-of-delta
                uint16_t dod_be;
                m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 14, 16-14);

                if ((*reinterpret_cast<uint16_t*>(&dod_be) & 0x0020) != 0)
                {
                    dod_be |= 0x00C0;
                }
                else
                {
                    dod_be &= 0xFF1F;
                }

                delta_of_delta = (int16_t)htobe16(dod_be);
            }
            else
            {
                m_bitset.retrieve(cursor, &byte, 1, 0);

                if ((byte & 0x80) == 0)
                {
                    // 17-bit delta-of-delta
                    uint32_t dod_be;
                    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);

                    if ((*reinterpret_cast<uint32_t*>(&dod_be) & 0x00000100) != 0)
                    {
                        dod_be |= 0x0000FEFF;
                    }
                    else
                    {
                        dod_be &= 0xFFFF0000;
                    }

                    delta_of_delta = (int32_t)htobe32(dod_be);
                }
                else
                {
                    // 33-bit delta-of-delta
                    uint64_t dod_be;
                    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 33, 64-33);

                    if ((*reinterpret_cast<uint64_t*>(&dod_be) & 0x0000000001000000) != 0)
                    {
                        dod_be |= 0x00000000FEFFFFFF;
                    }
                    else
                    {
                        dod_be &= 0xFFFFFFFF00000000;
                    }

                    delta_of_delta = (uint64_t)htobe64(dod_be);
                }
            }

            //Logger::info("U delta_of_delta = %ld", delta_of_delta);
            delta += delta_of_delta;
            ASSERT(delta >= 0);
            timestamp += delta;
        }

        // value
        m_bitset.retrieve(cursor, &byte, 1, 0);

        if ((byte & 0x80) != 0)  // if byte == 0, value is same as previous
        {
            m_bitset.retrieve(cursor, &byte, 1, 0);

            if ((byte & 0x80) != 0)
            {
                m_bitset.retrieve(cursor, &leading_zeros, 5, 8-5);
                leading_zeros &= 0x1F;
                m_bitset.retrieve(cursor, &none_zeros, 6, 8-6);
                none_zeros &= 0x3F;
                if (none_zeros == 0) none_zeros = 64;
            }
            else
            {
                ASSERT(none_zeros != 0);
            }

            uint64_t x = 0L;
            m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&x), none_zeros, leading_zeros);

            trailing_zeros = 64 - (none_zeros + leading_zeros);
            x &= TRAILING_ZEROS[trailing_zeros];

            value_be = value_be xor x;
            *reinterpret_cast<uint64_t*>(&value) = htobe64(value_be);
        }

        dps.emplace_back(timestamp, value);
    }

    MemoryManager::free_recyclable(cursor);

    if (restore)
    {
        m_dp_count = dps.size();
        m_prev_delta = delta;
        m_prev_tstamp = timestamp;
        m_prev_value = value;
        m_prev_leading_zeros = leading_zeros;
        m_prev_trailing_zeros = trailing_zeros;
        m_prev_none_zeros = none_zeros;
    }

    ASSERT(m_start_tstamp <= m_prev_tstamp);
}

bool
Compressor_v2::recycle()
{
    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = get_start_tstamp();
    m_prev_value = 0.0;
    m_prev_leading_zeros = 65;
    m_prev_trailing_zeros = 65;
    m_prev_none_zeros = 64;
    m_is_full = false;

    m_bitset.recycle();
    return Compressor::recycle();
}



// Compressor_v1: A simple compressor with a compression ratio of 2:1

Compressor_v1::Compressor_v1() :
    m_base(nullptr),
    m_cursor(nullptr),
    m_size(0),
    m_dp_count(0),
    m_prev_delta(0L),
    m_prev_tstamp(0L),
    m_prev_value(0.0),
    m_is_full(false)
{
}

void
Compressor_v1::init(Timestamp start, uint8_t *base, size_t size)
{
    Compressor::init(start, base, size);

    m_base = base;
    m_cursor = base;
    m_size = size;
    m_is_full = false;

    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = start;
    m_prev_value = 0.0;
}

void
Compressor_v1::restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base)
{
    ASSERT(dps.empty());
    ASSERT(m_dp_count == 0);
    //ASSERT(m_start_tstamp == position.m_start_tstamp);

    //m_start_tstamp = position.m_start_tstamp;
    ASSERT(position.m_start == 0);  // we don't use it, but it should be 0
    m_cursor = m_base + position.m_offset;

    if (base != nullptr)
    {
        memcpy(m_base, base, position.m_offset);
    }

    uncompress(dps, true);

    ASSERT((position.m_offset == 0) || (m_dp_count != 0));
    ASSERT(m_dp_count == dps.size());
}

void
Compressor_v1::rebase(uint8_t *base)
{
    if ((m_cursor != nullptr) && (m_base != nullptr))
    {
        ASSERT(m_cursor >= m_base);
        m_cursor = base + (m_cursor - m_base);
    }

    m_base = base;
}

void
Compressor_v1::compress1(
    Timestamp timestamp,        // t_1: tstamp of the new dp
    double value)               // v_1: value of the new dp
{
    ASSERT(m_dp_count == 0);
    ASSERT(m_start_tstamp <= timestamp);
    m_prev_delta = timestamp - get_start_tstamp();
    ASSERT(m_prev_delta <= INT_MAX);
    (reinterpret_cast<aligned_type<uint32_t>*>(m_cursor))->value = (uint32_t)m_prev_delta;
    m_cursor += sizeof(uint32_t);
    (reinterpret_cast<aligned_type<double>*>(m_cursor))->value = value;
    m_cursor += sizeof(double);

    m_prev_tstamp = timestamp;
    m_prev_value = value;
    m_dp_count++;
}

bool
Compressor_v1::compress(
    Timestamp timestamp,        // t_n: tstamp of the new dp
    double value)               // v_n: value of the new dp
{
    if (m_base == m_cursor)
    {
        // the first dp
        compress1(timestamp, value);
        return true;
    }

    if (m_prev_tstamp > timestamp)
    {
        Logger::info("out-of-order dp dropped, timestamp = %" PRIu64, timestamp);
        return true;    // drop it
    }

    //ASSERT(m_prev_tstamp <= timestamp); // TODO: handle backfill
    ASSERT(m_base < m_cursor);
    ASSERT(m_dp_count > 0);

    uint8_t base[32];
    uint8_t *cursor = &base[0];

    // Timestamp first
    Timestamp delta = timestamp - m_prev_tstamp;
    uint64_t delta_of_delta = (uint64_t)delta - (uint64_t)m_prev_delta;

    if (g_tstamp_resolution_ms)
    {
        if (std::labs(delta_of_delta) > 32767)
        {
            (reinterpret_cast<aligned_type<int16_t>*>(cursor))->value = -32768;
            cursor += sizeof(int16_t);
            (reinterpret_cast<aligned_type<int32_t>*>(cursor))->value = (int32_t)delta_of_delta;
            cursor += sizeof(int32_t);
        }
        else
        {
            (reinterpret_cast<aligned_type<int16_t>*>(cursor))->value = (int16_t)delta_of_delta;
            cursor += sizeof(int16_t);
        }
    }
    else    // timestamp resolution is 'second'
    {
        if (std::labs(delta_of_delta) > 127)
        {
            *(reinterpret_cast<int8_t*>(cursor)) = -128;
            cursor += sizeof(int8_t);
            (reinterpret_cast<aligned_type<int32_t>*>(cursor))->value = (int32_t)delta_of_delta;
            cursor += sizeof(int32_t);
        }
        else
        {
            *(reinterpret_cast<int8_t*>(cursor)) = (int8_t)delta_of_delta;
            cursor += sizeof(int8_t);
        }
    }

    // Value next
    uint64_t x = (*reinterpret_cast<uint64_t*>(&value)) xor (*reinterpret_cast<uint64_t*>(&m_prev_value));
    uint8_t *control_ptr = cursor++;
    uint8_t control = 0;

    for (int i = 0; i < 8; i++)
    {
        uint8_t byte = reinterpret_cast<uint8_t*>(&x)[i];

        if (byte != 0)
        {
            control |= (uint8_t)(1 << (8-i-1));
            *cursor++ = byte;
        }
    }

    *control_ptr = control;

    size_t cnt = cursor - base;
    if (cnt > (m_size - (m_cursor - m_base)))
    {
        // not enough space left
        Logger::trace("page full: (cursor-base)=%d, size=%d, cnt=%d, dp_cnt=%d",
            (m_cursor - m_base), m_size, cnt, m_dp_count);
        m_is_full = true;
        return false;
    }

    //memcpy(m_cursor, base, cnt);
    for (int i = 0; i < cnt; i++)
        m_cursor[i] = base[i];
    m_cursor += cnt;
    m_dp_count++;

    m_prev_tstamp = timestamp;
    m_prev_value = value;
    m_prev_delta = delta;

    return true;
}

void
Compressor_v1::uncompress(DataPointVector& dps, bool restore)
{
    uint8_t *b = m_base;
    uint64_t delta;
    double value;
    Timestamp tstamp;
    DataPointPair dp;

    if (m_base == m_cursor)
    {
        if (restore) m_dp_count = 0;
        return;
    }

    // first dp
    delta = (reinterpret_cast<aligned_type<uint32_t>*>(b))->value;
    tstamp = get_start_tstamp() + delta;
    dp.first = tstamp;
    b += sizeof(uint32_t);
    value = (reinterpret_cast<aligned_type<double>*>(b))->value;
    dp.second = value;
    b += sizeof(double);
    dps.push_back(dp);

    // rest of the dps
    while (b < m_cursor)
    {
        uint64_t delta_of_delta;

        if (g_tstamp_resolution_ms)
        {
            int16_t x = (reinterpret_cast<aligned_type<int16_t>*>(b))->value;
            b += sizeof(int16_t);

            if (x == -32768)
            {
                delta_of_delta = (reinterpret_cast<aligned_type<int32_t>*>(b))->value;
                b += sizeof(int32_t);
            }
            else
            {
                delta_of_delta = (uint64_t)x;
            }
        }
        else    // timestamp resolution is 'second'
        {
            int8_t x = *(reinterpret_cast<int8_t*>(b));
            b += sizeof(int8_t);

            if (x == -128)
            {
                delta_of_delta = (reinterpret_cast<aligned_type<int32_t>*>(b))->value;
                b += sizeof(int32_t);
            }
            else
            {
                delta_of_delta = (uint64_t)x;
            }
        }

        delta += delta_of_delta;
        tstamp += delta;
        dp.first = tstamp;

        double v = 0;
        uint8_t control = *b++;

        if (control != 0)
        {
            for (int i = 0; i < 8; i++)
            {
                if (control & 0x80)
                {
                    reinterpret_cast<uint8_t*>(&v)[i] = *b++;
                }

                control <<= 1;
            }
        }

        uint64_t y = *(reinterpret_cast<uint64_t*>(&v)) xor *(reinterpret_cast<uint64_t*>(&value));
        value = *(reinterpret_cast<double*>(&y));

        dp.second = value;
        dps.push_back(dp);
    }

    if (restore)
    {
        m_prev_delta = delta;
        m_prev_value = value;
        m_prev_tstamp = tstamp;
        m_dp_count = dps.size();
    }
}

bool
Compressor_v1::recycle()
{
    m_is_full = false;
    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = get_start_tstamp();
    m_prev_value = 0.0;
    m_cursor = m_base;

    return Compressor::recycle();
}



// This compressor does not compress at all.

void
Compressor_v0::init(Timestamp start, uint8_t *base, size_t size)
{
    ASSERT(base != nullptr);

    Compressor::init(start, base, size);
    m_dps.clear();
    m_dps.reserve(g_page_size/sizeof(DataPointPair));
    m_size = std::floor(size / sizeof(DataPointPair));
    m_data_points = reinterpret_cast<DataPointPair*>(base);
}

void
Compressor_v0::restore(DataPointVector& dpv, CompressorPosition& position, uint8_t *base)
{
    ASSERT(position.m_start == 0);
    //ASSERT(position.m_offset <= m_size);

    DataPointPair *dps = m_data_points;

    if (base != nullptr)
    {
        dps = reinterpret_cast<DataPointPair*>(base);
    }

    for (int i = 0; i < position.m_offset; i++, dps++)
    {
        dpv.push_back(*dps);
        m_dps.push_back(*dps);
    }
}

void
Compressor_v0::save(uint8_t *base)
{
    ASSERT(base != nullptr);

    DataPointPair *dps = reinterpret_cast<DataPointPair*>(base);
    //if (dps == m_data_points) return;

    for (auto it = m_dps.begin(); it != m_dps.end(); it++, dps++)
    {
        *dps = *it;
    }
}

bool
Compressor_v0::compress(Timestamp timestamp, double value)
{
    if (m_dps.size() >= m_size) return false;

    Timestamp last_tstamp = get_last_tstamp();

    if ((last_tstamp <= timestamp) || m_dps.empty())
    {
        // append
        m_dps.emplace_back(timestamp, value);
    }
    else
    {
        // insert at right position to keep sorted order
        DataPointPair dp(timestamp, value);
        m_dps.insert(std::upper_bound(m_dps.begin(), m_dps.end(), dp, dp_pair_less), dp);
    }

    return true;
}

void
Compressor_v0::uncompress(DataPointVector& dps)
{
    for (auto it = m_dps.begin(); it != m_dps.end(); it++)
    {
        dps.push_back(*it);
    }
}

bool
Compressor_v0::recycle()
{
    m_dps.clear();
    m_dps.shrink_to_fit();
    return true;
}

Timestamp
Compressor_v0::get_last_tstamp() const
{
    if (! m_dps.empty())
    {
        return m_dps.back().first;
    }
    else
    {
        return get_start_tstamp_const();
    }
}


}
