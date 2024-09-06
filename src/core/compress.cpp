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
#include "config.h"
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

short Compressor_v4::m_repetition;
unsigned short Compressor_v4::m_max_repetition;
double Compressor_v4::m_precision;
double Compressor_v3::m_precision;
double RollupCompressor_v1::m_precision;


Compressor::Compressor()
{
    get_start_tstamp() = 0L;
}

void
Compressor::init(Timestamp start, uint8_t *base, size_t size)
{
    get_start_tstamp() = start;
}

void
Compressor::initialize()
{
    Compressor_v3::initialize();
    Compressor_v4::initialize();
    RollupCompressor_v1::init();
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

        case 3:
            compressor = new Compressor_v3();
            break;

        case 4:
            compressor = new Compressor_v4();
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

void
Compressor::set_start_tstamp(Timestamp tstamp)
{
    get_start_tstamp() = tstamp;
}


// Taking advantage of repetitions.
Compressor_v4::Compressor_v4()
{
    m_dp_count = 0;
    m_prev_tstamp = 0L;
    m_prev_tstamp_delta = 0L;
    m_prev_value = 0.0;
    m_prev_value_delta = 0.0;
    m_is_full = false;
    m_padded = true;
    m_repeat = 0;

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
}

void
Compressor_v4::initialize()
{
    int p = Config::inst()->get_int(CFG_TSDB_COMPRESSOR_PRECISION, CFG_TSDB_COMPRESSOR_PRECISION_DEF);
    if ((p < 0) || (p > 20))
    {
        Logger::warn("config %s of %d ignored, using default %d",
            CFG_TSDB_COMPRESSOR_PRECISION, p, CFG_TSDB_COMPRESSOR_PRECISION_DEF);
        p = CFG_TSDB_COMPRESSOR_PRECISION_DEF;
    }
    m_precision = std::pow(10, p);

    // 1 <= m_reppetition <= 7
    m_repetition = 7;
    m_max_repetition = std::pow(2, m_repetition) - 1;
}

void
Compressor_v4::init(Timestamp start, uint8_t *base, size_t size)
{
    ASSERT(base != nullptr);

    Compressor::init(start, base, size);

    m_bitset.init(base, size);

    m_dp_count = 0;
    m_prev_tstamp = start;
    m_prev_tstamp_delta = 0L;
    m_prev_value = 0.0;
    m_prev_value_delta = 0.0;
    m_is_full = false;
    m_padded = true;
    m_repeat = 0;

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
    ASSERT(m_bitset.avail_capacity_in_bits() >= 1);
}

void
Compressor_v4::restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base)
{
    Logger::debug("cv4: restoring from position: offset=%d, start=%d",
        position.m_offset, position.m_start);

    m_bitset.copy_from(base, position.m_offset, position.m_start);
    uncompress(dps, true);

    Logger::debug("cv4: restored %d data-points", m_dp_count);
}

void
Compressor_v4::save(CompressorPosition& position)
{
    size_t bit_cnt = m_bitset.size_in_bits();

    position.m_offset = bit_cnt / 8;
    position.m_start = bit_cnt % 8;

    Logger::debug("cv4: saved position: offset=%d, start=%d, #dp=%d",
        position.m_offset, position.m_start, m_dp_count);
}

void
Compressor_v4::save(uint8_t *base)
{
    ASSERT(base != nullptr);

    if (! m_padded)
    {
        if (m_repeat > 0)
        {
            uint8_t one_zero = (uint8_t)(1 << m_repetition) | m_repeat;
            m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), m_repetition+1, 7-m_repetition);
            m_repeat = 0;
        }
        else
        {
            uint8_t one_zero = 0;
            ASSERT(m_bitset.avail_capacity_in_bits() >= 1);
            m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
        }

        m_padded = true;
    }

    m_bitset.copy_to(base);
}

int
Compressor_v4::append(FILE *file)
{
    ASSERT(file != nullptr);
    return m_bitset.append(file);
}

void
Compressor_v4::compress1(Timestamp timestamp, double value)
{
    ASSERT(m_dp_count == 0);
    ASSERT(get_start_tstamp() <= timestamp);
    ASSERT((timestamp - get_start_tstamp()) <= INT_MAX);

    uint32_t delta = timestamp - get_start_tstamp();

    // TODO: make these lengths constants
    m_bitset.append(reinterpret_cast<uint8_t*>(&delta), 8*sizeof(uint32_t), 0);
    m_bitset.append(reinterpret_cast<uint8_t*>(&value), 8*sizeof(double), 0);

    m_prev_tstamp = timestamp;
    m_prev_value = value;
    m_prev_tstamp_delta = delta;
    m_dp_count++;

    ASSERT(size() == 12);
    ASSERT(m_bitset.avail_capacity_in_bits() >= 1);
}

bool
Compressor_v4::compress(Timestamp timestamp, double value)
{
    ASSERT(get_start_tstamp() <= timestamp);
    //ASSERT(m_is_full == false);
    ASSERT(m_bitset.avail_capacity_in_bits() >= 1);

    if (UNLIKELY(m_is_full)) return false;
    //m_bitset.save_check_point();

    try
    {
        if (m_dp_count == 0)
        {
            m_bitset.save_check_point();
            compress1(timestamp, value);
            m_padded = false;
            return true;
        }

        if (UNLIKELY(m_prev_tstamp > timestamp))
        {
            Logger::debug("out-of-order dp dropped, timestamp = %" PRIu64, timestamp);
            return true;    // drop it
        }

        ASSERT(m_dp_count > 0);

        // calcullate delta
        Timestamp delta = timestamp - m_prev_tstamp;
        int64_t delta_of_delta = (uint64_t)delta - (uint64_t)m_prev_tstamp_delta;
        double delta_v = value - m_prev_value;
        double delta_of_delta_v = delta_v - m_prev_value_delta;

        if (UNLIKELY(m_dp_count == 1))
        {
            m_bitset.save_check_point();
            compress(delta_of_delta);
            compress(delta_v);
        }
        else if (UNLIKELY(m_dp_count == 2))
        {
            m_bitset.save_check_point();
            compress(delta_of_delta);
            compress(delta_of_delta_v);
        }
        else if (((std::abs(delta_v - m_prev_value_delta) < (1.0 / m_precision)) && (delta == m_prev_tstamp_delta) && (m_repeat < m_max_repetition)) && !m_padded)
        {
            if ((m_repeat == 0) && (m_bitset.avail_capacity_in_bytes() < 1))
                throw std::out_of_range("bitset is full");
            m_repeat++;
        }
        else
        {
            if (! m_padded)
            {
                if (m_repeat > 0)
                {
                    uint8_t one_zero = (uint8_t)(1 << m_repetition) | m_repeat;
                    ASSERT((0x80 & one_zero) != 0x00);
                    m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), m_repetition+1, 7-m_repetition);
                    m_repeat = 0;
                }
                else
                {
                    uint8_t one_zero = 0x00;
                    m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
                }
                m_padded = true;
            }

            m_bitset.save_check_point();
            compress(delta_of_delta);
            compress(delta_of_delta_v);

            if (m_bitset.avail_capacity_in_bits() < 1)
                throw std::out_of_range("bitset is full");
        }

        m_prev_tstamp = timestamp;
        m_prev_tstamp_delta = delta;

        m_dp_count++;
        m_prev_value = value;
        m_prev_value_delta = delta_v;
        m_padded = false;
    }
    catch (const std::out_of_range& ex)
    {
        m_bitset.restore_from_check_point();
        ASSERT(m_bitset.avail_capacity_in_bits() >= 1 || m_padded);
        m_is_full = true;
        return false;
    }

    ASSERT(m_bitset.avail_capacity_in_bits() >= 1);
    return true;
}

void
Compressor_v4::compress(double v)
{
    double i, f;

    f = std::modf(v, &i);

    if (std::abs(f) < (1.0 / m_precision))
    {
        uint8_t one_zero = 0x00;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
        compress((int64_t)v);
    }
    else
    {
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
        compress((int64_t)std::llround(v * m_precision));
    }
}

void
Compressor_v4::compress(int64_t n)
{
    if (n == 0)
    {
        // store a single '0' bit
        uint8_t zero = 0x00;
        m_bitset.append(reinterpret_cast<uint8_t*>(&zero), 1, 0);
    }
/*
    else if ((-32 <= n) && (n <= 31))
    {
        // store '10' followed by value in 6 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint8_t dod_be = htobe16((uint8_t)n);
        m_bitset.append(&dod_be, 6, 8-6);
    }
    else if ((-8192 <= n) && (n <= 8191))
    {
        // store '10' followed by value in 14 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 14, 16-14);
    }
    else if ((-4096 <= n) && (n <= 4095))
    {
        // store '10' followed by value in 13 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 13, 16-13);
    }
*/
    else if ((-2048 <= n) && (n <= 2047))
    {
        // store '10' followed by value in 12 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 12, 16-12);
    }
    else if ((-65536 <= n) && (n <= 65535))
    {
        // store '110' followed by value in 17 bits
        uint8_t one_one_zero = 0xC0;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_zero), 3, 0);
        uint32_t dod_be = htobe32((uint32_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);
    }
    else
    {
        // store '111' followed by value in 64 bits
        uint8_t one_one_one = 0xE0;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_one), 3, 0);
        uint64_t dod_be = htobe64((uint64_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 64, 0);
    }
}

void
Compressor_v4::uncompress(DataPointVector& dps, bool restore)
{
    if (m_bitset.is_empty())
        return;

    Timestamp timestamp;
    double value = 0.0;
    BitSetCursor *cursor = m_bitset.new_cursor();

    // TODO: check for m_bitset being empty
    // 1st data-point
    uint32_t delta32 = 0;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&delta32), 32, 0);
    timestamp = get_start_tstamp() + delta32;
    uint64_t delta = delta32;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&value), 8*sizeof(double), 0);
    ASSERT(get_start_tstamp() <= timestamp);
    dps.emplace_back(timestamp, value);

    double delta_v = 0;
    int64_t delta_of_delta;

    // 2nd data-point
    try
    {
        delta_of_delta = uncompress_i(cursor);
        delta += delta_of_delta;
        timestamp += delta;

        delta_v = uncompress_f(cursor);
        value += delta_v;
        dps.emplace_back(timestamp, value);
    }
    catch (const std::out_of_range& ex)
    {
    }

    while (true)
    {
        try
        {
            // timestamp
            delta_of_delta = uncompress_i(cursor);
            delta += delta_of_delta;
            timestamp += delta;

            // value
            double delta_of_delta_v = uncompress_f(cursor);
            delta_v += delta_of_delta_v;
            value += delta_v;
            dps.emplace_back(timestamp, value);

            // repeat, if any
            uint8_t byte = 0;
            m_bitset.retrieve(cursor, &byte, 1, 0);
            if ((byte & 0x80) != 0)
            //if (byte != 0)
            {
                byte = 0;
                m_bitset.retrieve(cursor, &byte, m_repetition, 8-m_repetition);
                ASSERT(byte != 0);

                for (uint8_t i = 0; i < byte; i++)
                {
                    timestamp += delta;
                    value += delta_v;
                    dps.emplace_back(timestamp, value);
                }
            }
        }
        catch (const std::out_of_range& ex)
        {
            break;
        }
    }

    if (m_repeat > 0)
    {
        for (uint8_t i = 0; i < m_repeat; i++)
        {
            timestamp += delta;
            value += delta_v;
            dps.emplace_back(timestamp, value);
        }
    }

    MemoryManager::free_recyclable(cursor);

    if (restore)
    {
        m_dp_count = dps.size();
        m_repeat = 0;
        m_padded = true;
        m_prev_tstamp_delta = delta;
        m_prev_tstamp = timestamp;
        m_prev_value = value;
        m_prev_value_delta = delta_v;
    }

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
}

double
Compressor_v4::uncompress_f(BitSetCursor *cursor)
{
    uint8_t byte = 0;

    m_bitset.retrieve(cursor, &byte, 1, 0);

    if ((byte & 0x80) == 0)
    {
        return (double)uncompress_i(cursor);
    }
    else
    {
        int64_t v = uncompress_i(cursor);
        return ((double)v) / m_precision;
    }
}

int64_t
Compressor_v4::uncompress_i(BitSetCursor *cursor)
{
    uint8_t byte = 0;
    int64_t result;

    m_bitset.retrieve(cursor, &byte, 1, 0);

    if ((byte & 0x80) == 0)
    {
        result = 0;
    }
    else
    {
        uint64_t delta_of_delta = 0;

        m_bitset.retrieve(cursor, &byte, 1, 0);

        if ((byte & 0x80) == 0)
        {
            // 12-bit
            uint16_t dod_be = 0;
            m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 12, 16-12);

            if ((*reinterpret_cast<uint16_t*>(&dod_be) & 0x0008) != 0)
            {
                dod_be |= 0x00F0;
            }
            else
            {
                dod_be &= 0xFF07;
            }

            result = (int16_t)htobe16(dod_be);
        }
        else
        {
            m_bitset.retrieve(cursor, &byte, 1, 0);

            if ((byte & 0x80) == 0)
            {
                // 17-bit
                uint32_t dod_be = 0;
                m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);

                if ((*reinterpret_cast<uint32_t*>(&dod_be) & 0x00000100) != 0)
                {
                    dod_be |= 0x0000FEFF;
                }
                else
                {
                    dod_be &= 0xFFFF0000;
                }

                result = (int32_t)htobe32(dod_be);
            }
            else
            {
                // 64-bit
                uint64_t dod_be;
                m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 64, 0);
                result = (int64_t)htobe64(dod_be);
            }
        }
    }

    return result;
}

bool
Compressor_v4::recycle()
{
    m_dp_count = 0;
    m_prev_tstamp_delta = 0L;
    m_prev_tstamp = get_start_tstamp();
    m_prev_value = 0.0;
    m_prev_value_delta = 0.0;
    m_is_full = false;

    m_bitset.recycle();
    return Compressor::recycle();
}


// Implementation of Gorilla compression algorithm.
Compressor_v3::Compressor_v3()
{
    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = 0L;
    m_prev_value = 0.0;
    m_is_full = false;

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
}

void
Compressor_v3::initialize()
{
    int p = Config::inst()->get_int(CFG_TSDB_COMPRESSOR_PRECISION, CFG_TSDB_COMPRESSOR_PRECISION_DEF);
    if ((p < 0) || (p > 20))
    {
        Logger::warn("config %s of %d ignored, using default %d",
            CFG_TSDB_COMPRESSOR_PRECISION, p, CFG_TSDB_COMPRESSOR_PRECISION_DEF);
        p = CFG_TSDB_COMPRESSOR_PRECISION_DEF;
    }
    m_precision = std::pow(10, p);
}

void
Compressor_v3::init(Timestamp start, uint8_t *base, size_t size)
{
    ASSERT(base != nullptr);

    Compressor::init(start, base, size);

    m_bitset.init(base, size);

    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = start;
    m_prev_value = 0.0;
    m_is_full = false;

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
}

void
Compressor_v3::restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base)
{
    Logger::debug("cv3: restoring from position: offset=%d, start=%d",
        position.m_offset, position.m_start);

    m_bitset.copy_from(base, position.m_offset, position.m_start);
    uncompress(dps, true);

    Logger::debug("cv3: restored %d data-points", m_dp_count);
}

void
Compressor_v3::save(CompressorPosition& position)
{
    size_t bit_cnt = m_bitset.size_in_bits();

    position.m_offset = bit_cnt / 8;
    position.m_start = bit_cnt % 8;

    Logger::debug("cv3: saved position: offset=%d, start=%d, #dp=%d",
        position.m_offset, position.m_start, m_dp_count);
}

int
Compressor_v3::append(FILE *file)
{
    ASSERT(file != nullptr);
    return m_bitset.append(file);
}

void
Compressor_v3::compress1(Timestamp timestamp, double value)
{
    ASSERT(m_dp_count == 0);
    ASSERT(get_start_tstamp() <= timestamp);
    ASSERT((timestamp - get_start_tstamp()) <= INT_MAX);

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
Compressor_v3::compress(Timestamp timestamp, double value)
{
    ASSERT(get_start_tstamp() <= timestamp);
    //ASSERT(m_is_full == false);

    if (UNLIKELY(m_is_full)) return false;
    m_bitset.save_check_point();

    try
    {
        if (m_dp_count == 0)
        {
            compress1(timestamp, value);
            return true;
        }

        if (UNLIKELY(m_prev_tstamp > timestamp))
        {
            Logger::debug("out-of-order dp dropped, timestamp = %" PRIu64, timestamp);
            return true;    // drop it
        }

        ASSERT(m_dp_count > 0);

        // Timestamp first
        Timestamp delta = timestamp - m_prev_tstamp;
        int64_t delta_of_delta = (uint64_t)delta - (uint64_t)m_prev_delta;
        compress(delta_of_delta);

        m_prev_tstamp = timestamp;
        m_prev_delta = delta;

        // Value next
        double v = value - m_prev_value;
        compress(v);

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
Compressor_v3::compress(double v)
{
    double i, f;

    f = std::modf(v, &i);

    if (std::abs(f) < (1.0 / m_precision))
    {
        uint8_t one_zero = 0x00;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
        compress((int64_t)v);
    }
    else
    {
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 1, 0);
        compress((int64_t)std::llround(v * m_precision));
    }
}

void
Compressor_v3::compress(int64_t n)
{
    if (n == 0)
    {
        // store a single '0' bit
        uint8_t zero = 0x00;
        m_bitset.append(reinterpret_cast<uint8_t*>(&zero), 1, 0);
    }
/*
    else if ((-32 <= n) && (n <= 31))
    {
        // store '10' followed by value in 6 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint8_t dod_be = htobe16((uint8_t)n);
        m_bitset.append(&dod_be, 6, 8-6);
    }
    else if ((-8192 <= n) && (n <= 8191))
    {
        // store '10' followed by value in 14 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 14, 16-14);
    }
    else if ((-4096 <= n) && (n <= 4095))
    {
        // store '10' followed by value in 13 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 13, 16-13);
    }
*/
    else if ((-2048 <= n) && (n <= 2047))
    {
        // store '10' followed by value in 12 bits
        uint8_t one_zero = 0x80;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_zero), 2, 0);
        uint16_t dod_be = htobe16((uint16_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 12, 16-12);
    }
    else if ((-65536 <= n) && (n <= 65535))
    {
        // store '110' followed by value in 17 bits
        uint8_t one_one_zero = 0xC0;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_zero), 3, 0);
        uint32_t dod_be = htobe32((uint32_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);
    }
    else
    {
        // store '111' followed by value in 64 bits
        uint8_t one_one_one = 0xE0;
        m_bitset.append(reinterpret_cast<uint8_t*>(&one_one_one), 3, 0);
        uint64_t dod_be = htobe64((uint64_t)n);
        m_bitset.append(reinterpret_cast<uint8_t*>(&dod_be), 64, 0);
    }
}

void
Compressor_v3::uncompress(DataPointVector& dps, bool restore)
{
    if (m_bitset.is_empty())
        return;

    Timestamp timestamp;
    double value = 0.0;

    //uint64_t value_be;

    //uint8_t leading_zeros = 0;
    //uint8_t trailing_zeros = 0;
    //uint8_t none_zeros = 0;

    BitSetCursor *cursor = m_bitset.new_cursor();

    // TODO: check for m_bitset being empty
    // 1st data-point
    uint32_t delta32 = 0;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&delta32), 32, 0);
    timestamp = get_start_tstamp() + delta32;
    uint64_t delta = delta32;
    m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&value), 8*sizeof(double), 0);
    ASSERT(get_start_tstamp() <= timestamp);
    dps.emplace_back(timestamp, value);

    //value_be = htobe64(*reinterpret_cast<uint64_t*>(&value));

    //uint64_t y = *reinterpret_cast<uint64_t*>(&value);

    while (true)
    {
        try
        {
            // timestamp
            int64_t delta_of_delta;

            delta_of_delta = uncompress_i(cursor);
            delta += delta_of_delta;
            timestamp += delta;

            // value
            double delta_of_delta_v = uncompress_f(cursor);
            value += delta_of_delta_v;
            dps.emplace_back(timestamp, value);
        }
        catch (const std::out_of_range& ex)
        {
            break;
        }
    }

    MemoryManager::free_recyclable(cursor);

    if (restore)
    {
        m_dp_count = dps.size();
        m_prev_delta = delta;
        m_prev_tstamp = timestamp;
        m_prev_value = value;
    }

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
}

double
Compressor_v3::uncompress_f(BitSetCursor *cursor)
{
    uint8_t byte = 0;

    m_bitset.retrieve(cursor, &byte, 1, 0);

    if ((byte & 0x80) == 0)
    {
        return (double)uncompress_i(cursor);
    }
    else
    {
        int64_t v = uncompress_i(cursor);
        return ((double)v) / m_precision;
    }
}

int64_t
Compressor_v3::uncompress_i(BitSetCursor *cursor)
{
    uint8_t byte = 0;
    int64_t result;

    m_bitset.retrieve(cursor, &byte, 1, 0);

    if ((byte & 0x80) == 0)
    {
        result = 0;
    }
    else
    {
        uint64_t delta_of_delta = 0;

        m_bitset.retrieve(cursor, &byte, 1, 0);

        if ((byte & 0x80) == 0)
        {
            // 12-bit
            uint16_t dod_be = 0;
            m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 12, 16-12);

            if ((*reinterpret_cast<uint16_t*>(&dod_be) & 0x0008) != 0)
            {
                dod_be |= 0x00F0;
            }
            else
            {
                dod_be &= 0xFF07;
            }

            result = (int16_t)htobe16(dod_be);
        }
        else
        {
            m_bitset.retrieve(cursor, &byte, 1, 0);

            if ((byte & 0x80) == 0)
            {
                // 17-bit
                uint32_t dod_be = 0;
                m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 17, 32-17);

                if ((*reinterpret_cast<uint32_t*>(&dod_be) & 0x00000100) != 0)
                {
                    dod_be |= 0x0000FEFF;
                }
                else
                {
                    dod_be &= 0xFFFF0000;
                }

                result = (int32_t)htobe32(dod_be);
            }
            else
            {
                // 64-bit
                uint64_t dod_be;
                m_bitset.retrieve(cursor, reinterpret_cast<uint8_t*>(&dod_be), 64, 0);
                result = (int64_t)htobe64(dod_be);
            }
        }
    }

    return result;
}

bool
Compressor_v3::recycle()
{
    m_dp_count = 0;
    m_prev_delta = 0L;
    m_prev_tstamp = get_start_tstamp();
    m_prev_value = 0.0;
    m_is_full = false;

    m_bitset.recycle();
    return Compressor::recycle();
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

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
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

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
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

int
Compressor_v2::append(FILE *file)
{
    ASSERT(file != nullptr);
    return m_bitset.append(file);
}

void
Compressor_v2::compress1(Timestamp timestamp, double value)
{
    ASSERT(m_dp_count == 0);
    ASSERT(get_start_tstamp() <= timestamp);
    ASSERT((timestamp - get_start_tstamp()) <= INT_MAX);

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
    ASSERT(get_start_tstamp() <= timestamp);

    m_bitset.save_check_point();

    try
    {
        if (m_dp_count == 0)
        {
            compress1(timestamp, value);
            return true;
        }

        if (UNLIKELY(m_prev_tstamp > timestamp))
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
    if (m_bitset.is_empty())
        return;

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
    ASSERT(get_start_tstamp() <= timestamp);
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

    ASSERT(get_start_tstamp() <= m_prev_tstamp);
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

    if ((base != nullptr) && (m_base != base))
        memcpy(m_base, base, position.m_offset);

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

int
Compressor_v1::append(FILE *file)
{
    ASSERT(file != nullptr);
    return fwrite(m_base, 1, (m_cursor-m_base), file);
}

void
Compressor_v1::compress1(
    Timestamp timestamp,        // t_1: tstamp of the new dp
    double value)               // v_1: value of the new dp
{
    ASSERT(m_dp_count == 0);
    ASSERT(get_start_tstamp() <= timestamp);
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

    // 'base' needs to be aligned
#if (__ARM_32BIT_STATE == 1)
    unsigned int r = (unsigned int)base % 4;
    if (r != 0)
    {
        ASSERT((4 - r) <= size);
        base += 4 - r;
        m_size -= 4 - r;
    }
#elif (__ARM_64BIT_STATE == 1)
    uint64_t r = (uint64_t)base % 8;
    if (r != 0)
    {
        ASSERT((8 - r) <= size);
        base += 8 - r;
        m_size -= 8 - r;
    }
#endif

    m_dps.clear();
    m_dps.reserve(g_page_size/sizeof(DataPointPair));
    m_size = std::floor(size / sizeof(DataPointPair));
    ASSERT(m_size > 0);
    m_data_points = reinterpret_cast<DataPointPair*>(base);
}

void
Compressor_v0::restore(DataPointVector& dpv, CompressorPosition& position, uint8_t *base)
{
    ASSERT(position.m_start == 0);
    //ASSERT(position.m_offset <= m_size);

#if (__ARM_32BIT_STATE == 1)
    unsigned int r = (unsigned int)base % 4;
    if (r != 0)
        base += 4 - r;
#elif (__ARM_64BIT_STATE == 1)
    uint64_t r = (uint64_t)base % 8;
    if (r != 0)
        base += 8 - r;
#endif

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

    // 'base' needs to be aligned
#if (__ARM_32BIT_STATE == 1)
    unsigned int r = (unsigned int)base % 4;
    if (r != 0)
        base += 4 - r;
#elif (__ARM_64BIT_STATE == 1)
    uint64_t r = (uint64_t)base % 8;
    if (r != 0)
        base += 8 - r;
#endif

    DataPointPair *dps = reinterpret_cast<DataPointPair*>(base);
    //if (dps == m_data_points) return;

    for (auto it = m_dps.begin(); it != m_dps.end(); it++, dps++)
    {
        *dps = *it;
    }
}

int
Compressor_v0::append(FILE *file)
{
    ASSERT(file != nullptr);
    ASSERT(m_data_points != nullptr);
    ASSERT(m_size >= m_dps.size());

    this->save((uint8_t*)m_data_points);
    return fwrite(m_data_points, 1, m_dps.size()*sizeof(DataPointPair), file);
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


void
RollupCompressor_v1::init()
{
    int p = Config::inst()->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF);
    if ((p < 0) || (p > 20))
    {
        Logger::warn("config %s of %d ignored, using default %d",
            CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, p, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF);
        p = CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF;
    }
    m_precision = std::pow(10, p);
}

/* flag bits:
 *   1st bit: tid (0 => 3 bytes, 1 => 4 bytes)
 *   2nd bit: cnt (0 => 2 bytes, 1 => 4 bytes)
 *   3-4 bit: min (00 => 2 bytes, 01 => 3 bytes, 10 => 4 bytes, 11 => 8 bytes)
 *   5-6 bit: max (00 => 3 bytes, 01 => 4 bytes, 10 => 5 bytes, 11 => 8 bytes)
 *   7-8 bit: sum (00 => 3 bytes, 01 => 4 bytes, 10 => 5 bytes, 11 => 8 bytes)
 */
int
RollupCompressor_v1::compress(uint8_t *buff, TimeSeriesId tid, uint32_t cnt, double min, double max, double sum, double precision)
{
    ASSERT(buff != nullptr);
    ASSERT(tid != TT_INVALID_TIME_SERIES_ID);

    int idx = 1;

    if (tid <= 0xFFFFFF)
    {
        buff[0] = 0x00;
        compress_int24(tid, buff+idx);
        idx += 3;
    }
    else
    {
        buff[0] = 0x80;
        compress_int32(tid, buff+idx);
        idx += 4;
    }

    if (cnt <= 0xFFFF)
    {
        compress_int16(cnt, buff+idx);
        idx += 2;
    }
    else
    {
        buff[0] |= 0x40;
        compress_int32(cnt, buff+idx);
        idx += 4;
    }

    if (cnt == 0)
        return idx;

    int64_t n = std::llround(min * precision);

    if (-32768 <= n && n <= 32767)
    {
        compress_int16(n, buff+idx);
        idx += 2;
    }
    else if (-8388608 <= n && n <= 8388607)
    {
        buff[0] |= 0x10;
        compress_int24(n, buff+idx);
        idx += 3;
    }
    else if (-2147483648 <= n && n <= 2147483647)
    {
        buff[0] |= 0x20;
        compress_int32(n, buff+idx);
        idx += 4;
    }
    else
    {
        buff[0] |= 0x30;
        compress_int64(n, buff+idx);
        idx += 8;
    }

    n = std::llround(max * precision);

    if (-8388608 <= n && n <= 8388607)
    {
        compress_int24(n, buff+idx);
        idx += 3;
    }
    else if (-2147483648 <= n && n <= 2147483647)
    {
        buff[0] |= 0x04;
        compress_int32(n, buff+idx);
        idx += 4;
    }
    else if (-549755813888 <= n && n <= 549755813887)
    {
        buff[0] |= 0x08;
        compress_int40(n, buff+idx);
        idx += 5;
    }
    else
    {
        buff[0] |= 0x0C;
        compress_int64(n, buff+idx);
        idx += 8;
    }

    n = std::llround(sum * precision);

    if (-8388608 <= n && n <= 8388607)
    {
        compress_int24(n, buff+idx);
        idx += 3;
    }
    else if (-2147483648 <= n && n <= 2147483647)
    {
        buff[0] |= 0x01;
        compress_int32(n, buff+idx);
        idx += 4;
    }
    else if (-549755813888 <= n && n <= 549755813887)
    {
        buff[0] |= 0x02;
        compress_int40(n, buff+idx);
        idx += 5;
    }
    else
    {
        buff[0] |= 0x03;
        compress_int64(n, buff+idx);
        idx += 8;
    }

    return idx;
}

/* @return Number of bytes processed during uncompress; 0 if not enough data in the buff
 */
int
RollupCompressor_v1::uncompress(uint8_t *buff, int size, struct rollup_entry *entry, double precision)
{
    ASSERT(buff != nullptr);
    ASSERT(entry != nullptr);
    ASSERT(precision != 0.0);

    if (size < 6) return 0;

    int len = 1;
    uint8_t flag = buff[0];

    if (flag & 0x80)            // tid
    {
        entry->tid = uncompress_uint32(buff+len);
        len += 4;
        ASSERT(len == 5);
    }
    else
    {
        entry->tid = uncompress_uint24(buff+len);
        len += 3;
        ASSERT(len == 4);
    }

    if (flag & 0x40)            // cnt
    {
        if ((size - len) < 4) return 0;
        entry->cnt = uncompress_uint32(buff+len);
        len += 4;
    }
    else
    {
        if ((size - len) < 2) return 0;
        entry->cnt = uncompress_uint16(buff+len);
        len += 2;
    }

    ASSERT(len <= 9);

    if (entry->cnt != 0)
    {
        if ((size - len) < 8) return 0;     // not enough data in buffer

        if ((flag & 0x30) == 0x00)          // min
        {
            int16_t min = uncompress_int16(buff+len);
            entry->min = (double)min / precision;
            len += 2;
        }
        else if ((flag & 0x30) == 0x10)
        {
            int32_t min = uncompress_int24(buff+len);
            entry->min = (double)min / precision;
            len += 3;
        }
        else if ((flag & 0x30) == 0x20)
        {
            int32_t min = uncompress_int32(buff+len);
            entry->min = (double)min / precision;
            len += 4;
        }
        else
        {
            int64_t min = uncompress_int64(buff+len);
            entry->min = (double)min / precision;
            len += 8;
        }

        if ((size - len) < 6) return 0;

        if ((flag & 0x0C) == 0x00)          // max
        {
            int32_t max = uncompress_int24(buff+len);
            entry->max = (double)max / precision;
            len += 3;
        }
        else if ((flag & 0x0C) == 0x04)
        {
            int32_t max = uncompress_int32(buff+len);
            entry->max = (double)max / precision;
            len += 4;
        }
        else if ((flag & 0x0C) == 0x08)
        {
            int64_t max = uncompress_int40(buff+len);
            entry->max = (double)max / precision;
            len += 5;
        }
        else
        {
            if ((size - len) < 8) return 0;

            int64_t max = uncompress_int64(buff+len);
            entry->max = (double)max / precision;
            len += 8;
        }

        if ((flag & 0x03) == 0x00)          // sum
        {
            if ((size - len) < 3) return 0;
            int32_t sum = uncompress_int24(buff+len);
            entry->sum = (double)sum / precision;
            len += 3;
        }
        else if ((flag & 0x03) == 0x01)
        {
            if ((size - len) < 4) return 0;
            int32_t sum = uncompress_int32(buff+len);
            entry->sum = (double)sum / precision;
            len += 4;
        }
        else if ((flag & 0x03) == 0x02)
        {
            if ((size - len) < 5) return 0;
            int64_t sum = uncompress_int40(buff+len);
            entry->sum = (double)sum / precision;
            len += 5;
        }
        else
        {
            if ((size - len) < 8) return 0;

            int64_t sum = uncompress_int64(buff+len);
            entry->sum = (double)sum / precision;
            len += 8;
        }
    }

    return len;
}

void
RollupCompressor_v1::compress_int16(int64_t n, uint8_t *buff)
{
    int idx = 0;
    uint16_t x = htobe16((uint16_t)n);
    buff[idx++] = ((uint8_t*)&x)[0];
    buff[idx++] = ((uint8_t*)&x)[1];
}

void
RollupCompressor_v1::compress_int24(int64_t n, uint8_t *buff)
{
    int idx = 0;
    uint32_t x = htobe32((uint32_t)n);
    buff[idx++] = ((uint8_t*)&x)[1];
    buff[idx++] = ((uint8_t*)&x)[2];
    buff[idx++] = ((uint8_t*)&x)[3];
}

void
RollupCompressor_v1::compress_int32(int64_t n, uint8_t *buff)
{
    int idx = 0;
    uint32_t x = htobe32((uint32_t)n);
    buff[idx++] = ((uint8_t*)&x)[0];
    buff[idx++] = ((uint8_t*)&x)[1];
    buff[idx++] = ((uint8_t*)&x)[2];
    buff[idx++] = ((uint8_t*)&x)[3];
}

void
RollupCompressor_v1::compress_int40(int64_t n, uint8_t *buff)
{
    int idx = 0;
    uint64_t x = htobe64((uint64_t)n);
    buff[idx++] = ((uint8_t*)&x)[3];
    buff[idx++] = ((uint8_t*)&x)[4];
    buff[idx++] = ((uint8_t*)&x)[5];
    buff[idx++] = ((uint8_t*)&x)[6];
    buff[idx++] = ((uint8_t*)&x)[7];
}

void
RollupCompressor_v1::compress_int64(int64_t n, uint8_t *buff)
{
    uint64_t x = htobe64((uint64_t)n);
    std::memcpy(buff, (uint8_t*)&x, 8);
}

int16_t
RollupCompressor_v1::uncompress_int16(uint8_t *buff)
{
    ASSERT(buff != nullptr);

    int16_t n = 0;
    ((uint8_t*)&n)[0] = buff[0];
    ((uint8_t*)&n)[1] = buff[1];
    return htobe16(n);
}

int32_t
RollupCompressor_v1::uncompress_int24(uint8_t *buff)
{
    ASSERT(buff != nullptr);

    uint32_t x = 0;

    ((uint8_t*)&x)[1] = buff[0];
    ((uint8_t*)&x)[2] = buff[1];
    ((uint8_t*)&x)[3] = buff[2];

    if ((x & 0x00008000))
        x |= 0x000000FF;    // it's a negative number

    return (int32_t)htobe32(x);
}

int32_t
RollupCompressor_v1::uncompress_int32(uint8_t *buff)
{
    ASSERT(buff != nullptr);

    uint32_t x = 0;

    ((uint8_t*)&x)[0] = buff[0];
    ((uint8_t*)&x)[1] = buff[1];
    ((uint8_t*)&x)[2] = buff[2];
    ((uint8_t*)&x)[3] = buff[3];

    return (int32_t)htobe32(x);
}

int64_t
RollupCompressor_v1::uncompress_int40(uint8_t *buff)
{
    ASSERT(buff != nullptr);
    uint64_t x = 0;

    ((uint8_t*)&x)[3] = buff[0];
    ((uint8_t*)&x)[4] = buff[1];
    ((uint8_t*)&x)[5] = buff[2];
    ((uint8_t*)&x)[6] = buff[3];
    ((uint8_t*)&x)[7] = buff[4];

    if ((x & 0x0000000080000000))
        x |= 0x0000000000FFFFFF;    // it's a negative number

    return (int64_t)htobe64(x);
}

int64_t
RollupCompressor_v1::uncompress_int64(uint8_t *buff)
{
    ASSERT(buff != nullptr);

    int64_t x = 0;

    for (int i = 0; i < 8; i++)
        ((uint8_t*)&x)[i] = buff[i];

    return (int64_t)htobe64(x);
}

uint32_t
RollupCompressor_v1::uncompress_uint16(uint8_t *buff)
{
    ASSERT(buff != nullptr);
    int32_t x = 0;

    ((uint8_t*)&x)[2] = buff[0];
    ((uint8_t*)&x)[3] = buff[1];

    return be32toh(x);
}

uint32_t
RollupCompressor_v1::uncompress_uint24(uint8_t *buff)
{
    ASSERT(buff != nullptr);
    uint32_t x = 0;

    ((uint8_t*)&x)[1] = buff[0];
    ((uint8_t*)&x)[2] = buff[1];
    ((uint8_t*)&x)[3] = buff[2];

    return be32toh(x);
}

uint32_t
RollupCompressor_v1::uncompress_uint32(uint8_t *buff)
{
    ASSERT(buff != nullptr);
    uint32_t x = 0;

    for (int i = 0; i < 4; i++)
        ((uint8_t*)&x)[i] = buff[i];

    return be32toh(x);
}


}
