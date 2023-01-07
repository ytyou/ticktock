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

#include <utility>
#include <vector>
#include "bitset.h"
#include "page.h"
#include "recycle.h"
#include "type.h"


// Timestamp compression:
//
// In the header section of the Tsdb file, we store the starting timestamp (t0).
// For each time series, the very first timestamp (t1) in the file is stored as
// (t1 - t0) in a uint32_t. The following timestamps (t_n) is stored as
// (t_n - t_n-1) - (t_n-1 - t_n-2) in a uint16_t. If the value is too big to
// fit in a uint16_t, then we store -32768, followed by a uint32_t. So on
// average we should be close to 2 bytes per dp for the timestamp part.
//
// Value compaction:
//
// The first value (v1) is stored uncompressed in a double (8-bytes). For the
// following values (v_n), calculate x = (v_n-1 XOR v_n). We the store the
// control byte followed by the non-zero bytes as follows:
//   0x00 if x is of 0x0000000000000000
//   0x01 if x is of 0x00000000000000vv
//   0x02 if x is of 0x000000000000vv00
//   0x03 if x is of 0x000000000000vvvv
//   0x04 if x is of 0x0000000000vv0000
//   0x05 if x is of 0x0000000000vv00vv
//   0x06 if x is of 0x0000000000vvvv00
//   0x07 if x is of 0x0000000000vvvvvv
//   0x08 if x is of 0x00000000vv000000
//   ... ...


namespace tt
{


class CompressorPosition
{
public:
    CompressorPosition() : CompressorPosition(0, 0, 0)
    {
    }

    CompressorPosition(Timestamp tstamp, PageSize offset, uint8_t start) :
        m_offset(offset),
        m_start(start)
    {
    }

    CompressorPosition(struct page_info_on_disk *piod) :
        m_offset(piod->m_cursor),
        m_start(piod->m_start)
    {
    }

    PageSize m_offset;
    uint8_t m_start;
};


// the Compressor interface
#ifdef __x86_64__
class __attribute__ ((__packed__)) Compressor : public Recyclable
#else
class Compressor : public Recyclable
#endif
{
public:
    static Compressor *create(int version);

    virtual void init(Timestamp start, uint8_t *base, size_t size);
    virtual void restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base) = 0;
    virtual void save(CompressorPosition& position) = 0;    // save meta
    virtual void save(uint8_t *base) = 0;                   // save data
    virtual bool recycle() { return true; }
    virtual void rebase(uint8_t *base) = 0;

    // return true if sucessfully added the dp;
    // return false if the buffer is full;
    virtual bool compress(Timestamp timestamp, double value) = 0;
    virtual void uncompress(DataPointVector& dps) = 0;

    virtual bool is_full() const = 0;
    virtual bool is_empty() const = 0;
    virtual size_t size() const = 0;    // return number of bytes

    // return number of data points if already uncompressed;
    // return 0 otherwise;
    virtual uint16_t get_dp_count() const = 0;
    virtual Timestamp get_last_tstamp() const = 0;
    virtual int get_version() const = 0;
    Timestamp& get_start_tstamp();
    Timestamp get_start_tstamp_const() const;

protected:
    Compressor();

#ifndef __x86_64__
    Timestamp m_start_tstamp;
#endif
};


// This is a modified version of Facebook's Gorilla compression algorithm.
class __attribute__ ((__packed__)) Compressor_v3 : public Compressor
{
public:
    void init(Timestamp start, uint8_t *base, size_t size);
    void restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base);
    void save(CompressorPosition& position);
    inline void rebase(uint8_t *base)
    {
        m_bitset.rebase(base);
    }

    inline void save(uint8_t *base)
    {
        ASSERT(base != nullptr);
        m_bitset.copy_to(base);
    }

    bool compress(Timestamp timestamp, double value);

    inline void uncompress(DataPointVector& dps)
    {
        uncompress(dps, false);
    }

    inline bool is_full() const
    {
        return m_is_full;
    }

    inline bool is_empty() const
    {
        return (m_dp_count == 0);
    }

    inline size_t size() const  // return number of bytes
    {
        return m_bitset.size_in_bytes();
    }

    inline uint16_t get_dp_count() const
    {
        return m_dp_count;
    }

    inline Timestamp get_last_tstamp() const
    {
        return m_prev_tstamp;
    }

    inline int get_version() const override
    {
        return 3;
    };

    virtual bool recycle();

private:
    friend class Compressor;

    Compressor_v3();
    void compress1(Timestamp timestamp, double value);
    void compress(double n);
    void compress(int64_t n);
    void uncompress(DataPointVector& dps, bool restore);
    double uncompress_f(BitSetCursor *cursor);
    int64_t uncompress_i(BitSetCursor *cursor);

    BitSet m_bitset;
    uint16_t m_dp_count;

    Timestamp m_prev_delta;
    Timestamp m_prev_tstamp;
    double m_prev_value;
    bool m_is_full;
};


// This implements Facebook's Gorilla compression algorithm.
class __attribute__ ((__packed__)) Compressor_v2 : public Compressor
{
public:
    void init(Timestamp start, uint8_t *base, size_t size);
    void restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base);
    void save(CompressorPosition& position);
    inline void rebase(uint8_t *base)
    {
        m_bitset.rebase(base);
    }

    inline void save(uint8_t *base)
    {
        ASSERT(base != nullptr);
        m_bitset.copy_to(base);
    }

    bool compress(Timestamp timestamp, double value);

    inline void uncompress(DataPointVector& dps)
    {
        uncompress(dps, false);
    }

    inline bool is_full() const
    {
        return m_is_full;
    }

    inline bool is_empty() const
    {
        return (m_dp_count == 0);
    }

    inline size_t size() const  // return number of bytes
    {
        return m_bitset.size_in_bytes();
    }

    inline uint16_t get_dp_count() const
    {
        return m_dp_count;
    }

    inline Timestamp get_last_tstamp() const
    {
        return m_prev_tstamp;
    }

    inline int get_version() const override
    {
        return 2;
    };

    virtual bool recycle();

private:
    friend class Compressor;

    Compressor_v2();
    void compress1(Timestamp timestamp, double value);
    void uncompress(DataPointVector& dps, bool restore);

    BitSet m_bitset;
    uint16_t m_dp_count;

    Timestamp m_prev_delta;
    Timestamp m_prev_tstamp;
    double m_prev_value;
    uint8_t m_prev_leading_zeros;
    uint8_t m_prev_trailing_zeros;
    uint8_t m_prev_none_zeros;
    bool m_is_full;
};


// This is a variation of Facebook's Gorilla compression algorithm.
class Compressor_v1 : public Compressor
{
public:
    void init(Timestamp start, uint8_t *base, size_t size);
    void restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base);
    inline void rebase(uint8_t *base);

    inline void save(CompressorPosition& position)
    {
        position.m_offset = m_cursor - m_base;
        position.m_start = 0;   // we don't use position.m_start
    }

    inline void save(uint8_t *base)
    {
        ASSERT(base != nullptr);
        ASSERT(m_base != nullptr);
        if (base != m_base) memcpy(base, m_base, (m_cursor-m_base));
    }

    // return true if sucessfully added the dp;
    // return false if the buffer is full;
    bool compress(Timestamp timestamp, double value);
    virtual bool recycle();

    // pass nullptr to this function to restore m_prev_xxx;
    inline void uncompress(DataPointVector& dps)
    {
        uncompress(dps, false);
    }

    inline bool is_full() const
    {
        return m_is_full;
    }

    inline bool is_empty() const
    {
        return (m_base == m_cursor);
    }

    inline size_t size() const
    {
        return m_cursor - m_base;
    }

/*
    inline Timestamp get_start_tstamp() const
    {
        return m_start_tstamp;
    }
*/

    inline Timestamp get_last_tstamp() const
    {
        return m_prev_tstamp;
    }

    inline int get_version() const override
    {
        return 1;
    };

    inline uint16_t get_dp_count() const
    {
        return m_dp_count;
    }

private:
    friend class Compressor;

    Compressor_v1();
    void compress1(Timestamp timestamp, double value);
    void uncompress(DataPointVector& dps, bool restore);

    uint8_t *m_base;
    size_t m_size;
    uint8_t *m_cursor;

    Timestamp m_prev_delta;
    Timestamp m_prev_tstamp;
    double m_prev_value;
    bool m_is_full;
    uint16_t m_dp_count;
};


// this one does not do compression at all
class Compressor_v0 : public Compressor
{
public:
    void init(Timestamp start, uint8_t *base, size_t size);
    void restore(DataPointVector& dps, CompressorPosition& position, uint8_t *base);
    void save(uint8_t *base);                       // save data

    inline void rebase(uint8_t *base)
    {
        m_data_points = reinterpret_cast<DataPointPair*>(base);
    }

    inline void save(CompressorPosition& position)  // save meta
    {
        position.m_offset = m_dps.size();
        position.m_start = 0;   // not used, but should be set to 0
    }

    // return true if sucessfully added the dp;
    // return false if the buffer is full;
    bool compress(Timestamp timestamp, double value);
    void uncompress(DataPointVector& dps);
    virtual bool recycle();

    inline bool is_full() const
    {
        return (m_dps.size() >= m_size);
    }

    inline bool is_empty() const
    {
        return m_dps.empty();
    }

    inline size_t size() const  // return number of bytes
    {
        return (m_dps.size() * sizeof(DataPointPair));
    }

    inline int get_version() const override
    {
        return 0;
    };

    inline uint16_t get_dp_count() const
    {
        return m_dps.size();
    }

    Timestamp get_last_tstamp() const;

private:
    size_t m_size;  // capacity of m_dps (number of dps)
    DataPointPair *m_data_points;
    DataPointVector m_dps;
};


}
