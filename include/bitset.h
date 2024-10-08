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

#include "recycle.h"
#include "serial.h"


namespace tt
{


class BitSet;


/* This is used to traverse a BitSet.
 */
class BitSetCursor : public Recyclable
{
private:
    friend class BitSet;

    void init() override;
    void init(BitSet *bitset);

    uint8_t *m_cursor;
    uint8_t m_start;
};


/* This class enables us to store data as series of bits. It is used by
 * the Gorilla compression algorithm.
 */
class __attribute__ ((__packed__)) BitSet
{
public:
    BitSet();
    BitSet(size_t size_in_bits);
    ~BitSet();

    void init(uint8_t *base, size_t capacity_in_bytes);
    void recycle();
    void rebase(uint8_t *base);
    BitSetCursor *new_cursor();

    void set(size_t idx);
    bool test(size_t idx);

    void reset();
    size_t capacity_in_bytes() const;

    // append 'len' of bits stored in 'bits', starting at
    // offset 'start'; return true if successful, return
    // false if there's not enough space left in the buffer;
    void append(uint8_t *bits, uint8_t len, uint8_t start);

    // write content to a file (append.log)
    int append(FILE *file);

    // this is the opposite of append(); it will retrieve
    // 'len' bits from this bitset starting at 'cursor';
    // 'start' indicates where in the first byte of the
    // destination 'bits' we should store the retrieved bits;
    void retrieve(BitSetCursor *cursor, uint8_t *bits, uint8_t len, uint8_t start);

    // save the current endpoint, which can be used to rollback future append() operations
    inline void save_check_point()
    {
        m_cp_cursor = m_cursor;
        m_cp_start = m_start;
    }

    // rollback to previously saved endpoint
    inline void restore_from_check_point()
    {
        m_cursor = m_cp_cursor;
        m_start = m_cp_start;
    }

    void copy_to(uint8_t *base) const;
    void copy_from(uint8_t *base, int bytes, uint8_t start);

    inline size_t size_in_bits() const
    {
        return 8 * (m_cursor - m_bits) + m_start;
    }

    inline size_t size_in_bytes() const
    {
        size_t size = (m_cursor - m_bits);
        if (m_start != 0) size++;
        return size;
    }

    inline size_t avail_capacity_in_bits() const
    {
        return m_capacity_in_bytes * 8 - size_in_bits();
    }

    inline size_t avail_capacity_in_bytes() const
    {
        return m_capacity_in_bytes - size_in_bytes();
    }

    inline bool is_empty() const
    {
        return (m_cursor == m_bits) && (m_start == 0);
    }

    //inline size_t c_size() const override { return 128; }
    //const char *c_str(char *buff) const override;

private:
    friend class BitSetCursor;

    // append some/all bits in the byte 'bits', staring at offset 'start';
    // 'len' indicates how many bits should be appended; it could be bigger
    // than what's available in 'bits', and we just need to update it to
    // reflect how many bits we did append;
    void append(uint8_t bits, uint8_t& len, uint8_t& start);

    // this is the opposite of the append(); it will retrieve
    // some/all bits in the first byte pointed to by 'cursor',
    // and copy them into 'byte' starting at 'start';
    void retrieve(BitSetCursor *cursor, uint8_t& byte, uint8_t& len, uint8_t& start);

    uint8_t *m_bits;        // beginning of this bitset
    size_t m_capacity_in_bytes;

    uint8_t *m_cursor;      // the byte to store next new bit
    uint8_t *m_end;         // the byte after the last byte in this bitset;
                            // when m_cursor == m_end, the bitset is full;

    uint8_t *m_cp_cursor;   // for saving check point so we can roll back to it
    uint8_t m_cp_start;     // for saving check point so we can roll back to it

    uint8_t m_start;        // offset within a byte where next new bit should go
    bool m_own_memory;      // do we own the memory?
};


// Used by PerfectHash
class BitSet64
{
public:
    BitSet64(std::size_t size); // size: no. of bits
    BitSet64(BitSet64&& src);   // move constructor
    ~BitSet64();

    inline void set(std::size_t idx)
    {
        m_bits[idx/64] |= (1ULL << (idx % 64));
    }

    void reset();
    uint64_t get64(std::size_t idx);
    uint64_t pop64(std::size_t idx);

    inline bool test(size_t idx)
    {
        return (m_bits[idx/64] & (1ULL << (idx % 64))) != 0;
    }

    inline std::size_t capacity64()   // in no. of uint64_t
    {
        return m_capacity;
    }

private:
    uint64_t *m_bits;
    std::size_t m_capacity; // in no. of uint64_t
};


}
