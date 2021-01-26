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

#pragma once


namespace tt
{


class BitSet;

class BitSetCursor
{
private:
    friend class BitSet;

    BitSetCursor(BitSet *bitset);

    uint8_t *m_cursor;
    uint8_t m_start;
};


/* This class enables us to store data as series of bits. It is used by
 * the Gorilla compression algorithm.
 */
class BitSet
{
public:
    BitSet();

    void init(uint8_t *base, size_t capacity_in_bytes);
    void recycle();
    void rebase(uint8_t *base);

    inline BitSetCursor *new_cursor()
    {
        return new BitSetCursor(this);
    }

    // append 'len' of bits stored in 'bits', starting at
    // offset 'start'; return true if successful, return
    // false if there's not enough space left in the buffer;
    void append(uint8_t *bits, uint8_t len, uint8_t start);

    // this is the opposite of append(); it will retrieve
    // 'len' bits from this bitset starting at 'cursor';
    // 'start' indicates where in the first byte of the
    // destination 'bits' we should store the retrieved bits;
    void retrieve(BitSetCursor *cursor, uint8_t *bits, uint8_t len, uint8_t start);

    inline void save_check_point()
    {
        m_cp_cursor = m_cursor;
        m_cp_start = m_start;
    }

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

    const char *c_str(char *buff, size_t size) const;

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
    uint8_t m_start;        // offset within a byte where next new bit should go

    uint8_t *m_cp_cursor;   // for saving check point so we can roll back to it
    uint8_t m_cp_start;     // for saving check point so we can roll back to it
};


}
