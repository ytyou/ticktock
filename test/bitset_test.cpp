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

#include "utils.h"
#include "bitset_test.h"


using namespace tt;

namespace tt_test
{


void
BitSetTests::run()
{
    log("Running %s...", m_name);

    test1();
    test2();

    log("Finished %s", m_name);
}

void
BitSetTests::test1()
{
    uint8_t page[4096];
    BitSet bits;
    bits.init(page, sizeof(page));

    int m1 = std::rand();
    uint8_t m2 = std::rand() % 256;
    uint8_t m3 = 0x06;
    double m4 = (double)std::time(0) / (double)std::rand();
    uint8_t m5 = 0x07;
    uint8_t m6 = 0x00;
    uint16_t m7 = (uint16_t)(std::rand() % 65536);

    bits.append(reinterpret_cast<uint8_t*>(&m1), 8*sizeof(m1), 0);
    bits.append(reinterpret_cast<uint8_t*>(&m2), 8*sizeof(m2), 0);
    bits.append(reinterpret_cast<uint8_t*>(&m3), 3, 5);
    bits.append(reinterpret_cast<uint8_t*>(&m4), 8*sizeof(m4), 0);
    bits.append(reinterpret_cast<uint8_t*>(&m5), 3, 5);
    bits.append(reinterpret_cast<uint8_t*>(&m6), 1, 7);
    bits.append(reinterpret_cast<uint8_t*>(&m7), 8*sizeof(m7), 0);

    int n1;
    uint8_t n2, n3 = 0, n5 = 0, n6 = 0;
    double n4;
    uint16_t n7;
    uint8_t not_exist;

    BitSetCursor *cursor = bits.new_cursor();
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n1), 8*sizeof(n1), 0);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n2), 8*sizeof(n2), 0);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n3), 3, 5);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n4), 8*sizeof(n4), 0);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n5), 3, 5);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n6), 1, 7);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n7), 8*sizeof(n7), 0);

    try
    {
        bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&not_exist), 1, 0);
        CONFIRM(false);
    }
    catch (const std::out_of_range& ex)
    {
    }

    /**
    printf("m1 = %d, n1 = %d\n", m1, n1);
    printf("m2 = %d, n2 = %d\n", m2, n2);
    printf("m3 = %d, n3 = %d\n", m3, n3);
    printf("m4 = %f, n4 = %f\n", m4, n4);
    printf("m5 = %d, n5 = %d\n", m5, n5);
    printf("m6 = %d, n6 = %d\n", m6, n6);
    printf("m7 = %d, n7 = %d\n", m7, n7);
    **/

    CONFIRM(m1 == n1);
    CONFIRM(m2 == n2);
    CONFIRM(m3 == n3);
    CONFIRM(m4 == n4);
    CONFIRM(m5 == n5);
    CONFIRM(m6 == n6);
    CONFIRM(m7 == n7);

    m_stats.add_passed(1);
}

void
BitSetTests::test2()
{
    uint8_t page[4096];
    BitSet bits;
    bits.init(page, sizeof(page));

    uint32_t m1 = std::rand();
    double m2 = random(0.0, 1000.0);
    uint8_t m3 = 0x00;
    uint8_t m4 = 0xC0;
    uint8_t m5 = random(1,31);
    uint8_t m6 = random(1,63);
    uint64_t m7 = 0x0000001234567896;
    uint8_t m8 = 0x00;

    uint64_t m7_be = htobe64(m7);

    bits.append(reinterpret_cast<uint8_t*>(&m1), 8*sizeof(m1), 0);
    bits.append(reinterpret_cast<uint8_t*>(&m2), 8*sizeof(m2), 0);
    bits.append(reinterpret_cast<uint8_t*>(&m3), 1, 0);
    bits.append(reinterpret_cast<uint8_t*>(&m4), 2, 0);
    bits.append(reinterpret_cast<uint8_t*>(&m5), 5, 3);
    bits.append(reinterpret_cast<uint8_t*>(&m6), 6, 2);
    bits.append(reinterpret_cast<uint8_t*>(&m7_be), 51, 12);
    log("total number of bits: %d", bits.size_in_bits());
    bits.append(reinterpret_cast<uint8_t*>(&m8), 1, 0);
    log("total number of bits: %d", bits.size_in_bits());

    uint32_t n1;
    double n2;
    uint8_t n3 = 0, n4 = 0, n5 = 0, n6 = 0, n8 = 0;
    uint64_t n7 = 0, n7_be = 0;
    uint8_t not_exist;

    BitSetCursor *cursor = bits.new_cursor();
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n1), 8*sizeof(n1), 0);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n2), 8*sizeof(n2), 0);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n3), 1, 0); n3 &= 0x80;
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n4), 2, 0); n4 &= 0xC0;
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n5), 5, 3); n5 &= 0x1F;
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n6), 6, 2); n6 &= 0x3F;
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n7_be), 51, 12);
    bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&n8), 1, 0); n8 &= 0x80;

    try
    {
        bits.retrieve(cursor, reinterpret_cast<uint8_t*>(&not_exist), 1, 0);
        CONFIRM(false);
    }
    catch (const std::out_of_range& ex)
    {
    }

    n7 = htobe64(n7_be);

    CONFIRM(m1 == n1);
    CONFIRM(m2 == n2);
    CONFIRM(m3 == n3);
    CONFIRM(m4 == n4);
    CONFIRM(m5 == n5);
    CONFIRM(m6 == n6);
    CONFIRM(m7 == n7);
    CONFIRM(m8 == n8);

    m_stats.add_passed(1);
}


}
