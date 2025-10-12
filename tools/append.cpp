/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include <iostream>
#include "compress.h"
#include "page.h"


using namespace tt;


int
main(int argc, char *argv[])
{
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <append.log>\n", argv[0]);
        return 1;
    }

    uint8_t buff[65536];
    size_t header_size = sizeof(struct append_log_entry);
    FILE *file = fopen(argv[1], "rb");
    uint8_t page[4096];
    Compressor *(compressors[4]);
    Compressor *compressor;

    for (int v = 0; v < 4; v++)
        compressors[v] = Compressor::create(v);

    for ( ; ; )
    {
        size_t size = fread(buff, header_size, 1, file);
        if (size < 1) break;

        MetricId mid = ((struct append_log_entry*)buff)->mid;
        TimeSeriesId tid = ((struct append_log_entry*)buff)->tid;
        Timestamp tstamp = ((struct append_log_entry*)buff)->tstamp;
        PageSize offset = ((struct append_log_entry*)buff)->offset;
        uint8_t start = ((struct append_log_entry*)buff)->start;
        uint8_t flags = ((struct append_log_entry*)buff)->flags;
        FileIndex file_idx = ((struct append_log_entry*)buff)->file_idx;
        HeaderIndex header_idx = ((struct append_log_entry*)buff)->header_idx;

        int compressor_version = flags & 0x03;
        bool is_ooo = ((flags & 0x80) == 0x80);
        ASSERT(0 <= compressor_version && compressor_version <= 3);
        ASSERT(!is_ooo || (compressor_version == 0));

        int bytes = offset;

        if ((flags & 0x03) == 0)    // version 0 compressor
            bytes *= sizeof(DataPointPair);
        else if (start != 0)
            bytes++;

        size = ::fread(buff, bytes, 1, file);

        if (size < 1)
        {
            std::cerr << "[ERROR] Truncated append log" << std::endl;
            break;
        }

        std::cout << "page: mid=" << mid
                  << ", tid=" << tid
                  << ", tstamp=" << tstamp
                  << ", offset=" << offset
                  << ", start=" << (unsigned int)start
                  << ", is_ooo=" << is_ooo
                  << ", comp_ver=" << compressor_version
                  << ", file_idx=" << file_idx
                  << ", header_idx=" << header_idx
                  << std::endl;

        compressor = compressors[compressor_version];
        CompressorPosition position(offset, start);
        compressor->init(tstamp, page, 4096);
        //compressor->set_start_tstamp(tstamp);
        DataPointVector dps;
        compressor->restore(dps, position, (uint8_t*)buff);

        //for (int i = 0; i < offset; i++)
        for (auto dp: dps)
        {
            //uint32_t ts = dp.first - tstamp;
            std::cout << "  [" << dp.first << ", " << dp.second << "]" << std::endl;
        }
    }

    for (int v = 0; v < 4; v++)
        delete compressors[v];

    return 0;
}
