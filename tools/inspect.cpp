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

#include <cstdio>
#include <vector>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "compress.h"
#include "page.h"
#include "type.h"
#include "utils.h"


using namespace tt;

int g_dump_all = false;
int g_dump_data = -1;
int g_dump_info = false;
std::string g_data_file;


void
find_matching_files(std::string& pattern, std::vector<std::string>& files)
{
    glob_t result;

    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);

    for (unsigned int i = 0; i < result.gl_pathc; i++)
    {
        files.push_back(std::string(result.gl_pathv[i]));
    }

    globfree(&result);
}

char *
open_mmap(std::string& file_name, int& fd, size_t& size)
{
    struct stat sb;

    fd = open(file_name.c_str(), O_RDONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

    if (fd == -1)
    {
        printf("failed to open file %s, errno = %d\n", file_name.c_str(), errno);
        return nullptr;
    }

    if (fstat(fd, &sb) == -1)
    {
        printf("failed to fstat file %s, errno = %d\n", file_name.c_str(), errno);
        return nullptr;
    }

    if (0 == sb.st_size)
    {
        printf("fstat returned 0 size\n");
        return nullptr;
    }

    size = sb.st_size;

    char *base = (char*) mmap64(nullptr,
                                size,
                                PROT_READ,
                                MAP_PRIVATE,
                                fd,
                                0);

    if (base == MAP_FAILED)
    {
        printf("failed to mmap64, errno = %d\n", errno);
        return nullptr;
    }

    return base;
}

void
close_mmap(int fd, char *base, size_t size)
{
    if (base != nullptr) munmap(base, size);
    if (fd != -1) close(fd);
}

void
dump_tsdb_header(struct tsdb_header *tsdb_header)
{
    printf("TSDB: (major=%d, minor=%d, page_cnt=%d, head_idx=%d, page_idx=%d, start=%" PRIu64 ", end=%" PRIu64 ", actual_cnt=%d, flags=0x%x)\n",
        tsdb_header->m_major_version, tsdb_header->m_minor_version,
        tsdb_header->m_page_count, tsdb_header->m_header_index,
        tsdb_header->m_page_index, tsdb_header->m_start_tstamp,
        tsdb_header->m_end_tstamp, tsdb_header->m_actual_pg_cnt, tsdb_header->m_flags);
    g_tstamp_resolution_ms = tsdb_header->is_millisecond();
}

void
dump_page_info_headers(struct tsdb_header *tsdb_header)
{
    struct page_info_on_disk *page_infos =
        (struct page_info_on_disk *) ((char*)tsdb_header + sizeof(struct tsdb_header));

    float total_util = 0.0;

    for (int i = 0; i < tsdb_header->m_header_index; i++)
    {
        struct page_info_on_disk *info = &page_infos[i];
        PageSize cursor = info->m_cursor;
        if (info->m_start > 0) cursor++;
        float util = (float)cursor/(float)info->m_size;
        printf("INFO(%4d): (offset=%d, size=%d, cursor=%d, start=%d, page_idx=%d, from=%d, to=%d, flags=%x, pctused=%.2f)\n",
            i, info->m_offset, info->m_size, info->m_cursor, info->m_start,
            info->m_page_index, info->m_tstamp_from, info->m_tstamp_to, info->m_flags, util);
        total_util += util;
    }

    if (tsdb_header->m_header_index != 0)
        printf("Average page utilization = %.2f\n", total_util/tsdb_header->m_header_index);
}

void
dump_data(struct tsdb_header *tsdb_header, int header_index)
{
    struct page_info_on_disk *page_infos =
        (struct page_info_on_disk *) ((char*)tsdb_header + sizeof(struct tsdb_header));

    struct page_info_on_disk *info = &page_infos[header_index];
    int page_idx = info->m_page_index;
    int version = info->is_out_of_order() ? 0 : tsdb_header->get_compressor_version();
    Compressor *compressor = Compressor::create(version);
    uint8_t *base = ((uint8_t*)tsdb_header) + page_idx*g_page_size + info->m_offset;
    compressor->init(tsdb_header->m_start_tstamp, base, info->m_size);
    CompressorPosition position(info);
    DataPointVector dps;
    compressor->restore(dps, position, nullptr);
    printf("dps.size() == %d, pos.offset = %d, pos.start = %d, index = %d, ooo = %s, range = (%u, %u)\n",
        (int)dps.size(), position.m_offset, position.m_start, header_index, info->is_out_of_order()?"true":"false",
        info->m_tstamp_from, info->m_tstamp_to);
    for (auto& dp: dps) printf("ts = %" PRIu64 ", value = %.2f\n", dp.first, dp.second);
    delete compressor;
}

void
dump_all_data(struct tsdb_header *tsdb_header)
{
    for (int i = 0; i < tsdb_header->m_header_index; i++)
    {
        dump_data(tsdb_header, i);
    }
}

void
inspect(char *base)
{
    struct tsdb_header *tsdb_header = (struct tsdb_header *) base;

    dump_tsdb_header(tsdb_header);

    //if (g_dump_info || g_dump_all)
    if (g_dump_info)
    {
        dump_page_info_headers(tsdb_header);
    }

    if (g_dump_all)
    {
        dump_all_data(tsdb_header);
    }
    else if (g_dump_data >= 0)
    {
        dump_data(tsdb_header, g_dump_data);
    }
}

static int
process_cmdline_opts(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "?ad:hp:")) != -1)
    {
        switch (c)
        {
            case 'a':
                g_dump_all = true;
                break;

            case 'd':
                g_data_file.assign(optarg);
                break;

            case 'h':
                g_dump_info = true;
                break;

            case 'p':
                g_dump_data = std::atoi(optarg);
                break;

            case '?':
                if (optopt == 'd')
                {
                    fprintf(stderr, "Option -d requires a data file (or pattern).\n");
                }
                else if (optopt == 'p')
                {
                    fprintf(stderr, "Option -p requires a page header index.\n");
                }
                else
                {
                    fprintf(stderr, "Usage: %s [-ah] [-p <header-index>] [-d] <data_file>\n", argv[0]);
                }
                return 1;
            default:
                return 2;
        }
    }

    if ((optind + 1) == argc)
    {
        g_data_file.assign(argv[optind]);
    }

    return 0;
}

int
main(int argc, char *argv[])
{
    if (process_cmdline_opts(argc, argv) != 0)
        return 1;

    if (g_data_file.empty())
    {
        fprintf(stderr, "-d <data-file> option is required and missing\n");
        return 2;
    }

    std::string data_file_pattern(g_data_file);

    std::vector<std::string> files;
    find_matching_files(data_file_pattern, files);

    for (std::string& file: files)
    {
        if (ends_with(file, ".meta") || ends_with(file, ".part"))
            continue;

        printf("Inspecting %s...\n", file.c_str());

        int fd;
        size_t size;
        char *base = open_mmap(file, fd, size);

        if (base != nullptr)
            inspect(base);

        close_mmap(fd, base, size);
    }

    return 0;
}
