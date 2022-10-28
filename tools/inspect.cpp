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
#include "mmap.h"
#include "page.h"
#include "tsdb.h"
#include "type.h"
#include "utils.h"


using namespace tt;

std::string g_tsdb_dir, g_data_dir;
char *g_index_base = nullptr;
std::vector<TimeSeries*> g_time_series;


void
find_matching_files(std::string& pattern, std::vector<std::string>& files)
{
    glob_t result;

    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);

    for (unsigned int i = 0; i < result.gl_pathc; i++)
        files.push_back(std::string(result.gl_pathv[i]));

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

static int
process_cmdline_opts(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "?d:t:")) != -1)
    {
        switch (c)
        {
            case 'd':
                g_data_dir.assign(optarg);
                break;

            case 't':
                g_tsdb_dir.assign(optarg);
                break;

            case '?':
                if (optopt == 't')
                {
                    fprintf(stderr, "Option -t requires a tsdb dir.\n");
                }
                else
                {
                    fprintf(stderr, "Usage: %s [-d <data_dir>] [-t <tsdb_dir>]\n", argv[0]);
                }
                return 1;
            default:
                return 2;
        }
    }

    if ((optind + 1) == argc)
        g_tsdb_dir.assign(argv[optind]);

    return 0;
}

void
inspect_page(const std::string& dir, FileIndex file_idx, HeaderIndex header_idx)
{
    char header_file_name[PATH_MAX];
    char data_file_name[PATH_MAX];

    snprintf(header_file_name, sizeof(header_file_name), "%s/header.%u", dir.c_str(), file_idx);
    snprintf(data_file_name, sizeof(data_file_name), "%s/data.%u", dir.c_str(), file_idx);

    int header_file_fd, data_file_fd;
    size_t header_file_size, data_file_size;

    std::string header_file_name_str(header_file_name);
    std::string data_file_name_str(data_file_name);

    char *header_base = open_mmap(header_file_name_str, header_file_fd, header_file_size);
    char *data_base = open_mmap(data_file_name_str, data_file_fd, data_file_size);

    struct tsdb_header *tsdb_header = (struct tsdb_header*)header_base;
    int compressor_version = tsdb_header->get_compressor_version();
    g_tstamp_resolution_ms = tsdb_header->is_millisecond();

    struct page_info_on_disk *header = (struct page_info_on_disk *)
        (header_base + sizeof(struct tsdb_header) + header_idx * sizeof(struct page_info_on_disk));
    char *page_base = data_base + (header->m_page_index * tsdb_header->m_page_size);
    ASSERT(header->m_page_index < tsdb_header->m_page_index);

    // dump page header
    printf("     [%u,%u][offset=%u,size=%u,cursor=%u,start=%u,flags=%x,page-idx=%u,from=%u,to=%u,next-file=%u,next-header=%u]\n",
        file_idx,
        header_idx,
        header->m_offset,
        header->m_size,
        header->m_cursor,
        header->m_start,
        header->m_flags,
        header->m_page_index,
        header->m_tstamp_from,
        header->m_tstamp_to,
        header->m_next_file,
        header->m_next_header);

    // dump page data
    DataPointVector dps;
    Compressor *compressor = Compressor::create(compressor_version);
    CompressorPosition position(header);
    compressor->init(tsdb_header->m_start_tstamp, (uint8_t*)page_base, tsdb_header->m_page_size);
    compressor->restore(dps, position, nullptr);
    for (auto& dp: dps) printf("ts = %" PRIu64 ", value = %.3f\n", dp.first, dp.second);
    delete compressor;

    file_idx = header->m_next_file;
    header_idx = header->m_next_header;

    close_mmap(data_file_fd, data_base, data_file_size);
    close_mmap(header_file_fd, header_base, header_file_size);

    if ((file_idx != TT_INVALID_FILE_INDEX) && (header_idx != TT_INVALID_HEADER_INDEX))
        inspect_page(dir, file_idx, header_idx);
}

void
inspect_tsdb(const std::string& dir)
{
    printf("Inspecting tsdb %s...\n", dir.c_str());

    // dump all headers first...
    std::string header_files_pattern = dir + "/header.*";
    std::vector<std::string> header_files;
    find_matching_files(header_files_pattern, header_files);

    for (std::string& header_file: header_files)
    {
        int header_fd;
        size_t header_size;

        char *base_hdr = open_mmap(header_file, header_fd, header_size);

        struct tsdb_header *tsdb_header = (struct tsdb_header*)base_hdr;
        printf("%s: [major=%u, minor=%u, flags=%x, page_cnt=%u, header_idx=%u, page_idx=%u, start=%lu, end=%lu, actual=%u, size=%u]\n",
            header_file.c_str(),
            tsdb_header->m_major_version,
            tsdb_header->m_minor_version,
            tsdb_header->m_flags,
            tsdb_header->m_page_count,
            tsdb_header->m_header_index,
            tsdb_header->m_page_index,
            tsdb_header->m_start_tstamp,
            tsdb_header->m_end_tstamp,
            tsdb_header->m_actual_pg_cnt,
            tsdb_header->m_page_size);

        close_mmap(header_fd, base_hdr, header_size);
    }

    std::string index_file_name = dir + "/index";
    int index_file_fd;
    size_t index_file_size;
    char *index_base = open_mmap(index_file_name, index_file_fd, index_file_size);
    struct index_entry *index_entries = (struct index_entry*)index_base;

    for (TimeSeries *ts: g_time_series)
    {
        TimeSeriesId id = ts->get_id();
        if (((id+1) * sizeof(struct index_entry)) >= index_file_size) continue;
        if (index_entries[id].file_index == TT_INVALID_FILE_INDEX) continue;
        printf("%4u %s %s\n", id, ts->get_metric(), ts->get_key());
        inspect_page(dir, index_entries[id].file_index, index_entries[id].header_index);
    }

    close_mmap(index_file_fd, index_base, index_file_size);
}

int
main(int argc, char *argv[])
{
    if (process_cmdline_opts(argc, argv) != 0)
        return 1;

    if (g_data_dir.empty())
    {
        fprintf(stderr, "-d <data-dir> option is required and missing\n");
        return 2;
    }

    Config::set_value(CFG_TSDB_DATA_DIR, g_data_dir);
    MetaFile::init(Tsdb::restore_ts);
    Tsdb::get_all_ts(g_time_series);

    // data directory structure:
    // <year>/<month>/<tsdb>/<index>|<header>|<data>
    for_all_dirs(g_data_dir, inspect_tsdb, 3);

    return 0;
}
