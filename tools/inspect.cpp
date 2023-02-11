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
#include <iostream>
#include <locale>
#include <mutex>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "compress.h"
#include "mmap.h"
#include "page.h"
#include "task.h"
#include "tsdb.h"
#include "type.h"
#include "utils.h"


using namespace tt;

std::string g_tsdb_dir, g_data_dir;
char *g_index_base = nullptr;
std::vector<TimeSeries*> g_time_series;
std::atomic<uint64_t> g_total_dps_cnt{0};
std::atomic<long> g_total_page_cnt{0};
bool g_verbose = false;
std::atomic<bool> g_new_line{false};
std::mutex g_mutex; // protect std::cerr
TaskScheduler inspector("inspector", std::thread::hardware_concurrency()+1, 128);


void
find_matching_files(std::string& pattern, std::vector<std::string>& files)
{
    glob_t result;

    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);

    for (unsigned int i = 0; i < result.gl_pathc; i++)
        files.push_back(std::string(result.gl_pathv[i]));

    globfree(&result);
    std::sort(files.begin(), files.end());
}

char *
open_mmap(const std::string& file_name, int& fd, size_t& size)
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

    while ((c = getopt(argc, argv, "?d:t:v")) != -1)
    {
        switch (c)
        {
            case 'd':
                g_data_dir.assign(optarg);
                break;

            case 't':
                g_tsdb_dir.assign(optarg);
                break;

            case 'v':
                g_verbose = true;
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

uint64_t
inspect_page(FileIndex file_idx, HeaderIndex header_idx, struct tsdb_header *tsdb_header, struct page_info_on_disk *page_header, char *data_base)
{
    uint64_t page_dps = 0;
    int compressor_version =
        page_header->is_out_of_order() ? 0 : tsdb_header->get_compressor_version();
    g_tstamp_resolution_ms = tsdb_header->is_millisecond();

    char *page_base = data_base + (page_header->m_page_index * tsdb_header->m_page_size) + page_header->m_offset;
    ASSERT(page_header->m_page_index <= tsdb_header->m_page_index);

    // dump page header
    if (g_verbose)
    {
        printf("     [%u,%u][offset=%u,size=%u,cursor=%u,start=%u,flags=%x,page-idx=%u,from=%u,to=%u,next-file=%u,next-header=%u]\n",
            file_idx,
            header_idx,
            page_header->m_offset,
            page_header->m_size,
            page_header->m_cursor,
            page_header->m_start,
            page_header->m_flags,
            page_header->m_page_index,
            page_header->m_tstamp_from,
            page_header->m_tstamp_to,
            page_header->m_next_file,
            page_header->m_next_header);
    }

    // dump page data
    DataPointVector dps;
    Compressor *compressor = Compressor::create(compressor_version);
    CompressorPosition position(page_header);
    compressor->init(tsdb_header->m_start_tstamp, (uint8_t*)page_base, tsdb_header->m_page_size);
    compressor->restore(dps, position, nullptr);
    if (g_verbose)
    {
        for (auto& dp: dps)
        {
            printf("ts = %" PRIu64 ", value = %.3f\n", dp.first, dp.second);
            page_dps++;
        }
    }
    else
        page_dps = dps.size();
    delete compressor;
    g_total_page_cnt++;
    //g_total_dps_cnt.fetch_add(page_dps, std::memory_order_relaxed);
    return page_dps;
}

void
inspect_index_file(const std::string& file_name)
{
    int index_file_fd;
    size_t index_file_size;
    char *index_base = open_mmap(file_name, index_file_fd, index_file_size);
    struct index_entry *index_entries = (struct index_entry*)index_base;

    for (TimeSeries *ts: g_time_series)
    {
        TimeSeriesId id = ts->get_id();
        printf("%u:%u:%u\n", id, index_entries[id].file_index, index_entries[id].header_index);
    }

    close_mmap(index_file_fd, index_base, index_file_size);
}

void
inspect_tsdb_internal(const std::string& dir)
{
    {
        std::lock_guard<std::mutex> guard(g_mutex);
        if (g_new_line)
        {
            std::cerr << std::endl;
            g_new_line = false;
        }
        std::cerr << "Inspecting tsdb " << dir << "..." << std::endl;
    }

    // dump all headers first...
    uint64_t tsdb_dps = 0;
    std::string header_files_pattern = dir + "/header.*";
    std::vector<std::string> header_files;
    find_matching_files(header_files_pattern, header_files);

    for (std::string& header_file: header_files)
    {
        int header_fd;
        size_t header_size;

        char *base_hdr = open_mmap(header_file, header_fd, header_size);

        struct tsdb_header *tsdb_header = (struct tsdb_header*)base_hdr;
        if (g_verbose)
        {
            printf("%s: [major=%u, minor=%u, flags=%x, page_cnt=%u, header_idx=%u, page_idx=%u, start=%" PRIu64 ", end=%" PRIu64 ", actual=%u, size=%u]\n",
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
        }

        close_mmap(header_fd, base_hdr, header_size);
    }

    std::string index_file_name = dir + "/index";
    int index_file_fd;
    size_t index_file_size;
    char *index_base = open_mmap(index_file_name, index_file_fd, index_file_size);
    struct index_entry *index_entries = (struct index_entry*)index_base;
    long ooo_page_cnt = 0;

    for (TimeSeries *ts: g_time_series)
    {
        TimeSeriesId id = ts->get_id();

        if (((id+1) * sizeof(struct index_entry)) >= index_file_size) continue;
        if (index_entries[id].file_index == TT_INVALID_FILE_INDEX) continue;

        if (g_verbose)
            printf("ts-id = %u\n", id);
        //inspect_page(dir, index_entries[id].file_index, index_entries[id].header_index);

        FileIndex file_idx = index_entries[id].file_index;
        HeaderIndex header_idx = index_entries[id].header_index;

        int header_file_fd = -1;
        int data_file_fd = -1;
        size_t header_file_size, data_file_size;
        char *header_base, *data_base;
        struct tsdb_header *tsdb_header;

        while ((file_idx != TT_INVALID_FILE_INDEX) && (header_idx != TT_INVALID_HEADER_INDEX))
        {
            if (header_file_fd < 0)
            {
                char header_file_name[PATH_MAX];
                char data_file_name[PATH_MAX];

                snprintf(header_file_name, sizeof(header_file_name), "%s/header.%u", dir.c_str(), file_idx);
                snprintf(data_file_name, sizeof(data_file_name), "%s/data.%u", dir.c_str(), file_idx);

                std::string header_file_name_str(header_file_name);
                std::string data_file_name_str(data_file_name);

                header_base = open_mmap(header_file_name_str, header_file_fd, header_file_size);
                data_base = open_mmap(data_file_name_str, data_file_fd, data_file_size);

                tsdb_header = (struct tsdb_header*)header_base;
            }

            if ((header_base == nullptr) || (data_base == nullptr) || (header_file_fd < 0))
            {
                printf("[ERROR] failed to open file\n");
                continue;
            }

            struct page_info_on_disk *page_header = (struct page_info_on_disk *)
                (header_base + sizeof(struct tsdb_header) + header_idx * sizeof(struct page_info_on_disk));

            if (page_header->is_out_of_order()) ooo_page_cnt++;

            tsdb_dps = inspect_page(file_idx, header_idx, tsdb_header, page_header, data_base);
            g_total_dps_cnt.fetch_add(tsdb_dps, std::memory_order_relaxed);

            if (file_idx != page_header->m_next_file)
            {
                file_idx = page_header->m_next_file;
                header_idx = page_header->m_next_header;

                close_mmap(data_file_fd, data_base, data_file_size);
                close_mmap(header_file_fd, header_base, header_file_size);

                data_file_fd = -1;
                header_file_fd = -1;
            }
            else
            {
                header_idx = page_header->m_next_header;
            }
        }
    }

    close_mmap(index_file_fd, index_base, index_file_size);
    //g_total_dps_cnt.fetch_add(tsdb_dps, std::memory_order_relaxed);

    //if (ooo_page_cnt > 0)
        //std::cerr << "tsdb dps = " << tsdb_dps << "; pages = " << g_total_page_cnt.load() << "; total dps = " << g_total_dps_cnt.load() << "; ooo pages = " << ooo_page_cnt << std::endl;
    //else
        //std::cerr << "tsdb dps = " << tsdb_dps << "; pages = " << g_total_page_cnt.load() << "; total dps = " << g_total_dps_cnt.load() << std::endl;
    //fprintf(stderr, "tsdb dps = %" PRIu64 "; total dps = %" PRIu64 "\n",
        //tsdb_dps, g_total_dps_cnt);
}

bool
inspect_tsdb_task(TaskData& data)
{
    std::string tsdb_dir((char*)data.pointer);
    inspect_tsdb_internal(tsdb_dir);
    return false;
}

void
inspect_tsdb(const std::string& dir)
{
    Task task;

    task.doit = inspect_tsdb_task;
    task.data.pointer = (void*)strdup(dir.c_str());

    inspector.submit_task(task);
}

int
main(int argc, char *argv[])
{
    if (process_cmdline_opts(argc, argv) != 0)
        return 1;

    if (g_data_dir.empty() && g_tsdb_dir.empty())
    {
        fprintf(stderr, "-d <data-dir> or -t <tsdb-dir> option is required and missing\n");
        return 2;
    }

    //std::locale loc("en_US.UTF-8");
    std::locale loc("");
    std::cerr.imbue(loc);

    MemoryManager::init();

    if (! g_data_dir.empty())
    {
        Config::set_value(CFG_TSDB_DATA_DIR, g_data_dir);
        MetaFile::init(Tsdb::restore_ts, Tsdb::restore_measurement);
        Tsdb::get_all_ts(g_time_series);
        std::cerr << "Total number of time series: " << g_time_series.size() << std::endl;
    }

    uint64_t total_cnt = 0;

    if (g_tsdb_dir.empty())
    {
        // data directory structure:
        // <year>/<month>/<tsdb>/<index>|<header>|<data>
        //for_all_dirs(g_data_dir, inspect_tsdb, 3);
        for_all_dirs(g_data_dir, inspect_tsdb, 3);

        std::vector<size_t> counts;
        while ((inspector.get_pending_task_count(counts) > 0) ||
               (total_cnt != g_total_dps_cnt.load()))
        {
            total_cnt = g_total_dps_cnt.load();
            counts.clear();
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::lock_guard<std::mutex> guard(g_mutex);
            g_new_line = true;
            std::cerr << "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\bTotal dps = " << g_total_dps_cnt.load();
        }

        std::lock_guard<std::mutex> guard(g_mutex);
        if (g_new_line.load())
            std::cerr << std::endl;
    }
    else
    {
        //std::string index_file_name = g_tsdb_dir + "/index";
        //inspect_index_file(index_file_name);
        inspect_tsdb_internal(g_tsdb_dir);
    }

    inspector.shutdown();
    inspector.wait(1);

    //fprintf(stderr, "Grand Total = %" PRIu64 "\n", g_total_dps_cnt);
    std::cerr << "Grand Total = " << g_total_dps_cnt.load() << std::endl;

    return 0;
}
