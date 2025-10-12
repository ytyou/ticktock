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
#include "limit.h"
#include "mmap.h"
#include "page.h"
#include "task.h"
#include "tsdb.h"
#include "type.h"
#include "utils.h"


// options
bool g_flush = false;       // flush after every writes?
bool g_verbose = false;
uint64_t g_block_size = 4096;
uint64_t g_io_size = 65536; // number of blocks
std::string g_mode;
std::string g_input_file_name { "/tmp/iobench.dat" };
std::string g_output_file_name { "/tmp/iobench.dat" };

uint8_t *g_block = nullptr;
int *g_order = nullptr;


static bool
is_mode_read()
{
    return g_mode[0] == 'r' || g_mode[0] == 'b';
}

static bool
is_mode_write()
{
    return g_mode[0] == 'w' || g_mode[0] == 'b';
}

static bool
is_mode_write_only()
{
    return is_mode_write() && !is_mode_read();
}

static bool
is_mode_mmap()
{
    return g_mode[1] == 'm';
}

static bool
is_mode_random()
{
    return g_mode[2] == 'r';
}

static bool
is_mode_sequential()
{
    return g_mode[2] == 's';
}

static bool
is_mode_forward()
{
    return g_mode.length() < 4 || g_mode[3] == 'f';
}

static bool
is_mode_backward()
{
    return g_mode.length() == 4 && g_mode[3] == 'b';
}


class TestFile : public tt::MmapFile
{
public:
    TestFile(const std::string& file_name) :
        MmapFile(file_name),
        m_file(nullptr)
    {
    }

    ~TestFile()
    {
        this->close();
    }

    void open(bool read_only) override
    {
        if (is_mode_mmap())
        {
            if (tt::file_exists(m_name))
                tt::MmapFile::open_existing(read_only, is_mode_sequential());
            else
                tt::MmapFile::open(g_io_size*g_block_size, read_only, is_mode_sequential(), true);
        }
        else
        {
            // write mode
            int fd =
                ::open(g_output_file_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

            if (fd > 0)
                m_file = fdopen(fd, "ab");
            else
            {
                std::cerr << "[ERROR] Failed to open "
                          << g_output_file_name
                          << " for write (errno="
                          << errno
                          << ")"
                          << std::endl;
            }
        }
    }

    void close() override
    {
        if (m_file != nullptr)
        {
            std::fclose(m_file);
            m_file = nullptr;
        }

        tt::MmapFile::close();
    }

    void flush(bool sync) override
    {
        if (m_file != nullptr)
            std::fflush(m_file);

        tt::MmapFile::flush(sync);
    }

    void write_block(int blk)
    {
        uint8_t *pages = (uint8_t*)get_pages() + blk * g_block_size;
        std::memcpy((void*)pages, (void*)g_block, g_block_size);
        if (g_flush) flush(true);
    }

    void append_block()
    {
        std::fwrite((void*)g_block, g_block_size, 1, m_file);
        if (g_flush) flush(true);
    }

    void read_block(int blk, volatile uint8_t *block)
    {
        uint8_t *pages = (uint8_t*)get_pages() + blk * g_block_size;
        std::memcpy((void*)block, (void*)pages, g_block_size);
    }

private:
    FILE *m_file;
};


static int
cmdline_options(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "?b:fi:m:o:s:v")) != -1)
    {
        switch (c)
        {
            case 'b':
                g_block_size = tt::Property::as_bytes(std::string(optarg));
                std::cerr << "[INFO] Using block-size " << g_block_size << std::endl;
                break;

            case 'f':
                g_flush = true;
                std::cerr << "[INFO] Flush (after write) ON" << std::endl;
                break;

            case 'i':
                g_input_file_name.assign(optarg);
                break;

            case 'm':
                g_mode.assign(optarg);
                break;

            case 'o':
                g_output_file_name.assign(optarg);
                break;

            case 's':
                g_io_size = std::stoull(std::string(optarg));
                std::cerr << "[INFO] Using io-size-in-blocks: "
                          << g_io_size
                          << std::endl;
                break;

            case 'v':
                g_verbose = true;
                std::cerr << "[INFO] Verbose mode ON" << std::endl;
                break;

            case '?':
            default:
                std::cerr << "Usage: "
                          << argv[0]
                          << " [-b <block-size>]"
                          << " [-f (flush)]"
                          << " [-i <input-file>]"
                          << " [-m <mode>]"
                          << " [-o <output-file>]"
                          << " [-s <io-size-in-blocks>]"
                          << " [-v (verbose)]"
                          << std::endl;
                return 1;
        }
    }

    // validate options
    if (g_mode.empty())
    {
        std::cerr << "[ERROR] cmdline option '-m <mode>' is required" << std::endl;
        return 2;
    }
    else if ((g_mode.compare("?") == 0) ||
             (g_mode[0] != 'r' && g_mode[0] != 'w' && g_mode[0] != 'b') ||
             (g_mode[1] != 'm' && g_mode[1] != 'r') ||
             (g_mode[2] != 'r' && g_mode[2] != 's') ||
             (g_mode.length() == 4 && g_mode[3] != 'f' && g_mode[3] != 'b') ||
             (g_mode.length() > 4))
    {
        std::cerr << "Supported mode: (e.g.: wa)" << std::endl
                  << " 1st char: r|w|b  (read|write|both)" << std::endl
                  << " 2nd char: m|r    (mmap|regular)" << std::endl
                  << " 3nd char: r|s    (random|sequential)" << std::endl
                  << " 4th char: f|b    (forward|backward)" << std::endl;
        return 1;
    }

    if (is_mode_write_only() && is_mode_backward())
    {
        std::cerr << "[ERROR] Writing backwards is not supported" << std::endl;
        return 1;
    }

    return 0;
}

static void
perform_write()
{
    if (g_verbose)
        std::cerr << "Perform write..." << std::endl;

    TestFile test_file(g_output_file_name);
    test_file.open(false);

    if (is_mode_random())
    {
        for (int i = 0; i < g_io_size; i++)
            test_file.write_block(g_order[i]);
    }
    else    // sequential
    {
        if (is_mode_mmap())
        {
            if (is_mode_forward())
            {
                for (int i = 0; i < g_io_size; i++)
                    test_file.write_block(i);
            }
            else    // backwards
            {
                for (int i = g_io_size-1; i >= 0; i--)
                    test_file.write_block(i);
            }
        }
        else
        {
            // forward direction is assumed
            for (int i = 0; i < g_io_size; i++)
                test_file.append_block();
        }
    }

    test_file.flush(true);
}

static void
perform_read()
{
    if (g_verbose)
        std::cerr << "Perform read..." << std::endl;

    TestFile test_file(g_input_file_name);
    test_file.open(true);

    if (is_mode_random())
    {
        volatile uint8_t block[g_block_size];

        for (int i = 0; i < g_io_size; i++)
            test_file.read_block(g_order[i], block);
    }
    else    // sequential
    {
        if (is_mode_forward())
        {
            volatile uint8_t block[g_block_size];

            for (int i = 0; i < g_io_size; i++)
                test_file.read_block(i, block);
        }
        else    // backwards
        {
            volatile uint8_t block[g_block_size];

            for (int i = g_io_size-1; i >= 0; i--)
                test_file.read_block(i, block);
        }
    }
}


int
main(int argc, char *argv[])
{
    if (cmdline_options(argc, argv) != 0)
        return 1;

    if (g_verbose)
        std::cerr << "Seting up tests..." << std::endl;

    g_block = (uint8_t*)std::malloc(g_block_size);

    for (int i = 0; i < g_block_size; i++)
        g_block[i] = i % UINT8_MAX;

    g_order = (int*)std::malloc(g_io_size*sizeof(int));

    if (is_mode_random())
    {
        for (int i = 0; i < g_io_size; i++)
            g_order[i] = i;

        // shuffle
        for (int i = 0; i < 8*g_io_size; i++)
        {
            int x = tt::random(0, g_io_size-1);
            int y = tt::random(0, g_io_size-1);
            std::swap(g_order[x], g_order[y]);
        }
    }

    if (is_mode_write())
        tt::rm_file(g_output_file_name);

    // perform tests
    long long t0 = tt::ts_now_ms();

    if (is_mode_write())
        perform_write();

    long long t1 = tt::ts_now_ms();

    if (is_mode_read())
        perform_read();

    long long t2 = tt::ts_now_ms();

    // display stats
    if (is_mode_write())
        std::cerr << "Write Time: " << (t1-t0) << "ms" << std::endl;
    if (is_mode_read())
        std::cerr << "Read Time : " << (t2-t1) << "ms" << std::endl;

    std::free(g_block);
    std::free(g_order);
    return 0;
}
