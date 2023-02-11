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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "config.h"
#include "fd.h"
#include "global.h"
#include "logger.h"
#include "mmap.h"
#include "page.h"
#include "tsdb.h"


namespace tt
{


#define TT_INDEX_SIZE       sizeof(struct index_entry)
#define TT_SIZE_INCREMENT   (4096 * TT_INDEX_SIZE)


MmapFile::MmapFile(const std::string& file_name) :
    m_name(file_name),
    m_fd(-1),
    m_length(0),
    m_pages(nullptr)
{
}

MmapFile::~MmapFile()
{
    close();
}

void
MmapFile::open(off_t length, bool read_only, bool append_only, bool resize)
{
    ASSERT(length > 0);

    if (m_fd > 0) ::close(m_fd);
    m_read_only = read_only;
    m_length = length;

    struct stat sb;
    mode_t mode = read_only ? O_RDONLY : (O_CREAT|O_RDWR);
    m_fd = ::open(m_name.c_str(), mode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    m_fd = FileDescriptorManager::dup_fd(m_fd, FileDescriptorType::FD_FILE);

    if (m_fd == -1)
    {
        Logger::error("Failed to open file %s, errno = %d", m_name.c_str(), errno);
        if (ENOMEM == errno)
            throw std::runtime_error("Out of memory");
        return;
    }

    if (resize && ((fstat(m_fd, &sb) == -1) || (sb.st_size < m_length)))
    {
        if (ftruncate(m_fd, m_length) != 0)
        {
            Logger::error("Failed to resize file %s, errno = %d", m_name.c_str(), errno);
            return;
        }
    }

    m_pages = mmap64(nullptr,
                     m_length,
                     read_only ? PROT_READ : (PROT_READ|PROT_WRITE),
                     read_only ? MAP_PRIVATE : MAP_SHARED,
                     m_fd,
                     0);

    if (m_pages == MAP_FAILED)
    {
        Logger::error("Failed to mmap file %s, errno = %d", m_name.c_str(), errno);
        m_pages = nullptr;

        if (m_fd > 0)
        {
            ::close(m_fd);
            m_fd = -1;
        }

        if (ENOMEM == errno)
            throw std::runtime_error("Out of memory");
        return;
    }

    int rc = madvise(m_pages, m_length, append_only ? MADV_SEQUENTIAL : MADV_RANDOM);

    if (rc != 0)
        Logger::warn("Failed to madvise(), page = %p, errno = %d", m_pages, errno);

    ASSERT(is_open(read_only));
}

void
MmapFile::open_existing(bool read_only, bool append_only)
{
    if (m_fd > 0) ::close(m_fd);
    m_read_only = read_only;

    struct stat sb;
    mode_t mode = read_only ? O_RDONLY : (O_CREAT|O_RDWR);
    m_fd = ::open(m_name.c_str(), mode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    m_fd = FileDescriptorManager::dup_fd(m_fd, FileDescriptorType::FD_FILE);

    if (m_fd == -1)
    {
        Logger::error("Failed to open file %s, errno = %d", m_name.c_str(), errno);
        if (ENOMEM == errno)
            throw std::runtime_error("Out of memory");
        return;
    }

    if (fstat(m_fd, &sb) == -1)
    {
        Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
        return;
    }

    m_length = sb.st_size;

    m_pages = mmap64(nullptr,
                     m_length,
                     read_only ? PROT_READ : (PROT_READ|PROT_WRITE),
                     read_only ? MAP_PRIVATE : MAP_SHARED,
                     m_fd,
                     0);

    if (m_pages == MAP_FAILED)
    {
        Logger::error("Failed to mmap file %s, errno = %d", m_name.c_str(), errno);
        m_pages = nullptr;

        if (m_fd > 0)
        {
            ::close(m_fd);
            m_fd = -1;
        }

        if (ENOMEM == errno)
            throw std::runtime_error("Out of memory");
        return;
    }

    int rc = madvise(m_pages, m_length, append_only ? MADV_SEQUENTIAL : MADV_RANDOM);

    if (rc != 0)
        Logger::warn("Failed to madvise(), page = %p, errno = %d", m_pages, errno);

    ASSERT(is_open(read_only));
}

bool
MmapFile::resize(off_t length)
{
    ASSERT(m_fd > 0);

    if (length == m_length)
        return false;

    int rc = ftruncate(m_fd, length);

    if (rc != 0)
    {
        Logger::error("Failed to ftruncate file %s, errno = %d", m_name.c_str(), errno);
        return false;
    }

    void *pages = mremap(m_pages, m_length, length, MREMAP_MAYMOVE);

    if (pages == MAP_FAILED)
    {
        Logger::error("Failed to mremap file %s from %u to %u, errno = %d",
            m_name.c_str(), m_length, length, errno);
        return false;
    }

    m_pages = pages;
    m_length = length;
    return true;
}

void
MmapFile::close()
{
    if (m_pages != nullptr)
    {
        if (! m_read_only) flush(true);
        munmap(m_pages, m_length);
        m_pages = nullptr;
        Logger::info("closing %s", m_name.c_str());
    }

    if (m_fd > 0)
    {
        ::close(m_fd);
        m_fd = -1;
    }
}

void
MmapFile::flush(bool sync)
{
    if (m_pages == nullptr) return;

    int rc = msync(m_pages, m_length, (sync?MS_SYNC:MS_ASYNC));

    if (rc == -1)
        Logger::info("Failed to msync() file %s, errno = %d", m_name.c_str(), errno);

    rc = madvise(m_pages, m_length, MADV_DONTNEED);

    if (rc == -1)
        Logger::info("Failed to madvise(DONTNEED) file %s, errno = %d", m_name.c_str(), errno);
}

void
MmapFile::ensure_open(bool for_read)
{
    if (! is_open(for_read))
    {
        std::lock_guard<std::mutex> guard(m_lock);
        if (! is_open(for_read)) open(for_read);
    }

    ASSERT(is_open(for_read));
}

bool
MmapFile::is_open(bool for_read) const
{
    if (for_read)
        return (m_pages != nullptr);
    else
        return (m_pages != nullptr) && !is_read_only();
}


IndexFile::IndexFile(const std::string& file_name) :
    MmapFile(file_name)
{
}

void
IndexFile::open(bool for_read)
{
    bool is_new = ! exists();
    if (is_new && for_read) return;

    if (is_new)
    {
        MmapFile::open(TT_SIZE_INCREMENT, for_read, false, true);
        struct index_entry *entries = (struct index_entry*)get_pages();
        ASSERT(entries != nullptr);
        uint64_t max_idx = get_length() / TT_INDEX_SIZE;

        // TODO: memcpy()?
        for (uint64_t i = 0; i < max_idx; i++)
            entries[i].file_index = TT_INVALID_FILE_INDEX;
    }
    else
    {
        MmapFile::open_existing(for_read, false);
        ASSERT(get_pages() != nullptr);
    }

    Logger::debug("index file %s length: %" PRIu64, m_name.c_str(), get_length());
    Logger::info("opening %s for %s", m_name.c_str(), for_read?"read":"write");
}

bool
IndexFile::expand(off_t new_len)
{
    size_t old_len = get_length();
    ASSERT(old_len < new_len);
    ASSERT((old_len % TT_INDEX_SIZE) == 0);
    ASSERT((new_len % TT_INDEX_SIZE) == 0);

    if (! this->resize(new_len))
        return false;

    uint64_t old_idx = old_len / TT_INDEX_SIZE;
    uint64_t new_idx = new_len / TT_INDEX_SIZE;
    struct index_entry *entries = (struct index_entry*)get_pages();

    // TODO: memcpy()?
    for ( ; old_idx < new_idx; old_idx++)
        entries[old_idx].file_index = TT_INVALID_FILE_INDEX;

    Logger::debug("index file %s length: %" PRIu64, m_name.c_str(), get_length());

    return true;
}

bool
IndexFile::set_indices(TimeSeriesId id, FileIndex file_index, HeaderIndex header_index)
{
    void *pages = get_pages();
    ASSERT(pages != nullptr);
    ASSERT(! is_read_only());

    size_t new_len = (id+1) * TT_INDEX_SIZE;
    size_t old_len = get_length();
    ASSERT(0 < old_len);

    if (old_len < new_len)
    {
        // file too small, expand it
        if (! expand(new_len + TT_SIZE_INCREMENT))
            return false;
        pages = get_pages();
    }

    struct index_entry entry;
    entry.file_index = file_index;
    entry.header_index = header_index;
    struct index_entry *entries = (struct index_entry*)pages;
    entries[id] = entry;

    return true;
}

void
IndexFile::get_indices(TimeSeriesId id, FileIndex& file_index, HeaderIndex& header_index)
{
    void *pages = get_pages();

    size_t idx = (id+1) * TT_INDEX_SIZE;
    size_t len = get_length();

    if ((len <= idx) || (pages == nullptr))
    {
        file_index = TT_INVALID_FILE_INDEX;
        header_index = TT_INVALID_HEADER_INDEX;
    }
    else
    {
        struct index_entry *entries = (struct index_entry*)pages;
        struct index_entry entry = entries[id];
        file_index = entry.file_index;
        header_index = entry.header_index;
    }
}


HeaderFile::HeaderFile(const std::string& file_name, FileIndex id, PageCount page_count, Tsdb *tsdb) :
    MmapFile(file_name),
    m_id(id),
    m_page_count(page_count)
{
    ASSERT(page_count > 0);
    ASSERT(tsdb != nullptr);
    open(false);    // open for write
    init_tsdb_header(tsdb);
}

HeaderFile::HeaderFile(FileIndex id, const std::string& file_name) :
    MmapFile(file_name),
    m_id(id),
    m_page_count(g_page_count)
{
}

void
HeaderFile::init_tsdb_header(Tsdb *tsdb)
{
    ASSERT(tsdb != nullptr);
    struct tsdb_header *header = (struct tsdb_header*)get_pages();
    ASSERT(header != nullptr);

    int compressor_version =
        Config::get_int(CFG_TSDB_COMPRESSOR_VERSION, CFG_TSDB_COMPRESSOR_VERSION_DEF);

    header->m_major_version = TT_MAJOR_VERSION;
    header->m_minor_version = TT_MINOR_VERSION;
    header->m_flags = 0;
    header->set_compacted(false);
    header->set_compressor_version(compressor_version);
    header->set_millisecond(g_tstamp_resolution_ms);
    header->m_page_count = m_page_count;
    header->m_header_index = 0;
    header->m_page_index = 0;
    header->m_start_tstamp = tsdb->get_time_range().get_from();
    header->m_end_tstamp = tsdb->get_time_range().get_to();
    header->m_actual_pg_cnt = m_page_count;
    header->m_page_size = tsdb->get_page_size();

    ASSERT(header->m_page_count > 0);
    ASSERT(header->m_actual_pg_cnt > 0);
}

HeaderFile *
HeaderFile::restore(const std::string& file_name)
{
    FileIndex id = get_file_suffix(file_name);
    HeaderFile *header_file = new HeaderFile(id, file_name);
    //header_file->open(true);
    //struct tsdb_header *header = header_file->get_tsdb_header();
    //ASSERT(header != nullptr);
    //header_file->m_page_count = header->m_page_count;
    ASSERT(header_file->m_id != TT_INVALID_FILE_INDEX);
    return header_file;
}

void
HeaderFile::open(bool for_read)
{
    bool is_new = ! exists();
    if (is_new && for_read) return;

    if (is_new)
    {
        ASSERT(m_page_count > 0);
        off_t length =
            sizeof(struct tsdb_header) + m_page_count * sizeof(struct page_info_on_disk);
        MmapFile::open(length, for_read, false, true);
        ASSERT(get_pages() != nullptr);
    }
    else
    {
        MmapFile::open_existing(for_read, false);
        struct tsdb_header *header = get_tsdb_header();
        ASSERT(header != nullptr);
        m_page_count = header->m_page_count;
        ASSERT(m_page_count > 0);
    }

    Logger::info("opening %s for %s", m_name.c_str(), for_read?"read":"write");
}

PageSize
HeaderFile::get_page_size()
{
    PageSize size;
    struct tsdb_header *header = get_tsdb_header();

    if (header != nullptr)
        size = header->m_page_size;
    else
        size = g_page_size;

    return size;
}

PageCount
HeaderFile::get_page_index()
{
    PageCount index;
    struct tsdb_header *header = get_tsdb_header();

    if (header != nullptr)
        index = header->m_page_index;
    else
        index = TT_INVALID_PAGE_INDEX;

    return index;
}

int
HeaderFile::get_compressor_version()
{
    int version;
    struct tsdb_header *header = get_tsdb_header();

    if (header != nullptr)
        version = header->get_compressor_version();
    else
        version = Config::get_int(CFG_TSDB_COMPRESSOR_VERSION, CFG_TSDB_COMPRESSOR_VERSION_DEF);

    return version;
}

HeaderIndex
HeaderFile::new_header_index(Tsdb *tsdb)
{
    ASSERT(is_open(false));

    struct tsdb_header *tsdb_header = get_tsdb_header();
    ASSERT(tsdb_header != nullptr);

    if (tsdb_header->is_full())
        return TT_INVALID_PAGE_INDEX;

    HeaderIndex header_idx = tsdb_header->m_header_index++;
    struct page_info_on_disk *header = get_page_header(header_idx);
    header->init();
    //header->m_page_index = tsdb_header->m_page_index++;

    return header_idx;
}

struct tsdb_header *
HeaderFile::get_tsdb_header()
{
    ASSERT(is_open(true));
    void *pages = get_pages();
    ASSERT(pages != nullptr);
    return (struct tsdb_header*)pages;
}

struct page_info_on_disk *
HeaderFile::get_page_header(HeaderIndex header_idx)
{
    ASSERT(is_open(true));
    struct page_info_on_disk *headers =
        reinterpret_cast<struct page_info_on_disk*>(static_cast<char*>(get_pages())+(sizeof(struct tsdb_header)));
    return &headers[header_idx];
}

void
HeaderFile::update_next(HeaderIndex prev_header_idx, FileIndex this_file_idx, HeaderIndex this_header_idx)
{
    struct page_info_on_disk *header = get_page_header(prev_header_idx);
    ASSERT(header != nullptr);
    header->m_next_file = this_file_idx;
    header->m_next_header = this_header_idx;
}

bool
HeaderFile::is_full()
{
    ASSERT(is_open(true));
    struct tsdb_header *tsdb_header =
        reinterpret_cast<struct tsdb_header *>(get_pages());
    ASSERT(tsdb_header != nullptr);
    return tsdb_header->is_full();
}

// for testing only
int
HeaderFile::count_pages(bool ooo)
{
    ensure_open(true);
    struct tsdb_header *tsdb_header = get_tsdb_header();
    ASSERT(tsdb_header != nullptr);
    int total = 0;
    for (int i = 0; i < tsdb_header->m_header_index; i++)
    {
        struct page_info_on_disk *page_header = get_page_header(i);
        ASSERT(page_header != nullptr);
        if ((ooo && page_header->is_out_of_order()) ||
            (!ooo && !page_header->is_out_of_order()))
            total++;
    }
    return total;
}


DataFile::DataFile(const std::string& file_name, FileIndex id, PageSize size, PageCount count) :
    MmapFile(file_name),
    m_id(id),
    m_file(nullptr),
    m_page_size(size),
    m_offset(0),
    m_page_count(count),
    m_page_index(TT_INVALID_PAGE_INDEX),
    m_header_file(nullptr),
    m_last_read(0),
    m_last_write(0)
{
}

/*
void
DataFile::init(HeaderFile *header_file)
{
    m_id = header_file->get_id();
    m_page_size = header_file->get_page_size();
    m_page_index = header_file->get_page_index();
}
*/

void
DataFile::open(bool for_read)
{
    if (for_read)
    {
        off_t length = (off_t)m_page_size * (off_t)m_page_count;
        MmapFile::open(length, true, false, false);
        Logger::info("opening %s for read", m_name.c_str());
    }
    else
    {
        ASSERT(m_file == nullptr);
        m_file = std::fopen(m_name.c_str(), "ab");

        // get file size
        int fd = ::fileno(m_file);
        struct stat sb;
        if (fstat(fd, &sb) == -1)
            Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
        off_t length = sb.st_size;

        m_page_index = length / m_page_size;
        Logger::info("opening %s for write", m_name.c_str());
    }
}

void
DataFile::close()
{
    if (m_file != nullptr)
    {
        std::fclose(m_file);
        m_file = nullptr;
    }

    Logger::info("closing data file %s (for both read & write), length = %" PRIu64, m_name.c_str(), get_length());
    MmapFile::close();
}

void
DataFile::close(int rw)
{
    if (rw == 0)
    {
        this->close();
    }
    else if (rw == 1)
    {
        // close read only
        MmapFile::close();
        Logger::info("closing %s for read", m_name.c_str());
    }
    else if (m_file != nullptr)
    {
        std::fclose(m_file);
        m_file = nullptr;
        Logger::info("closing %s for write", m_name.c_str());
    }
}

bool
DataFile::is_open(bool for_read) const
{
    if (for_read)
        return MmapFile::is_open(true);
    else
        return (m_file != nullptr);
}

PageCount
DataFile::append(const void *page, PageSize size)
{
    ASSERT(page != nullptr);
    ASSERT((size > 0) && ((size + m_offset) <= m_page_size));

    PageSize sum = m_offset + size;
    PageIndex idx = m_page_index;

    if ((sum < m_page_size) && ((m_page_size - sum) < 16))
    {
        size = m_page_size - m_offset;
        sum = m_page_size;
    }

    if (m_file == nullptr) open(false);
    ASSERT(m_file != nullptr);
    std::fwrite(page, size, 1, m_file);
    std::fflush(m_file);
    m_last_write = ts_now_sec();

    if (sum >= m_page_size)
    {
        m_offset = 0;
        m_page_index++;
    }
    else
        m_offset += size;

    return idx;
}

void
DataFile::flush(bool sync)
{
    if (m_file != nullptr)
        std::fflush(m_file);
}

void *
DataFile::get_page(PageIndex idx, PageSize offset)
{
    ASSERT(idx != TT_INVALID_PAGE_INDEX);
    uint8_t *pages = (uint8_t*)get_pages();
    ASSERT(pages != nullptr);
    m_last_read = ts_now_sec();
    return (void*)(pages + (idx * m_page_size) + offset);
}


}
