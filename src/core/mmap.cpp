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

bool
MmapFile::open(off_t length, bool read_only, bool append_only)
{
    ASSERT(length > 0 || read_only);

    if (m_fd > 0) ::close(m_fd);
    bool existing = file_exists(m_name);

    if (! existing && read_only)
        return false;
    m_read_only = read_only;

    struct stat sb;
    mode_t mode = read_only ? O_RDONLY : (O_CREAT|O_RDWR);
    m_fd = ::open(m_name.c_str(), mode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    m_fd = FileDescriptorManager::dup_fd(m_fd, FileDescriptorType::FD_FILE);

    if (m_fd == -1)
    {
        Logger::error("Failed to open file %s, errno = %d", m_name.c_str(), errno);
        return false;
    }

    if (fstat(m_fd, &sb) == -1)
    {
        Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
        return false;
    }

    m_length = length;

    if (! existing)
    {
        if (ftruncate(m_fd, length) != 0)
        {
            Logger::error("Failed to resize file %s, errno = %d", m_name.c_str(), errno);
            return false;
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

        if (! existing)
        {
            if (rm_file(m_name.c_str()) != 0)
                Logger::error("Mmap fails, but unable to remove newly created file %s", m_name.c_str());
        }

        return false;
    }

    int rc = madvise(m_pages, m_length, append_only ? MADV_SEQUENTIAL : MADV_RANDOM);

    if (rc != 0)
        Logger::warn("Failed to madvise(), page = %p, errno = %d", m_pages, errno);

    ASSERT(is_open(read_only));
    return !existing;
}

bool
MmapFile::fopen(const char *mode, std::FILE * (&file))
{
    bool existing = file_exists(m_name);
    file = std::fopen(m_name.c_str(), mode);

    // get file size
    if (existing)
    {
        int fd = ::fileno(file);
        struct stat sb;
        if (fstat(fd, &sb) == -1)
        {
            Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
            m_length = 0;
        }
        m_length = sb.st_size;
    }
    else
        m_length = 0;

    return !existing;
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
        munmap(m_pages, m_length);
        m_pages = nullptr;
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

bool
IndexFile::open(bool for_read)
{
    bool is_new = MmapFile::open(TT_SIZE_INCREMENT, for_read, false);

    if (is_new)
    {
        struct index_entry *entries = (struct index_entry*)get_pages();
        uint64_t max_idx = get_length() / TT_INDEX_SIZE;

        // TODO: memcpy()?
        for (uint64_t i = 0; i < max_idx; i++)
            entries[i].file_index = TT_INVALID_FILE_INDEX;
    }

    return is_new;
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
    m_page_count(0)
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
    header->m_page_size = g_page_size;

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

bool
HeaderFile::open(bool for_read)
{
    off_t length =
        sizeof(struct tsdb_header) + m_page_count * sizeof(struct page_info_on_disk);
    return MmapFile::open(length, for_read, false);
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
    header->m_page_index = tsdb_header->m_page_index++;

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


DataFile::DataFile(const std::string& file_name, FileIndex id, PageSize size, PageCount count) :
    MmapFile(file_name),
    m_id(id),
    m_file(nullptr),
    m_page_size(size),
    m_page_count(count),
    m_page_index(TT_INVALID_PAGE_INDEX)
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

bool
DataFile::open(bool for_read)
{
    if (for_read)
    {
        off_t length = (off_t)m_page_size * (off_t)m_page_count;
        return MmapFile::open(length, true, false);
    }
    else
    {
        ASSERT(m_file == nullptr);
        bool new_one = MmapFile::fopen("ab", m_file);
        size_t len = get_length();
        m_page_index = len / m_page_size;
        return new_one;
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

    MmapFile::close();
}

bool
DataFile::is_open(bool for_read) const
{
    if (for_read)
        return MmapFile::is_open(for_read);
    else
        return (m_file != nullptr);
}

PageCount
DataFile::append(const void *page)
{
    ASSERT(page != nullptr);
    if (m_file == nullptr) open(false);
    ASSERT(m_file != nullptr);
    std::fwrite(page, m_page_size, 1, m_file);
    std::fflush(m_file);
    return m_page_index++;
}

void
DataFile::flush(bool sync)
{
    if (m_file != nullptr)
        std::fflush(m_file);
}

void *
DataFile::get_page(PageIndex idx)
{
    ASSERT(idx != TT_INVALID_PAGE_INDEX);
    uint8_t *pages = (uint8_t*)get_pages();
    ASSERT(pages != nullptr);
    return (void*)(pages + (idx * m_page_size));
}


}
