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
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include "compress.h"
#include "config.h"
#include "fd.h"
#include "global.h"
#include "logger.h"
#include "mmap.h"
#include "page.h"
#include "query.h"
#include "tsdb.h"
#include "utils.h"


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
MmapFile::open(int64_t length, bool read_only, bool append_only, bool resize)
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
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
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
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
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
        Logger::error("Failed to open existing file %s, errno = %d", m_name.c_str(), errno);
        if (ENOMEM == errno)
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
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
            throw std::runtime_error(TT_MSG_OUT_OF_MEMORY);
        return;
    }

    int rc = madvise(m_pages, m_length, append_only ? MADV_SEQUENTIAL : MADV_RANDOM);

    if (rc != 0)
        Logger::warn("Failed to madvise(), page = %p, errno = %d", m_pages, errno);

    ASSERT(is_open(read_only));
}

bool
MmapFile::remap()
{
    // if it's closed, open it
    if (m_fd <= 0)
    {
        std::lock_guard<std::mutex> guard(m_lock);
        open_existing(true, false);
        return true;
    }

    struct stat sb;

    if (fstat(m_fd, &sb) == -1)
    {
        Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
        return false;
    }

    int64_t length = sb.st_size;

    if (length == m_length)
        return true;

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

bool
MmapFile::resize(int64_t length)
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
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_pages != nullptr)
    {
        if (! m_read_only) flush(true);
        munmap(m_pages, m_length);
        m_pages = nullptr;
        Logger::debug("closing %s", m_name.c_str());
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
    std::lock_guard<std::mutex> guard(m_lock);

    if (! is_open(for_read))
        open(for_read);

    // open() could fail if we are trying to open a non-existing file for read
    ASSERT(is_open(for_read) || (for_read && !exists()));
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
        {
            entries[i].file_index = TT_INVALID_FILE_INDEX;
            entries[i].file_index2 = TT_INVALID_FILE_INDEX;
        }
    }
    else
    {
        MmapFile::open_existing(for_read, false);
        ASSERT(get_pages() != nullptr);
    }

    Logger::debug("index file %s length: %" PRIu64, m_name.c_str(), get_length());
    Logger::debug("opening %s for %s", m_name.c_str(), for_read?"read":"write");
}

bool
IndexFile::close_if_idle(Timestamp threshold_sec, Timestamp now_sec)
{
    if ((threshold_sec + m_last_access.load()) < now_sec)
    {
        close();
        return true;
    }
    else
    {
        Logger::debug("index file %s last access at %" PRIu64 "; now is %" PRIu64,
            m_name.c_str(), m_last_access.load(), now_sec);
    }

    return false;
}

void
IndexFile::ensure_open(bool for_read)
{
    m_last_access = ts_now_sec();   // to prevent it from being closed
    MmapFile::ensure_open(for_read);
}

bool
IndexFile::expand(int64_t new_len)
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
    {
        entries[old_idx].flags = 0;
        entries[old_idx].file_index = TT_INVALID_FILE_INDEX;
        entries[old_idx].header_index = TT_INVALID_HEADER_INDEX;
        entries[old_idx].file_index2 = TT_INVALID_FILE_INDEX;
        entries[old_idx].header_index2 = TT_INVALID_HEADER_INDEX;
    }

    Logger::debug("index file %s length: %" PRIu64, m_name.c_str(), get_length());

    return true;
}

bool
IndexFile::set_indices(TimeSeriesId id, FileIndex file_index, HeaderIndex header_index)
{
    std::lock_guard<std::mutex> guard(m_lock);
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

    struct index_entry *entries = (struct index_entry*)pages;
    entries[id].file_index = file_index;
    entries[id].header_index = header_index;

    return true;
}

bool
IndexFile::set_indices2(TimeSeriesId id, FileIndex file_index, HeaderIndex header_index)
{
    std::lock_guard<std::mutex> guard(m_lock);
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

    struct index_entry *entries = (struct index_entry*)pages;
    entries[id].file_index2 = file_index;
    entries[id].header_index2 = header_index;

    return true;
}

void
IndexFile::get_indices(TimeSeriesId id, FileIndex& file_index, HeaderIndex& header_index)
{
    std::lock_guard<std::mutex> guard(m_lock);
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

void
IndexFile::get_indices2(TimeSeriesId id, FileIndex& file_index, HeaderIndex& header_index)
{
    std::lock_guard<std::mutex> guard(m_lock);
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
        file_index = entry.file_index2;
        header_index = entry.header_index2;
    }
}

#if 0
void
IndexFile::set_rollup_index(TimeSeriesId tid, RollupIndex idx)
{
    void *pages = get_pages();
    ASSERT(pages != nullptr);
    ASSERT(! is_read_only());

    size_t new_len = (tid+1) * TT_INDEX_SIZE;
    size_t old_len = get_length();
    ASSERT(0 < old_len);

    if (old_len < new_len)
    {
        // file too small, expand it
        if (! expand(new_len + TT_SIZE_INCREMENT))
        {
            Logger::error("IndexFile::set_rollup_index(tid=%u, idx=%u) failed to expand!", tid, idx);
            return;
        }
        pages = get_pages();
    }

    struct index_entry *entries = (struct index_entry*)pages;
    entries[tid].rollup_index = idx;
}

RollupIndex
IndexFile::get_rollup_index(TimeSeriesId id)
{
    RollupIndex rollup_idx = TT_INVALID_ROLLUP_INDEX;
    void *pages = get_pages();

    size_t idx = (id+1) * TT_INDEX_SIZE;
    size_t len = get_length();

    if ((len > idx) && (pages != nullptr))
    {
        struct index_entry *entries = (struct index_entry*)pages;
        rollup_idx = entries[id].rollup_index;
    }

    return rollup_idx;
}
#endif

bool
IndexFile::get_out_of_order(TimeSeriesId id)
{
    std::lock_guard<std::mutex> guard(m_lock);
    void *pages = get_pages();

    size_t idx = (id+1) * TT_INDEX_SIZE;
    size_t len = get_length();

    if ((len <= idx) || (pages == nullptr))
    {
        return false;
    }
    else
    {
        struct index_entry *entries = (struct index_entry*)pages;
        struct index_entry entry = entries[id];
        return entry.flags & 0x01;
    }
}

void
IndexFile::set_out_of_order(TimeSeriesId id, bool ooo)
{
    std::lock_guard<std::mutex> guard(m_lock);
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
            return;
        pages = get_pages();
    }

    struct index_entry *entries = (struct index_entry*)pages;

    if (ooo)
        entries[id].flags |= 0x01;
    else
        entries[id].flags &= ~0x01;
}

// apply to rollup data only
bool
IndexFile::get_out_of_order2(TimeSeriesId id)
{
    std::lock_guard<std::mutex> guard(m_lock);
    void *pages = get_pages();

    size_t idx = (id+1) * TT_INDEX_SIZE;
    size_t len = get_length();

    if ((len <= idx) || (pages == nullptr))
    {
        return false;
    }
    else
    {
        struct index_entry *entries = (struct index_entry*)pages;
        struct index_entry entry = entries[id];
        return entry.flags & 0x02;
    }
}

// apply to rollup data only
void
IndexFile::set_out_of_order2(TimeSeriesId id, bool ooo)
{
    std::lock_guard<std::mutex> guard(m_lock);
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
            return;
        pages = get_pages();
    }

    struct index_entry *entries = (struct index_entry*)pages;

    if (ooo)
        entries[id].flags |= 0x02;
    else
        entries[id].flags &= ~0x02;
}


HeaderFile::HeaderFile(const std::string& file_name, FileIndex id, PageCount page_count, PageSize page_size) :
    MmapFile(file_name),
    m_id(id),
    m_page_count(page_count)
{
    ASSERT(page_count > 0);
    ASSERT(page_size > 0);
    open(false);    // open for write
    init_tsdb_header(page_size);
}

HeaderFile::HeaderFile(FileIndex id, const std::string& file_name) :
    MmapFile(file_name),
    m_id(id),
    m_page_count(g_page_count)
{
    ASSERT(file_exists(file_name));
}

HeaderFile::~HeaderFile()
{
    this->close();
}

void
HeaderFile::init_tsdb_header(PageSize page_size)
{
    struct tsdb_header *header = (struct tsdb_header*)get_pages();
    ASSERT(header != nullptr);

    int compressor_version =
        Config::inst()->get_int(CFG_TSDB_COMPRESSOR_VERSION, CFG_TSDB_COMPRESSOR_VERSION_DEF);

    header->m_major_version = TT_MAJOR_VERSION;
    header->m_minor_version = TT_MINOR_VERSION;
    header->m_flags = 0;
    header->set_compacted(false);
    header->set_compressor_version(compressor_version);
    header->set_millisecond(g_tstamp_resolution_ms);
    header->m_page_count = m_page_count;
    header->m_header_index = 0;
    header->m_page_index = 0;
    header->m_start_tstamp = TimeRange::MAX.get_to();   //tsdb->get_time_range().get_from();
    header->m_end_tstamp = TimeRange::MAX.get_from();   //tsdb->get_time_range().get_to();
    header->m_actual_pg_cnt = m_page_count;
    header->m_page_size = page_size;

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
        int64_t length =
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

    if (is_new)
        Logger::debug("opening new %s for %s, page-count=%u", m_name.c_str(), for_read?"read":"write", m_page_count);
    else
        Logger::debug("opening %s for %s", m_name.c_str(), for_read?"read":"write");
}

void
HeaderFile::ensure_open(bool for_read)
{
    m_last_access = ts_now_sec();   // to prevent it from being closed
    MmapFile::ensure_open(for_read);
}

bool
HeaderFile::close_if_idle(Timestamp threshold_sec, Timestamp now_sec)
{
    if ((threshold_sec + m_last_access) < now_sec)
    {
        close();
        return true;
    }
    else
    {
        Logger::debug("header file %u last access at %" PRIu64 "; now is %" PRIu64,
            get_id(), m_last_access, now_sec);
    }

    return false;
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
    m_last_access = ts_now_sec();   // to prevent it from being closed
    return (struct tsdb_header*)pages;
}

struct page_info_on_disk *
HeaderFile::get_page_header(HeaderIndex header_idx)
{
    ASSERT(is_open(true));
    ASSERT(get_tsdb_header() != nullptr);
    ASSERT(header_idx < get_tsdb_header()->m_header_index);

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
    m_last_access = ts_now_sec();   // to prevent it from being closed
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
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_lock, &attr);
}

DataFile::~DataFile()
{
    this->close();
    pthread_rwlock_destroy(&m_lock);
}

void
DataFile::open(bool for_read)
{
    if (for_read)
    {
        if (m_file != nullptr)  // open for write?
        {
            // To avoid frequent remapping in this case, we open
            // and map the full potential length of the file.
            int64_t length = (int64_t)m_page_size * (int64_t)m_page_count;
            MmapFile::open(length, true, false, false);
        }
        else
        {
            MmapFile::open_existing(true, false);
        }

        m_last_read = ts_now_sec();
        Logger::debug("opening %s for read", m_name.c_str());
    }
    else
    {
        ASSERT(m_file == nullptr);
        //m_file = std::fopen(m_name.c_str(), "ab");
        int fd = ::open(m_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

        if (fd == -1)
        {
            Logger::error("Failed to open data file %s for append: %d", m_name.c_str(), errno);
        }
        else
        {
            // get file size
            struct stat sb;
            std::memset(&sb, 0, sizeof(sb));
            if (fstat(fd, &sb) == -1)
                Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);
            int64_t length = sb.st_size;

            // allocate enough disk space for this file
/**
            if (length == 0)
            {
                if (fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, g_page_size*g_page_count) != 0)
                    Logger::warn("fallocate(%d) failed, errno = %d", fd, errno);
                else
                    Logger::debug("fallocate(%s, %u) called", m_name.c_str(), g_page_size*g_page_count);
            }
**/

            m_page_index = length / m_page_size;
            Logger::debug("opening %s for write", m_name.c_str());

            m_file = fdopen(fd, "ab");
            ASSERT(m_file != nullptr);
            ASSERT(m_page_index != TT_INVALID_PAGE_INDEX);
            m_last_write = ts_now_sec();
        }
    }
}

void
DataFile::ensure_open(bool for_read)
{
    // to prevent it from being closed
    if (for_read)
    {
        m_last_read = ts_now_sec();
        MmapFile::ensure_open(for_read);
    }
    else
    {
        m_last_write = ts_now_sec();
        //PThread_WriteLock(get_lock());
        if (m_file == nullptr)
            open(false);    // open for write
    }
}

void
DataFile::close()
{
    if (m_file != nullptr)
    {
        std::fclose(m_file);
        m_file = nullptr;
        Logger::debug("closing data file %s (for both read & write), length = %lu", m_name.c_str(), get_length());
    }

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
        Logger::debug("closing %s for read", m_name.c_str());
    }
    else if (m_file != nullptr)
    {
        std::fclose(m_file);
        m_file = nullptr;
        Logger::debug("closing %s for write", m_name.c_str());
    }
}

bool
DataFile::close_if_idle(Timestamp threshold_sec, Timestamp now_sec)
{
    bool closed = true;

    if (is_open(true))  // open for read?
    {
        if ((threshold_sec + m_last_read) < now_sec)
            close(1);   // close for read
        else
        {
            closed = false;
            Logger::debug("data file %u last read at %" PRIu64 "; now is %" PRIu64,
                get_id(), m_last_read, now_sec);
        }
    }

    if (is_open(false)) // open for write?
    {
        if ((threshold_sec + m_last_write) < now_sec)
            close(2);   // close for write
        else
        {
            closed = false;
            Logger::debug("data file %u last write at %" PRIu64 "; now is %" PRIu64,
                get_id(), m_last_write, now_sec);
        }
    }

    return closed;
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
    ASSERT(idx != TT_INVALID_PAGE_INDEX);

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

// In case where the page is not mapped into memory,
// it will return nullptr. In this case, you need to
// remap the file.
void *
DataFile::get_page(PageIndex idx)
{
    ASSERT(idx != TT_INVALID_PAGE_INDEX);

    // make sure the 'compress_info_on_disk' at the beginning of
    // the page is already mapped into memory
    PageSize cursor = sizeof(struct compress_info_on_disk);
    long page_idx = idx * m_page_size;
    if ((page_idx + cursor) > get_length())
        return nullptr;     // needs remap

    uint8_t *pages = (uint8_t*)get_pages();
    ASSERT(pages != nullptr);
    uint8_t *page = pages + page_idx;

    // make sure the whole page is already mapped into memory
    struct compress_info_on_disk *ciod =
        reinterpret_cast<struct compress_info_on_disk*>(page);
    cursor += ciod->m_cursor;
    if (ciod->m_start != 0) cursor++;
    if ((page_idx + cursor) > get_length())
        return nullptr;     // needs remap

    m_last_read = ts_now_sec();
    return (void*)(page);
}


/* @param mid Metric ID
 * @param begin Timestamp (in seconds) of beginning of a month
 */
RollupDataFile::RollupDataFile(MetricId mid, Timestamp begin, bool monthly) :
    m_file(nullptr),
    m_begin(begin),
    m_last_access(0),
    m_index(0),
    m_size(0),
    m_ref_count(0),
    m_for_read(false),
    m_monthly(monthly)
{
    int year, month;
    get_year_month(begin, year, month);
    Config *cfg = nullptr;

    if (monthly)
    {
        cfg = RollupManager::get_rollup_config(year, month, true);
        m_name = get_name_by_mid_1h(mid, year, month);;
    }
    else
    {
        cfg = RollupManager::get_rollup_config(year, true);
        m_name = get_name_by_mid_1d(mid, year);
    }

    ASSERT(cfg != nullptr);
    m_compressor_version = monthly ?
        cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_VERSION, CFG_TSDB_ROLLUP_COMPRESSOR_VERSION_DEF)
        : 0;
    m_compressor_precision = std::pow(10,
        cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF));
}

// create 1h rollup file
RollupDataFile::RollupDataFile(const std::string& name, Timestamp begin) :
    m_file(nullptr),
    m_begin(begin),
    m_last_access(0),
    m_index(0),
    m_size(0),
    m_ref_count(0),
    m_name(name),
    m_for_read(false),
    m_monthly(true)
{
    int year, month;
    get_year_month(begin, year, month);
    Config *cfg = RollupManager::get_rollup_config(year, month, false);
    if (cfg != nullptr)
    {
        m_compressor_version =
            cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_VERSION, CFG_TSDB_ROLLUP_COMPRESSOR_VERSION_DEF);
        m_compressor_precision = std::pow(10,
            cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF));
    }
}

// create 1d rollup file
RollupDataFile::RollupDataFile(int bucket, Timestamp tstamp) :
    m_file(nullptr),
    m_begin(TT_INVALID_TIMESTAMP),
    m_last_access(0),
    m_index(0),
    m_size(0),
    m_ref_count(0),
    m_for_read(false),
    m_monthly(false)
{
    ASSERT(bucket >= 0);
    ASSERT(is_sec(tstamp));

    int year, month;
    get_year_month(tstamp, year, month);
    m_name = get_name_by_bucket_1d(bucket, year);
    Config *cfg = RollupManager::get_rollup_config(year, true);
    ASSERT(cfg != nullptr);
    m_compressor_version = 0;   // TODO: compress 1d rollup data file
        //cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_VERSION, CFG_TSDB_ROLLUP_COMPRESSOR_VERSION_DEF);;
    m_compressor_precision = std::pow(10,
        cfg->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF));
}

RollupDataFile::~RollupDataFile()
{
    this->close();
}

void
RollupDataFile::open(bool for_read)
{
    ASSERT(m_file == nullptr);
    int fd = ::open(m_name.c_str(), O_RDWR|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

    if (fd == -1)
    {
        Logger::error("Failed to open rollup data file %s for %s: %d",
            m_name.c_str(),
            for_read ? "read" : "write",
            errno);
    }
    else
    {
        if (! for_read)
        {
            struct stat sb;
            std::memset(&sb, 0, sizeof(sb));

            if (fstat(fd, &sb) == -1)
                Logger::error("Failed to fstat file %s, errno = %d", m_name.c_str(), errno);

            if (sb.st_size == 0)
            {
                int64_t length = RollupManager::get_rollup_data_file_size(m_monthly);
                if (fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, length) != 0)
                    Logger::warn("fallocate(%d) failed, errno = %d", fd, errno);
                else
                    Logger::debug("fallocate(%s, %lu) called", m_name.c_str(), length);
            }
            else
                m_size = sb.st_size;
        }

        m_for_read = for_read;
        m_file = fdopen(fd, for_read?"r+":"a+b");
        ASSERT(m_file != nullptr);
        Logger::debug("opening %s for read/write", m_name.c_str());
    }
}

void
RollupDataFile::flush()
{
    if ((m_file != nullptr) && (m_index > 0))
    {
        ASSERT(! m_for_read);
        ASSERT(0 <= m_index && m_index <= sizeof(m_buff));
        std::fwrite(m_buff, m_index, 1, m_file);
        std::fflush(m_file);
        m_index = 0;
    }
}

void
RollupDataFile::close()
{
    if (m_file != nullptr)
    {
        if (m_index > 0)
        {
            ASSERT(0 <= m_index && m_index <= sizeof(m_buff));
            std::fwrite(m_buff, m_index, 1, m_file);
            m_index = 0;
        }
        std::fflush(m_file);
        std::fclose(m_file);
        m_file = nullptr;
        Logger::debug("closing rollup data file %s (for both read & write)", m_name.c_str());
    }
}

bool
RollupDataFile::close_if_idle(Timestamp threshold_sec, Timestamp now_sec)
{
    std::lock_guard<std::mutex> guard(m_lock);

    if ((m_ref_count <= 0) && ((threshold_sec + m_last_access) < now_sec))
    {
        close();
        return true;
    }
    else
    {
        Logger::debug("rollup data file %s last access at %" PRIu64 "; now is %" PRIu64,
            m_name.c_str(), m_last_access, now_sec);
    }

    return false;
}

bool
RollupDataFile::is_open(bool for_read) const
{
    return (m_file != nullptr) && (for_read == m_for_read);
}

void
RollupDataFile::add_data_point(TimeSeriesId tid, uint32_t cnt, double min, double max, double sum)
{
    int size;
    uint8_t buff[128];

    size = RollupCompressor_v1::compress(buff, tid, cnt, min, max, sum, m_compressor_precision);

    std::lock_guard<std::mutex> guard(m_lock);

    if (sizeof(m_buff) < (m_index + size))
    {
        // write the m_buff[] out
        if (! is_open(false))
        {
            if (is_open(true)) close();
            open(false);
        }
        ASSERT(m_file != nullptr);
        std::fwrite(m_buff, m_index, 1, m_file);
        std::fflush(m_file);
        m_index = 0;
        m_size = 0;
    }

    std::memcpy(m_buff+m_index, buff, size);
    m_index += size;
    m_size += size;
    m_last_access = ts_now_sec();
}

// This is for daily rollup as well as backup file.
void
RollupDataFile::add_data_point(TimeSeriesId tid, Timestamp tstamp, uint32_t cnt, double min, double max, double sum)
{
    struct rollup_entry_ext entry;

    entry.tid = tid;
    entry.cnt = cnt;
    entry.min = min;
    entry.max = max;
    entry.sum = sum;
    entry.tstamp = tstamp;

    if (! is_open(false))
    {
        if (is_open(true)) close();
        open(false);
    }
    ASSERT(m_file != nullptr);
    std::fwrite(&entry, sizeof(entry), 1, m_file);
    //std::fflush(m_file);
    //m_index += sizeof(entry);
    //m_size += sizeof(entry);
}

void
RollupDataFile::add_data_points(std::unordered_map<TimeSeriesId,std::vector<struct rollup_entry_ext>>& data)
{
    std::lock_guard<std::mutex> guard(m_lock);

    ASSERT(! is_open(false) && ! is_open(true));
    open(false);

    for (auto& it: data)
    {
        std::vector<struct rollup_entry_ext>& entries = it.second;

        for (auto& entry: entries)
        {
            ASSERT(it.first == entry.tid);

            if (entry.cnt == 0)
                continue;

            add_data_point(
                entry.tid,
                entry.tstamp,
                entry.cnt,
                entry.min,
                entry.max,
                entry.sum);
        }
    }

    close();
}

struct rollup_entry *
RollupDataFile::first_entry(RollupDataFileCursor& cursor)
{
    // make sure we are open for read
    m_last_access = ts_now_sec();
    if (! is_open(true) && ! is_open(false))
        open(true);
    std::fseek(m_file, 0, SEEK_SET);    // seek to beginning of file

    cursor.m_index = 0;
    cursor.m_size = std::fread(&cursor.m_buff[0], 1, sizeof(cursor.m_buff), m_file);
    if (cursor.m_size <= 0) return nullptr;

    cursor.m_index = RollupCompressor_v1::uncompress(cursor.m_buff,
                                                     cursor.m_size,
                                                     &cursor.m_entry,
                                                     m_compressor_precision);
    if (cursor.m_index == 0)
        return nullptr;
    else
        return &cursor.m_entry;
}

struct rollup_entry *
RollupDataFile::next_entry(RollupDataFileCursor& cursor)
{
    int len = 0;

    if (cursor.m_index < cursor.m_size)
    {
        len = RollupCompressor_v1::uncompress(cursor.m_buff+cursor.m_index,
                                              cursor.m_size-cursor.m_index,
                                              &cursor.m_entry,
                                              m_compressor_precision);
    }

    if (len == 0)
    {
        // not enough data in m_buff for the next entry
        ASSERT(cursor.m_index > 0);

        // copy remaining unprocessed data to the beginning of m_buff
        for (int i = cursor.m_index, j = 0; (i+j) < cursor.m_size; j++)
            cursor.m_buff[j] = cursor.m_buff[i+j];
        int offset = cursor.m_size - cursor.m_index;

        // read next buff
        cursor.m_index = 0;
        cursor.m_size = std::fread(&cursor.m_buff[offset], 1, sizeof(cursor.m_buff)-offset, m_file);
        if (cursor.m_size <= 0) return nullptr;
        cursor.m_size += offset;
        return next_entry(cursor);
    }
    else
    {
        cursor.m_index += len;
        return &cursor.m_entry;
    }
}

int
RollupDataFile::query_entry(const TimeRange& range, struct rollup_entry *entry, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup)
{
    ASSERT(entry != nullptr);

    int found = 0;
    auto search = map.find(entry->tid);

    if (search != map.end())
    {
        QueryTask *task = search->second;
        Timestamp ts;

        if (task->get_last_tstamp() == TT_INVALID_TIMESTAMP)
        {
            ts = m_begin;
            task->set_last_tstamp(ts = m_begin);    // in seconds
        }
        else
        {
            ts = task->get_last_tstamp() + g_rollup_interval_1h;    // in seconds
            task->set_last_tstamp(ts);
        }

        ts = validate_resolution(ts);
        found = range.in_range(ts);

        if (entry->cnt != 0 && found == 0)
        {
            struct rollup_entry_ext ext(entry);
            ext.tstamp = ts;
            task->add_data_point(&ext, rollup);
        }
    }

    return found;
}

void
RollupDataFile::query(const TimeRange& range, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup)
{
    RollupDataFileCursor cursor;
    std::lock_guard<std::mutex> guard(m_lock);

    for (struct rollup_entry *entry = first_entry(cursor); entry != nullptr; entry = next_entry(cursor))
    {
        if (query_entry(range, entry, map, rollup) > 0)
            return;
    }

    // look into m_buff...
    int len;

    for (int i = 0; i < m_index; i += len)
    {
        struct rollup_entry entry;
        len = RollupCompressor_v1::uncompress((uint8_t*)(m_buff+i), m_index-i, &entry, m_compressor_precision);
        if (len <= 0) break;
        if (query_entry(range, &entry, map, rollup) > 0) break;
    }
}

void
RollupDataFile::query2(const TimeRange& range, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup)
{
    set_rollup_level(rollup, false);    // unset level2

    std::lock_guard<std::mutex> guard(m_lock);
    uint8_t buff[1024*sizeof(struct rollup_entry_ext)];
    std::size_t n;

    m_last_access = ts_now_sec();
    if (! is_open(true) && ! is_open(false))
        open(true);
    std::fseek(m_file, 0, SEEK_SET);    // seek to beginning of file

    while ((n = std::fread(&buff[0], 1, sizeof(buff), m_file)) > 0)
    {
        ASSERT((n % sizeof(struct rollup_entry_ext)) == 0);

        for (std::size_t i = 0; i < n; i += sizeof(struct rollup_entry_ext))
        {
            struct rollup_entry_ext *entry = (struct rollup_entry_ext*)&buff[i];

            auto search = map.find(entry->tid);

            if (search != map.end())
            {
                QueryTask *task = search->second;
                Timestamp ts = entry->tstamp;
                Timestamp last_ts = task->get_last_tstamp();

                // out-of-order?
                if ((last_ts != TT_INVALID_TIMESTAMP) && (ts <= last_ts))
                {
                    // TODO: handle OOO
                }

                task->set_last_tstamp(ts);
                ts = validate_resolution(ts);

                if (entry->cnt != 0 && range.in_range(ts) == 0)
                {
                    //double val = RollupManager::query((struct rollup_entry*)entry, rollup);
                    //task->add_data_point(ts, val);
                    task->add_data_point(entry, rollup);
                }
            }
        }
    }
}

void
RollupDataFile::query(const TimeRange& range, std::unordered_map<TimeSeriesId,struct rollup_entry_ext>& outputs)
{
    RollupDataFileCursor cursor;
    std::lock_guard<std::mutex> guard(m_lock);

    for (struct rollup_entry *entry = first_entry(cursor); entry != nullptr; entry = next_entry(cursor))
    {
        auto search = outputs.find(entry->tid);

        if (search != outputs.end())
        {
            if (search->second.tstamp == TT_INVALID_TIMESTAMP)
                search->second.tstamp = m_begin;    // in seconds
            else
                search->second.tstamp += g_rollup_interval_1h;  // in seconds

            if ((entry->cnt != 0) && (range.in_range(search->second.tstamp) == 0))
            {
                search->second.cnt += entry->cnt;
                search->second.min = std::min(search->second.min,entry->min);
                search->second.max = std::max(search->second.max,entry->max);
                search->second.sum += entry->sum;
            }
        }
    }
}

// used to restore RollupManager from WAL
void
RollupDataFile::query_ext(const TimeRange& range, std::unordered_map<TimeSeriesId,struct rollup_entry_ext>& outputs)
{
    std::lock_guard<std::mutex> guard(m_lock);
    uint8_t buff[1024*sizeof(struct rollup_entry_ext)];
    std::size_t n;

    m_last_access = ts_now_sec();
    if (! is_open(true) && ! is_open(false))
        open(true);
    std::fseek(m_file, 0, SEEK_SET);    // seek to beginning of file

    while ((n = std::fread(&buff[0], 1, sizeof(buff), m_file)) > 0)
    {
        ASSERT((n % sizeof(struct rollup_entry_ext)) == 0);

        for (std::size_t i = 0; i < n; i += sizeof(struct rollup_entry_ext))
        {
            struct rollup_entry_ext *entry = (struct rollup_entry_ext*)&buff[i];
            auto search = outputs.find(entry->tid);

            if (search != outputs.end())
            {
                if (search->second.tstamp == TT_INVALID_TIMESTAMP)
                    search->second.tstamp = m_begin;    // in seconds
                else
                    search->second.tstamp += g_rollup_interval_1h;  // in seconds

                if ((entry->cnt != 0) && (range.in_range(search->second.tstamp) == 0))
                {
                    search->second.cnt += entry->cnt;
                    search->second.min = std::min(search->second.min,entry->min);
                    search->second.max = std::max(search->second.max,entry->max);
                    search->second.sum += entry->sum;
                }
            }
            else
            {
                TimeSeriesId tid = entry->tid;
                outputs.emplace(std::make_pair(tid, *entry));
            }
        }
    }
}

void
RollupDataFile::query(std::unordered_map<TimeSeriesId,std::vector<struct rollup_entry_ext>>& data)
{
    RollupDataFileCursor cursor;
    std::lock_guard<std::mutex> guard(m_lock);

    flush();

    for (struct rollup_entry *entry = first_entry(cursor); entry != nullptr; entry = next_entry(cursor))
    {
        auto search = data.find(entry->tid);

        if (search == data.end())
        {
            // first data point
            std::vector<struct rollup_entry_ext> entries;
            data.emplace(std::make_pair((TimeSeriesId)entry->tid, entries));
            search = data.find(entry->tid);
            ASSERT(search != data.end());
            struct rollup_entry_ext ext;

            ext.tid = entry->tid;
            ext.cnt = entry->cnt;
            ext.min = entry->min;
            ext.max = entry->max;
            ext.sum = entry->sum;
            ext.tstamp = m_begin;

            search->second.emplace_back(ext);
        }
        else
        {
            auto& ext = search->second.back();
            Timestamp last_ts = ext.tstamp;
            Timestamp curr_ts = last_ts + g_rollup_interval_1h;
            Timestamp last_step_down = step_down(last_ts, g_rollup_interval_1d);
            Timestamp curr_step_down = step_down(curr_ts, g_rollup_interval_1d);

            if (last_step_down == curr_step_down)
            {
                ext.cnt += entry->cnt;
                ext.min = std::min(ext.min, entry->min);
                ext.max = std::max(ext.max, entry->max);
                ext.sum += entry->sum;
                ext.tstamp += g_rollup_interval_1h;
            }
            else
            {
                ext.tstamp = last_step_down;

                struct rollup_entry_ext new_ext;

                new_ext.tid = entry->tid;
                new_ext.cnt = entry->cnt;
                new_ext.min = entry->min;
                new_ext.max = entry->max;
                new_ext.sum = entry->sum;
                new_ext.tstamp = curr_ts;

                search->second.emplace_back(new_ext);
            }
        }
    }

    close();

    // step down the last data point
    for (auto& it: data)
    {
        auto& ext = it.second.back();
        ext.tstamp = step_down(ext.tstamp, g_rollup_interval_1d);
    }
}

std::string
RollupDataFile::get_name_by_mid_1h(MetricId mid, int year, int month)
{
    return get_name_by_bucket_1h(RollupManager::get_rollup_bucket(mid), year, month);
}

std::string
RollupDataFile::get_name_by_bucket_1h(int bucket, int year, int month)
{
    std::ostringstream oss;
    oss << Config::get_data_dir() << "/"
        << std::to_string(year) << "/"
        << std::setfill('0') << std::setw(2) << month << "/rollup"
        << "/r" << std::setfill('0') << std::setw(6) << bucket << ".data";
    return oss.str();
}

std::string
RollupDataFile::get_name_by_mid_1d(MetricId mid, int year)
{
    return get_name_by_bucket_1d(RollupManager::get_rollup_bucket(mid), year);
}

std::string
RollupDataFile::get_name_by_bucket_1d(int bucket, int year)
{
    std::ostringstream oss;
    oss << Config::get_data_dir() << "/"
        << std::to_string(year) << "/rollup"
        << "/r" << std::setfill('0') << std::setw(6) << bucket << ".data";
    return oss.str();
}

void
RollupDataFile::dec_ref_count()
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_ref_count--;
}

void
RollupDataFile::inc_ref_count()
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_ref_count++;
}


}
