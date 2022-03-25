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

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <stdexcept>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "compress.h"
#include "config.h"
#include "fd.h"
#include "memmgr.h"
#include "meter.h"
#include "page.h"
#include "tsdb.h"
#include "logger.h"


namespace tt
{


PageInfo::PageInfo() :
    m_page_mgr(nullptr),
    m_compressor(nullptr),
    m_header(nullptr)
{
}

bool
PageInfo::is_full() const
{
    if (m_compressor != nullptr)
    {
        return m_compressor->is_full();
    }
    else
    {
        ASSERT(m_header != nullptr);
        return m_header->is_full();
    }
}

bool
PageInfo::is_empty() const
{
    if (m_compressor != nullptr)
    {
        return m_compressor->is_empty();
    }
    else
    {
        ASSERT(m_header != nullptr);
        return m_header->is_empty();
    }
}

void
PageInfo::flush()
{
    if (m_compressor == nullptr) return;

    persist();

    /* TODO: Is this needed?
    int rc = msync(m_pages, size, (sync?MS_SYNC:MS_ASYNC));

    if (rc == -1)
    {
        Logger::info("Failed to flush file %s, errno = %d", m_file_name.c_str(), errno);
    }
    */

    // Skip this if m_offset != 0. madvise() can only be done when address is
    // aligned perfectly along 4K.
    if (m_header->m_offset == 0)
    {
        int rc = madvise(get_page(), g_page_size, MADV_DONTNEED);
        if (rc == -1)
            Logger::info("Failed to madvise(DONTNEED), page = %p, errno = %d", get_page(), errno);
    }

    if (is_full())
        recycle();
}

void
PageInfo::shrink_to_fit()
{
    persist();
    m_header->m_size = m_header->m_cursor;
    ASSERT(m_header->m_size != 0);
    if (m_header->m_start != 0) m_header->m_size++;
    if (m_page_mgr->get_compressor_version() == 0) m_header->m_size *= 16;
    m_header->set_full(true);
    flush();
}

void
PageInfo::reset()
{
    ASSERT(m_compressor != nullptr);
    //ASSERT(m_page_mgr == nullptr);

    //m_id = 0;
    //m_page_index = 0;
    //m_compressor->init(range.get_from(), reinterpret_cast<uint8_t*>(m_page), m_size);
    // TODO: should call m_compressor->reset() or m_compressor->init()
    m_compressor->recycle();
}

bool
PageInfo::recycle()
{
    if (m_compressor != nullptr)
    {
        MemoryManager::free_recyclable(m_compressor);
        m_compressor = nullptr;
    }

    return true;
}

// initialize a PageInfo that represent a page on disk;
// this is a new page, so we will not read page_info_on_disk;
void
PageInfo::init_for_disk(PageManager *pm, struct page_info_on_disk *header, PageCount page_idx, PageSize size, bool is_ooo)
{
    ASSERT(pm != nullptr);
    ASSERT(header != nullptr);
    ASSERT(size > 1);

    m_header = header;
    const TimeRange& range = pm->get_time_range();
    m_time_range.init(range.get_to(), range.get_from());    // empty range
    m_header->init(range);
    m_header->set_out_of_order(is_ooo);
    m_header->m_page_index = page_idx;
    m_header->m_offset = 0;
    m_header->m_size = size;
    ASSERT(m_header->m_size != 0);
    m_page_mgr = pm;
    //set_page();
    m_compressor = nullptr;
}

// initialize a PageInfo that represent a page on disk
// 'id': global id
void
PageInfo::init_from_disk(PageManager *pm, struct page_info_on_disk *header)
{
    ASSERT(pm != nullptr);
    ASSERT(header != nullptr);
    ASSERT(pm->is_open());

    m_page_mgr = pm;
    m_header = header;
    m_compressor = nullptr;
    Timestamp start = pm->get_time_range().get_from();
    m_time_range.init(m_header->m_tstamp_from + start, m_header->m_tstamp_to + start);
    ASSERT(pm->get_time_range().contains(m_time_range));
}

/* 'range' should be the time range of the Tsdb.
 */
void
PageInfo::setup_compressor(const TimeRange& range, int compressor_version)
{
    if (m_compressor != nullptr)
    {
        MemoryManager::free_recyclable(m_compressor);
        m_compressor = nullptr;
    }

    ASSERT(m_header != nullptr);

    if (m_header->is_out_of_order())
    {
        m_compressor =
            (Compressor*)MemoryManager::alloc_recyclable(RecyclableType::RT_COMPRESSOR_V0);
    }
    else
    {
        RecyclableType type =
            (RecyclableType)(compressor_version + RecyclableType::RT_COMPRESSOR_V0);
        m_compressor = (Compressor*)MemoryManager::alloc_recyclable(type);
    }

    m_compressor->init(range.get_from(), reinterpret_cast<uint8_t*>(get_page()), m_header->m_size);
}

void
PageInfo::ensure_dp_available(DataPointVector *dps)
{
    if (m_compressor != nullptr) return;

    ASSERT(m_page_mgr->is_open());
    Meter meter(METRIC_TICKTOCK_PAGE_RESTORE_TOTAL_MS);

    CompressorPosition position(m_header);
    setup_compressor(m_page_mgr->get_time_range(), m_page_mgr->get_compressor_version());
    if (dps == nullptr)
    {
        DataPointVector v;
        v.reserve(700);
        m_compressor->restore(v, position, nullptr);
    }
    else
    {
        m_compressor->restore(*dps, position, nullptr);
    }
    ASSERT(m_page_mgr->get_time_range().contains(m_time_range));
}

void
PageInfo::persist(bool copy_data)
{
    if (m_compressor == nullptr) return;

    // write data
    CompressorPosition position;
    m_compressor->save(position);
    // only version 0 compressor needs saving
    if ((m_compressor->get_version() == 0) || copy_data)
        m_compressor->save((uint8_t*)get_page()); // copy content to mmap file

    // write header
    //struct page_info_on_disk *piod = m_page_mgr->get_page_info_on_disk(m_id);
    ASSERT(m_header != nullptr);
    Timestamp start = m_page_mgr->get_time_range().get_from();
    ASSERT(start <= m_time_range.get_from());

    m_header->init(position.m_offset,
                   position.m_start,
                   m_compressor->is_full(),
                   m_time_range.get_from() - start,
                   m_time_range.get_to() - start);
}

// Append this page after 'dst' inside the same physical page;
// This is done during compaction.
void
PageInfo::merge_after(PageInfo *dst)
{
    ASSERT(dst != nullptr);
    //ASSERT(m_page_mgr == dst->m_page_mgr);
    //ASSERT(m_id > dst->m_id);
    ASSERT(m_compressor != nullptr);
    ASSERT(dst->m_compressor != nullptr);

    m_header->m_page_index = dst->m_header->m_page_index;
    m_header->m_offset = dst->m_header->m_offset + dst->m_header->m_size;
    m_header->m_size = m_compressor->size();
    //set_page();
    persist(true);
    m_compressor->rebase(static_cast<uint8_t*>(get_page()));
}

void
PageInfo::copy_to(PageCount dst_id)
{
    //ASSERT(m_id > dst_id);
    ASSERT(m_compressor != nullptr);

    m_header->m_page_index = dst_id;
    m_header->m_offset = 0;
    m_header->m_size = m_compressor->size();
    //set_page();
    persist(true);
    m_compressor->rebase(static_cast<uint8_t*>(get_page()));
}

PageCount
PageInfo::get_id() const
{
    ASSERT(m_page_mgr != nullptr);
    return m_page_mgr->calc_page_info_index(m_header);
}

PageCount
PageInfo::get_file_id() const
{
    ASSERT(m_page_mgr != nullptr);
    return m_page_mgr->get_id();
}

int
PageInfo::get_page_order() const
{
    return (get_file_id() * m_page_mgr->get_page_count()) + m_header->m_page_index;
}

void *
PageInfo::get_page()
{
    uint8_t *first_page = m_page_mgr->get_first_page();
    ASSERT(first_page != nullptr);
    PageCount idx = m_header->m_page_index;
    return static_cast<void*>(first_page + (idx * g_page_size) + m_header->m_offset);
}

Timestamp
PageInfo::get_last_tstamp() const
{
    ASSERT(m_compressor != nullptr);
    //ASSERT(m_page_mgr->get_time_range().in_range(m_compressor->get_last_tstamp()));
    return m_compressor->get_last_tstamp();
}

bool
PageInfo::add_data_point(Timestamp tstamp, double value)
{
    if (m_compressor == nullptr) return false;
    //ASSERT(m_page_mgr->get_time_range().in_range(tstamp));
    bool success = m_compressor->compress(tstamp, value);
    if (success) m_time_range.add_time(tstamp);
    return success;
}

void
PageInfo::get_all_data_points(DataPointVector& dps)
{
    if (m_compressor != nullptr) m_compressor->uncompress(dps);
}

int
PageInfo::get_dp_count() const
{
    return (m_compressor == nullptr) ? 0 : m_compressor->get_dp_count();
}

const char *
PageInfo::c_str(char *buff) const
{
    std::snprintf(buff, c_size(), "idx=%d is_ooo=%d comp=%p",
        m_header->m_page_index, m_header->is_out_of_order(), m_compressor);
    return buff;
}


PageManager::PageManager(TimeRange& range, PageCount id, bool temp) :
    m_major_version(TT_MAJOR_VERSION),
    m_minor_version(TT_MINOR_VERSION),
    m_compacted(false),
    m_time_range(range),
    m_id(id),
    m_fd(-1)
{
    m_file_name = Tsdb::get_file_name(range, std::to_string(m_id), temp);
    m_compressor_version =
        Config::get_int(CFG_TSDB_COMPRESSOR_VERSION, CFG_TSDB_COMPRESSOR_VERSION_DEF);

    PageCount page_count =
        Config::get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    m_total_size = (TsdbSize)page_count * (TsdbSize)g_page_size;

    bool is_new = open_mmap(page_count);

    if (m_pages == nullptr)
        throw std::runtime_error("Failed to create data file in page manager.");
        //return; // failed to open

    if (is_new)
    {
        init_headers(); // zero-out headers
    }
    else
    {
        // In the case of abnormal shutdown, there's a chance m_page_index was
        // persisted, but the latest page_info_on_disk was not. In that case,
        // we need to discard those pages to avoid corrupted data.
        PageCount id = *m_header_index;
        PageCount page_idx = *m_page_index;

        for (id--; id >= 0; id--)
        {
            struct page_info_on_disk *info = get_page_info_on_disk(id);

            if (info->m_page_index != 0)
            {
                page_idx = info->m_page_index + 1;
                break;
            }
        }

        if ((++id) != *m_header_index)
        {
            Logger::warn("Last %d pages are not initialized, will be discarded",
                (*m_header_index)-id);
            *m_header_index = id;
            *m_page_index = page_idx;
            ASSERT(*m_page_index <= *m_actual_pg_cnt);
        }
    }
}

PageManager::~PageManager()
{
    close_mmap();
}

void
PageManager::init_headers()
{
    ASSERT(m_page_count != nullptr);
    ASSERT(m_page_info != nullptr);

    size_t size = (*m_page_count) * sizeof(struct page_info_on_disk);
    std::memset((void*)m_page_info, 0, size);
    msync((void*)m_page_info, size, MS_SYNC);
}

bool
PageManager::reopen()
{
    if (m_pages == nullptr)
        open_mmap(0);   // the parameter is unused since this file exists
    return (m_pages != nullptr);
}

PageCount
PageManager::calc_first_page_info_index(PageCount page_count)
{
    return std::ceil((page_count * sizeof(struct page_info_on_disk) + (sizeof(struct tsdb_header))) / (double)g_page_size);
}

bool
PageManager::open_mmap(PageCount page_count)
{
    struct stat sb;
    bool is_new = ! file_exists(m_file_name);

    Logger::debug("Trying to open file %s...", m_file_name.c_str());
    m_fd = open(m_file_name.c_str(), O_CREAT|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    m_fd = FileDescriptorManager::dup_fd(m_fd, FileDescriptorType::FD_FILE);

    if (m_fd == -1)
    {
        Logger::error("Failed to open file %s, errno = %d", m_file_name.c_str(), errno);
        return false;
    }

    if (fstat(m_fd, &sb) == -1)
    {
        Logger::error("Failed to fstat file %s, errno = %d", m_file_name.c_str(), errno);
        return false;
    }

    if ((0 != sb.st_size) && (m_total_size != sb.st_size))
    {
        m_total_size = sb.st_size;
    }

    Logger::debug("File size: %" PRIu64, m_total_size);

    int rc = ftruncate(m_fd, m_total_size);

    if (rc != 0)
    {
        Logger::error("Failed to resize file %s, errno = %d", m_file_name.c_str(), errno);
        return false;
    }

    m_pages = mmap64(nullptr,
                     m_total_size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     m_fd,
                     0);

    if (m_pages == MAP_FAILED)
    {
        Logger::error("Failed to mmap file %s, errno = %d", m_file_name.c_str(), errno);
        m_pages = nullptr;

        if (m_fd > 0)
        {
            close(m_fd);
            m_fd = -1;
        }

        if (is_new)
        {
            // Meta file not ready yet. Delete the data file to be safe.
            if (rm_file(m_file_name.c_str()) == 0)
            {
                Logger::info("Due to mmap failure, remove newly created file %s", m_file_name.c_str());
            }
            else
            {
                Logger::error("Mmap fails, but unable to remove newly created file %s", m_file_name.c_str());
            }
        }
        return false;
    }

    rc = madvise(m_pages, m_total_size, MADV_RANDOM);

    if (rc != 0)
        Logger::info("Failed to madvise(RANDOM), page = %p, errno = %d", m_pages, errno);

    struct tsdb_header *header = reinterpret_cast<struct tsdb_header*>(m_pages);

    m_page_count = &(header->m_page_count);
    m_page_index = &(header->m_page_index);
    m_header_index = &(header->m_header_index);
    m_actual_pg_cnt = &(header->m_actual_pg_cnt);

    m_page_info = reinterpret_cast<struct page_info_on_disk*>(static_cast<char*>(m_pages)+(sizeof(struct tsdb_header)));

    if (sb.st_size == 0)
    {
        // new file
        header->m_major_version = m_major_version;
        header->m_minor_version = m_minor_version;
        header->m_start_tstamp = m_time_range.get_from();
        header->m_end_tstamp = m_time_range.get_to();
        header->set_compacted(m_compacted);
        header->set_compressor_version(m_compressor_version);
        header->set_millisecond(g_tstamp_resolution_ms);
        *m_page_count = page_count;
        *m_page_index = calc_first_page_info_index(page_count);
        *m_header_index = 0;
        *m_actual_pg_cnt = page_count;
        ASSERT(*m_page_index <= *m_actual_pg_cnt);
    }
    else
    {
        if (m_major_version != header->m_major_version)
        {
            Logger::fatal("file major version: %d, our major version: %d",
                header->m_major_version, m_major_version);
        }

        if (m_minor_version != header->m_minor_version)
        {
            Logger::warn("file minor version: %d, our minor version: %d",
                header->m_minor_version, m_minor_version);
        }

        int compressor_version = header->get_compressor_version();

        if (m_compressor_version != compressor_version)
        {
            Logger::warn("file compressor version: %d, our compressor version: %d, switching to %d",
                compressor_version, m_compressor_version, compressor_version);
            m_compressor_version = compressor_version;
        }

        if (g_tstamp_resolution_ms != header->is_millisecond())
        {
            Logger::fatal("timestamp unit in config different than in data file");
            throw std::range_error("timestamp unit in config different than in data file");
        }

        m_compacted = header->is_compacted();
        m_total_size = (TsdbSize)*m_actual_pg_cnt * (TsdbSize)g_page_size;
        ASSERT(*m_page_index <= *m_actual_pg_cnt);

        // TODO: verify time range in the header. It should agree with our m_time_range!
    }

    Logger::debug("page count = %d", *m_page_count);
    Logger::debug("page index = %d", *m_page_index);
    return is_new;
}

void
PageManager::close_mmap()
{
    if (m_pages != nullptr)
    {
        // TODO: Do we need to do a flush first?
        munmap(m_pages, m_total_size);
        m_pages = nullptr;

        if (m_fd > 0)
        {
            close(m_fd);
            m_fd = -1;
        }

        // these need to be invalidated
        m_page_count = nullptr;
        m_page_index = nullptr;
        m_header_index = nullptr;
        m_page_info = nullptr;
    }
}

struct page_info_on_disk *
PageManager::get_page_info_on_disk(PageCount index)
{
    ASSERT(m_page_count != nullptr);
    ASSERT(index < (*m_page_count));
    return &m_page_info[index];
}

PageInfo *
PageManager::get_free_page_on_disk(Tsdb *tsdb, bool ooo)
{
    PageInfo *info =
        (PageInfo*)MemoryManager::alloc_recyclable(RecyclableType::RT_PAGE_INFO);

    if (info == nullptr)
    {
        // TODO: handle OOM
        Logger::fatal("Running out of memory!");
        return nullptr;
    }

    // get a new, unsed page from mmapped file
    std::lock_guard<std::mutex> guard(m_lock);

    if (((*m_page_index) < (*m_actual_pg_cnt)) && ((*m_header_index) < (*m_page_count)))
    {
        PageCount id = *m_header_index;
        PageCount page_idx = *m_page_index;
        struct page_info_on_disk *header = get_page_info_on_disk(id);
        info->init_for_disk(this, header, page_idx, g_page_size, ooo);
        info->setup_compressor(m_time_range, (ooo ? 0 : m_compressor_version));
        ASSERT(info->is_out_of_order() == ooo);

        (*m_page_index)++;
        (*m_header_index)++;
    }
    else
    {
        // TODO: need to open another mmapped file
        MemoryManager::free_recyclable(info);
        info = nullptr;
        Logger::debug("Running out of pages!");
    }

    ASSERT(*m_page_index <= *m_actual_pg_cnt);
    return info;
}

PageInfo *
PageManager::get_free_page_for_compaction(Tsdb *tsdb)
{
    PageInfo *info =
        (PageInfo*)MemoryManager::alloc_recyclable(RecyclableType::RT_PAGE_INFO);

    if (info == nullptr)
    {
        // TODO: handle OOM
        Logger::fatal("Running out of memory!");
        return nullptr;
    }

    // get a new, unsed page from mmapped file
    std::lock_guard<std::mutex> guard(m_lock);

    if (((*m_page_index) < (*m_actual_pg_cnt)) && ((*m_header_index) < (*m_page_count)))
    {
        PageCount id = *m_header_index;
        PageCount page_idx = *m_page_index;

        struct page_info_on_disk *header = get_page_info_on_disk(id);
        info->init_for_disk(this, header, page_idx, g_page_size, false);

        (*m_header_index)++;

        if (id > 0)
        {
            // Check to see if last page is completely full.
            // If not, use the remaining space.
            struct page_info_on_disk *header = get_page_info_on_disk(id - 1);
            PageSize offset = header->m_offset + header->m_size;

            if ((g_page_size - offset) >= 12) // at least 12 bytes left
            {
                info->m_header->m_page_index = header->m_page_index;
                info->m_header->m_offset = offset;
                info->m_header->m_size = g_page_size - offset;
            }
            else
            {
                (*m_page_index)++;
                info->m_header->m_page_index = header->m_page_index + 1;
            }
        }

        info->setup_compressor(m_time_range, m_compressor_version);
    }
    else
    {
        // TODO: need to open another mmapped file
        MemoryManager::free_recyclable(info);
        info = nullptr;
        Logger::debug("Running out of pages!");
    }

    ASSERT(*m_page_index <= *m_actual_pg_cnt);
    return info;
}

// get an occupied mmapped page
PageInfo *
PageManager::get_the_page_on_disk(PageCount header_index)
{
    ASSERT(m_pages != nullptr);
    ASSERT(m_page_count != nullptr);

    if (*m_page_count <= header_index)
    {
        return nullptr;
    }

    PageInfo *info =
        (PageInfo*)MemoryManager::alloc_recyclable(RecyclableType::RT_PAGE_INFO);
    ASSERT(info != nullptr);
    struct page_info_on_disk *header = get_page_info_on_disk(header_index);
    ASSERT(header != nullptr);
    info->init_from_disk(this, header);
    return info;
}

PageCount
PageManager::calc_page_info_index(struct page_info_on_disk *piod) const
{
    ASSERT(piod >= m_page_info);
    PageCount idx = ((char*)piod - (char*)m_page_info) / sizeof(struct page_info_on_disk);
    ASSERT(m_page_count != nullptr);
    ASSERT(idx < (*m_page_count));
    return idx;
}

void
PageManager::flush(bool sync)
{
    if (m_pages == nullptr) return;

    ASSERT(m_page_index != nullptr);
    TsdbSize size = *m_page_index * g_page_size;
    if (size > m_total_size) size = m_total_size;   // could happen after compaction
    int rc = msync(m_pages, size, (sync?MS_SYNC:MS_ASYNC));

    if (rc == -1)
        Logger::info("Failed to flush file %s, errno = %d", m_file_name.c_str(), errno);

    rc = madvise(m_pages, m_total_size, MADV_DONTNEED);

    if (rc == -1)
    {
        Logger::info("Failed to madvise(DONTNEED), page = %p, size = %" PRIu64 ", errno = %d",
            m_pages, m_total_size, errno);
    }
}

void
PageManager::persist()
{
    if (m_pages == nullptr) return;

    ASSERT(m_page_index != nullptr);
    TsdbSize size = *m_page_index * g_page_size;
    ASSERT(size <= m_total_size);
    msync(m_pages, size, MS_SYNC);
}

bool
PageManager::resize(TsdbSize old_size)
{
    ASSERT(m_fd != -1);

    if (old_size == m_total_size) return false;

    int rc = ftruncate(m_fd, m_total_size);

    if (rc != 0)
    {
        Logger::error("Failed to resize data file, errno = %d", errno);
        return false;
    }

    void *pages = mremap(m_pages, old_size, m_total_size, 0);

    if (pages != m_pages)
    {
        Logger::error("Failed to resize data file, errno = %d, pages = %p", errno, pages);
        return false;
    }

    return true;
}

void
PageManager::shrink_to_fit()
{
    TsdbSize old_total_size = m_total_size;
    PageCount id = *m_header_index - 1;
    struct page_info_on_disk *header = get_page_info_on_disk(id);
    PageCount last = header->m_page_index + 1;
    *m_actual_pg_cnt = last;
    ASSERT(*m_page_index <= *m_actual_pg_cnt);
    m_total_size = last * g_page_size;
    persist_compacted_flag(true);
    Logger::debug("shrink from %" PRIu64 " to %" PRIu64, old_total_size, m_total_size);
    resize(old_total_size);
}

void
PageManager::persist_compacted_flag(bool compacted)
{
    m_compacted = compacted;
    ASSERT(m_pages != nullptr);
    struct tsdb_header *header = reinterpret_cast<struct tsdb_header*>(m_pages);
    header->set_compacted(m_compacted);
}

double
PageManager::get_page_percent_used() const
{
    if ((m_page_index == nullptr) || (m_actual_pg_cnt == nullptr)) return 0.0;
    if (*m_actual_pg_cnt == 0) return 0.0;
    return ((double)*m_page_index / (double)*m_actual_pg_cnt) * 100.0;
}


}
