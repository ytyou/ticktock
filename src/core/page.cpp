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
#include <limits>
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
#include "mmap.h"
#include "page.h"
#include "tsdb.h"
#include "logger.h"


namespace tt
{


PageInfo::PageInfo() :
    m_compressor(nullptr),
    m_page(nullptr),
    m_start(0)
{
}

bool
PageInfo::is_full()
{
    if (m_compressor != nullptr)
        return m_compressor->is_full();
    else
        return get_page_header()->is_full();
}

bool
PageInfo::is_empty()
{
    if (m_compressor != nullptr)
        return m_compressor->is_empty();
    else
        return get_page_header()->is_empty();
}

TimeRange
PageInfo::get_time_range()
{
    struct page_info_on_disk *header = get_page_header();
    ASSERT(header != nullptr);
    return TimeRange(header->m_tstamp_from + m_start, header->m_tstamp_to + m_start);
}

/* 'range' should be the time range of the Tsdb.
 */
void
PageInfo::setup_compressor(const TimeRange& range, PageSize page_size, int compressor_version)
{
    if (m_compressor != nullptr)
    {
        MemoryManager::free_recyclable(m_compressor);
        m_compressor = nullptr;
    }

    RecyclableType type =
        (RecyclableType)(compressor_version + RecyclableType::RT_COMPRESSOR_V0);
    m_compressor = (Compressor*)MemoryManager::alloc_recyclable(type);
    m_compressor->init(range.get_from(), reinterpret_cast<uint8_t*>(m_page), page_size);
}

Timestamp
PageInfo::get_last_tstamp() const
{
    ASSERT(m_compressor != nullptr);
    return m_compressor->get_last_tstamp();
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


PageInMemory::PageInMemory(TimeSeriesId id, Tsdb *tsdb, bool is_ooo)
{
    m_page_header.init();
    init(id, tsdb, is_ooo);
}

void
PageInMemory::init(TimeSeriesId id, Tsdb *tsdb, bool is_ooo)
{
    ASSERT(tsdb != nullptr);

    // preserve m_page_header.m_next_file and m_page_header.m_next_header, if
    // tsdb remains the same...
    Timestamp from = tsdb->get_time_range().get_from();
    FileIndex file_idx = m_page_header.m_next_file;
    HeaderIndex header_idx = m_page_header.m_next_header;

    m_page_header.init();
    m_page_header.set_out_of_order(is_ooo);

    if (m_start == from)    // same tsdb
    {
        m_page_header.m_next_file = file_idx;
        m_page_header.m_next_header = header_idx;
    }
    else
    {
        m_start = from;

        // need to locate the last page of this TS
        tsdb->get_last_header_indices(id, file_idx, header_idx);
        m_page_header.m_next_file = file_idx;
        m_page_header.m_next_header = header_idx;
    }

    PageSize page_size = tsdb->get_page_size();

    if (m_page == nullptr)
    {
        if (page_size == g_page_size)
            m_page = MemoryManager::alloc_memory_page();
        else
            m_page = malloc(page_size);
    }

    int compressor_version = is_ooo ? 0 : tsdb->get_compressor_version();
    setup_compressor(tsdb->get_time_range(), page_size, compressor_version);
}

void
PageInMemory::flush(TimeSeriesId id, Tsdb *tsdb)
{
    ASSERT(tsdb != nullptr);
    ASSERT(m_compressor != nullptr);

    if (m_compressor->is_empty()) return;

    // [m_page_header.m_next_file, m_page_header.m_next_header] is the
    // indices of the previous page, if any.
    FileIndex prev_file_idx = m_page_header.m_next_file;
    HeaderIndex prev_header_idx = m_page_header.m_next_header;

    CompressorPosition position;
    m_compressor->save(position);

    m_page_header.m_cursor = position.m_offset;
    m_page_header.m_start = position.m_start;
    m_page_header.set_full(m_compressor->is_full());
    m_page_header.m_next_file = TT_INVALID_FILE_INDEX;
    m_page_header.m_next_header = TT_INVALID_HEADER_INDEX;

    tsdb->append_page(id, prev_file_idx, prev_header_idx, &m_page_header, m_page);

    // re-initialize the compressor
    m_compressor->init(m_start, (uint8_t*)m_page, tsdb->get_page_size());
}

void
PageInMemory::append(TimeSeriesId id, FILE *file)
{
    if (m_compressor == nullptr) return;

    CompressorPosition position;
    m_compressor->save(position);

    struct append_log_entry header =
        {
            .id = id,
            .tstamp = m_compressor->get_start_tstamp(),
            .offset = position.m_offset,
            .start = position.m_start,
            .is_ooo = is_out_of_order() ? (uint8_t)1 : (uint8_t)0
        };

    int ret;
    ret = fwrite(&header, 1, sizeof(header), file);
    if (ret != sizeof(header)) Logger::error("PageInMemory::append() failed");
    ret = fwrite(m_page, 1, position.m_offset, file);
    if (ret != position.m_offset) Logger::error("PageInMemory::append() failed");
}

bool
PageInMemory::add_data_point(Timestamp tstamp, double value)
{
    ASSERT(m_compressor != nullptr);
    bool success = m_compressor->compress(tstamp, value);
    if (success)
    {
        ASSERT(m_start <= tstamp);
        uint32_t ts = tstamp - m_start;
        if (ts < m_page_header.m_tstamp_from)
            m_page_header.m_tstamp_from = ts;
        ts++;
        if (m_page_header.m_tstamp_to < ts)
            m_page_header.m_tstamp_to = ts;
    }
    return success;
}


void
PageOnDisk::init(Tsdb *tsdb,
                 struct page_info_on_disk *header,
                 FileIndex file_idx,
                 HeaderIndex header_idx,
                 void *page,
                 bool is_ooo)
{
    ASSERT(tsdb != nullptr);
    ASSERT(page != nullptr);

    m_tsdb = tsdb;
    m_page = page;
    m_file_index = file_idx;
    m_header_index = header_idx;
    m_page_header = header;

    m_start = 0;
    m_compressor = nullptr;
}

PageIndex
PageOnDisk::get_global_page_index()
{
    PageCount page_count = m_tsdb->get_page_count();
    return (m_file_index * page_count) + m_page_header->m_page_index;
}

bool
PageOnDisk::recycle()
{
    if (m_compressor != nullptr)
    {
        MemoryManager::free_recyclable(m_compressor);
        m_compressor = nullptr;
    }

    return true;
}


}
