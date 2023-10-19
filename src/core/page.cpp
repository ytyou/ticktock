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


bool
PageInMemory::is_full()
{
    if (m_compressor != nullptr)
        return m_compressor->is_full();
    else
        return get_page_header()->is_full();
}

bool
PageInMemory::is_empty()
{
    if (m_compressor != nullptr)
        return m_compressor->is_empty();
    else
    {
        struct compress_info_on_disk *ciod = get_compress_header();
        return ciod == nullptr || ciod->is_empty();
    }
}

TimeRange
PageInMemory::get_time_range()
{
    struct page_info_on_disk *header = get_page_header();
    ASSERT(header != nullptr);
    return TimeRange(m_tstamp_from + m_start, header->m_tstamp_to + m_start);
}

int
PageInMemory::in_range(Timestamp tstamp) const
{
    ASSERT(m_tsdb != nullptr);
    return m_tsdb->in_range(tstamp);
}

/* 'range' should be the time range of the Tsdb.
 */
void
PageInMemory::setup_compressor(const TimeRange& range, PageSize page_size, int compressor_version)
{
    if (m_compressor != nullptr)
    {
        MemoryManager::free_recyclable(m_compressor);
        m_compressor = nullptr;
    }

    PageSize compress_info_size = sizeof(struct compress_info_on_disk);
    RecyclableType type =
        (RecyclableType)(compressor_version + RecyclableType::RT_COMPRESSOR_V0);
    m_compressor = (Compressor*)MemoryManager::alloc_recyclable(type);
    m_compressor->init(range.get_from(),
                       reinterpret_cast<uint8_t*>(m_page) + compress_info_size,
                       page_size - compress_info_size);
}

void
PageInMemory::update_indices(PageInMemory *info)
{
    ASSERT(info != nullptr);

    if (m_tsdb != info->m_tsdb)
        return;

    struct page_info_on_disk *this_header = get_page_header();
    struct page_info_on_disk *that_header = info->get_page_header();

    this_header->m_next_file = that_header->m_next_file;
    this_header->m_next_header = that_header->m_next_header;
}

Timestamp
PageInMemory::get_last_tstamp(MetricId mid, TimeSeriesId tid) const
{
    ASSERT(m_tsdb != nullptr);
    ASSERT(m_compressor != nullptr);
    ASSERT(! m_compressor->is_empty());

    if (m_compressor->is_empty())
        return m_tsdb->get_last_tstamp(mid, tid);
    else
        return m_compressor->get_last_tstamp();
}

void
PageInMemory::get_all_data_points(DataPointVector& dps)
{
    ASSERT(m_compressor != nullptr);
    m_compressor->uncompress(dps);
}

int
PageInMemory::get_dp_count() const
{
    return (m_compressor == nullptr) ? 0 : m_compressor->get_dp_count();
}


PageInMemory::PageInMemory(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, PageSize actual_size) :
    m_compressor(nullptr),
    m_page(nullptr),
    m_tsdb(nullptr),
    m_start(0)
{
    m_page_header.init();
    ASSERT(m_page == nullptr);
    init(mid, tid, tsdb, is_ooo, actual_size);
}

// used during restore
PageInMemory::PageInMemory(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx) :
    m_compressor(nullptr),
    m_page(nullptr),
    m_tsdb(nullptr),
    m_start(0)
{
    m_page_header.init();
    ASSERT(m_page == nullptr);
    init(mid, tid, tsdb, is_ooo, file_idx, header_idx);
}

PageInMemory::~PageInMemory()
{
    if (m_tsdb != nullptr)
        m_tsdb->dec_ref_count();

    if (m_compressor != nullptr)
        MemoryManager::free_recyclable(m_compressor);

    if (m_page != nullptr)
        std::free(m_page);
}

void
PageInMemory::init(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, PageSize actual_size)
{
    if (tsdb == nullptr) tsdb = m_tsdb; // same tsdb
    ASSERT(actual_size <= tsdb->get_page_size());

    // preserve m_page_header.m_next_file and m_page_header.m_next_header, if
    // tsdb remains the same...
    Timestamp from = tsdb->get_time_range().get_from();
    FileIndex file_idx = m_page_header.m_next_file;
    HeaderIndex header_idx = m_page_header.m_next_header;

    m_page_header.init();
    m_page_header.set_out_of_order(is_ooo);

    if (m_tsdb == tsdb)    // same tsdb
    {
        m_page_header.m_next_file = file_idx;
        m_page_header.m_next_header = header_idx;
        ASSERT((file_idx == TT_INVALID_FILE_INDEX) || (tsdb->get_header_file(mid, file_idx) != nullptr));
    }
    else
    {
        if (m_tsdb != nullptr)
            m_tsdb->dec_ref_count();

        m_tsdb = tsdb;
        m_start = from;

        // need to locate the last page of this TS
        tsdb->get_last_header_indices(mid, tid, file_idx, header_idx);
        m_page_header.m_next_file = file_idx;
        m_page_header.m_next_header = header_idx;
        ASSERT((file_idx == TT_INVALID_FILE_INDEX) || (tsdb->get_header_file(mid, file_idx) != nullptr));
    }

    if (m_page == nullptr)
        m_page = malloc(tsdb->get_page_size());

    if (actual_size == 0)
        actual_size = tsdb->get_page_size();

    int compressor_version = is_ooo ? 0 : tsdb->get_compressor_version();
    setup_compressor(tsdb->get_time_range(), actual_size, compressor_version);
}

// used during restore
void
PageInMemory::init(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx)
{
    ASSERT(tsdb != nullptr);
    ASSERT(m_tsdb == nullptr);

    // preserve m_page_header.m_next_file and m_page_header.m_next_header, if
    // tsdb remains the same...
    Timestamp from = tsdb->get_time_range().get_from();

    m_page_header.init();
    m_page_header.set_out_of_order(is_ooo);

    m_tsdb = tsdb;
    m_start = from;

    m_page_header.m_next_file = file_idx;
    m_page_header.m_next_header = header_idx;
    ASSERT((file_idx == TT_INVALID_FILE_INDEX) || (tsdb->get_header_file(mid, file_idx) != nullptr));

    ASSERT(m_page == nullptr);
    m_page = malloc(tsdb->get_page_size());
    PageSize actual_size = tsdb->get_page_size();
    int compressor_version = is_ooo ? 0 : tsdb->get_compressor_version();
    setup_compressor(tsdb->get_time_range(), actual_size, compressor_version);
}

PageSize
PageInMemory::flush(MetricId mid, TimeSeriesId tid, bool compact)
{
    if (m_compressor->is_empty()) return 0;

    // [m_page_header.m_next_file, m_page_header.m_next_header] is the
    // indices of the previous page, if any.
    FileIndex prev_file_idx = m_page_header.m_next_file;
    HeaderIndex prev_header_idx = m_page_header.m_next_header;

    CompressorPosition position;
    m_compressor->save(position);
    m_compressor->save((uint8_t*)m_page + sizeof(struct compress_info_on_disk));

    struct compress_info_on_disk *ciod = get_compress_header();
    ciod->m_cursor = position.m_offset;
    ciod->m_start = position.m_start;

    //m_page_header.m_size = m_compressor->size();
    m_page_header.set_full(m_compressor->is_full());
    m_page_header.m_next_file = TT_INVALID_FILE_INDEX;
    m_page_header.m_next_header = TT_INVALID_HEADER_INDEX;

    return m_tsdb->append_page(mid, tid, prev_file_idx, prev_header_idx, &m_page_header, m_tstamp_from, m_page, compact);

    // re-initialize the compressor
    //m_compressor->init(m_start, (uint8_t*)m_page, m_tsdb->get_page_size());
    //ASSERT(m_page != nullptr);
}

void
PageInMemory::append(MetricId mid, TimeSeriesId tid, FILE *file)
{
    if (m_compressor == nullptr) return;

    CompressorPosition position;
    m_compressor->save(position);

    struct append_log_entry header =
        {
            .mid = mid,
            .tid = tid,
            .tstamp = m_compressor->get_start_tstamp(),
            .offset = position.m_offset,
            .start = position.m_start,
            .is_ooo = is_out_of_order() ? (uint8_t)1 : (uint8_t)0,
            .file_idx = m_page_header.m_next_file,
            .header_idx = m_page_header.m_next_header
        };

    int ret;
    ret = fwrite(&header, 1, sizeof(header), file);
    if (ret != sizeof(header)) Logger::error("PageInMemory::append() failed");
    ret = fwrite(((uint8_t*)m_page + sizeof(struct compress_info_on_disk)), 1, position.m_offset, file);
    //std::fflush(file);
    if (ret != position.m_offset) Logger::error("PageInMemory::append() failed");
    ASSERT(m_page != nullptr);
}

void
PageInMemory::restore(Timestamp tstamp, uint8_t *buff, PageSize offset, uint8_t start)
{
    ASSERT(buff != nullptr);
    ASSERT(m_page != nullptr);
    ASSERT(m_compressor != nullptr);

    DataPointVector dps;
    CompressorPosition position(offset, start);
    m_compressor->set_start_tstamp(tstamp);
    m_compressor->restore(dps, position, buff);
    //m_compressor->restore(dps, position, buff + sizeof(struct compress_info_on_disk));

    struct page_info_on_disk *header = get_page_header();
    ASSERT(header != nullptr);

    m_start = tstamp;
    for (auto dp: dps)
    {
        uint32_t ts = dp.first - m_start;
        if (ts < m_tstamp_from)
            m_tstamp_from = ts;
        ts++;
        if (m_page_header.m_tstamp_to < ts)
            m_page_header.m_tstamp_to = ts;
    }
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
        if (ts < m_tstamp_from)
            m_tstamp_from = ts;
        ts++;
        if (m_page_header.m_tstamp_to < ts)
            m_page_header.m_tstamp_to = ts;
    }
    return success;
}


#if 0
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
#endif


}
