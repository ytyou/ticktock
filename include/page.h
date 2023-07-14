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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include "mmap.h"
#include "range.h"
#include "recycle.h"
#include "serial.h"
#include "type.h"


namespace tt
{


class Compressor;
class Tsdb;


/* m_major_version: major version of ticktock that created this file;
 *                  different major versions are considered imcompatible;
 * m_minor_version: minor version of ticktock that created this file;
 *                  different minor versions should be compatible with
 *                  each other;
 * m_flags:         2 least significant bits represent compressor version
 *                  used in this file; it can be specified in the config
 *                  (tsdb.compressor.version); the most significant bit
 *                  indicates whether or not the file is compacted;
 *                  the second most significant bit indicates whether or
 *                  not timestamps are milliseconds; 1 means yes;
 *                  the rest of the bits are unused at the moment;
 * m_page_count:    total number of (4K) pages in the file, including
 *                  headers; this can be specified in the config file;
 *                  total size of the file: m_page_count * page_size(4K);
 * m_header_index:  index of the next unused header in the file; it starts
 *                  at 0;
 * m_page_index:    index of the next unused page in the file; note that
 *                  it does not start with 0, since page 0 is occupied by
 *                  the tsdb_header, followed by an array of page_info_on_disk
 *                  headers; so in an empty file this is usually much bigger
 *                  than 0; when m_page_index == m_page_count, this data
 *                  file is full;
 * m_start_tstamp:  timestamp of the earliest data points in the file;
 * m_end_tstamp:    timestamp of the latest data points in the file;
 * m_actual_pg_cnt: total number of pages in the file AFTER compaction;
 *                  it should be the same as m_page_count before compaction.
 */
struct __attribute__ ((__packed__)) tsdb_header
{
    uint8_t m_major_version;    //  8-bit
    uint16_t m_minor_version;   // 16-bit
    uint8_t m_flags;            //  8-bit
    PageCount m_page_count;     // 32-bit
    PageCount m_header_index;   // 32-bit
    PageCount m_page_index;     // 32-bit
    Timestamp m_start_tstamp;   // 64-bit
    Timestamp m_end_tstamp;     // 64-bit
    PageCount m_actual_pg_cnt;  // 32-bit
    PageSize m_page_size;       // 16-bit
    uint16_t m_reserved;        // 16-bit

    inline int get_compressor_version() const
    {
        return (int)(m_flags & 0x03);
    }

    inline void set_compressor_version(int version)
    {
        m_flags = (m_flags & 0xF6) | (uint8_t)version;
    }

    inline bool is_compacted() const
    {
        return ((m_flags & 0x80) != 0);
    }

    inline void set_compacted(bool compacted)
    {
        m_flags = compacted ? (m_flags | 0x80) : (m_flags & 0x7F);
    }

    inline bool is_millisecond() const
    {
        return ((m_flags & 0x40) != 0);
    }

    bool is_full() const
    {
        return (m_page_index >= m_actual_pg_cnt) || (m_header_index >= m_page_count);
    }

    inline void set_millisecond(bool milli)
    {
        m_flags = milli ? (m_flags | 0x40) : (m_flags & 0xBF);
    }
};


struct __attribute__ ((__packed__)) compress_info_on_disk
{
    PageSize m_cursor;          // 16-bit
    uint8_t m_start;            //  8-bit

    inline bool is_empty() const { return ((m_cursor == 0) && (m_start == 0)); }
};


/* There will be an array of page_info_on_disk immediately following the
 * tsdb_header (defined above) at the beginning of each tsdb data file;
 * The number of page_info_on_disk in this array is determined by the
 * m_page_count in the tsdb_header.
 *
 * m_offset:        0 based starting position from which data will be stored;
 *                  if this physical page is shared between multiple time
 *                  series, some m_offset will not be zero;
 * m_size:          capacity of this page, in bytes; usually this is 4K,
 *                  but if this page is shared between multiple time series,
 *                  this will be less than 4K;
 * m_cursor:        used by compressors to save their last position;
 *                  when m_cursor and m_start are both 0, the page is empty
 * m_start:         used by compressors to save their last position;
 *                  when m_cursor and m_start are both 0, the page is empty
 * m_flags:         the least significant bit indicates whether or not the
 *                  page is full; the second least significant bit indicates
 *                  whether or not this is an out-of-order page;
 * m_page_index:    the index of the page where data can be found;
 * m_tstamp_from:   first timestamp on this page, RELATIVE TO the starting
 *                  timestamp of the Tsdb range, in either seconds or
 *                  milliseconds, depending on the resolution of the Tsdb.
 * m_tstamp_to:     last timestamp on this page, RELATIVE TO the starting
 *                  timestamp of the Tsdb range, in either seconds or
 *                  milliseconds, depending on the resolution of the Tsdb.
 */
struct __attribute__ ((__packed__)) page_info_on_disk
{
    PageSize m_offset;          // 16-bit
    PageSize m_size;            // 16-bit
    //PageSize m_cursor;          // 16-bit
    //uint8_t m_start;            //  8-bit
    uint8_t m_flags;            //  8-bit
    PageIndex m_page_index;     // 32-bit
    uint32_t m_tstamp_from;     // 32-bit
    uint32_t m_tstamp_to;       // 32-bit
    FileIndex m_next_file;      // 16-bit
    HeaderIndex m_next_header;  // 32-bit

    void init()
    {
        //m_offset = m_size = m_cursor = 0;
        //m_start = m_flags = 0;
        m_offset = m_size = 0;
        m_flags = 0;
        m_page_index = TT_INVALID_PAGE_INDEX;
        m_tstamp_from = UINT32_MAX;
        m_tstamp_to = 0;
        m_next_file = TT_INVALID_FILE_INDEX;
        m_next_header = TT_INVALID_HEADER_INDEX;
    }

    void init(struct page_info_on_disk *header)
    {
        ASSERT(header != nullptr);

        m_offset = header->m_offset;
        m_size = header->m_size;
        //m_cursor = header->m_cursor;
        //m_start = header->m_start;
        m_flags = header->m_flags;
        m_page_index = header->m_page_index;
        m_tstamp_from = header->m_tstamp_from;
        m_tstamp_to = header->m_tstamp_to;
        m_next_file = header->m_next_file;
        m_next_header = header->m_next_header;
    }

    void init(const TimeRange& range)
    {
        //m_offset = m_size = m_cursor = 0;
        //m_start = m_flags = 0;
        m_offset = m_size = 0;
        m_flags = 0;
        m_page_index = 0;
        m_tstamp_from = 0;
        m_tstamp_to = range.get_duration();
    }

    void init(PageSize cursor, uint8_t start, bool is_full, uint32_t from, uint32_t to)
    {
        //m_cursor = cursor;
        //m_start = start;
        m_tstamp_from = from;
        m_tstamp_to = to;
        set_full(is_full);
    }

    inline bool is_full() const { return ((m_flags & 0x01) != 0); }
    inline bool is_out_of_order() const { return ((m_flags & 0x02) != 0); }
    inline bool is_empty(struct compress_info_on_disk *ciod) const { return ((ciod->m_cursor == 0) && (ciod->m_start == 0)); }
    inline bool is_valid() const { return (m_page_index != TT_INVALID_PAGE_INDEX); }
    inline PageSize get_size() const { return m_size; }
    inline long int get_global_page_index(FileIndex file_idx, PageCount page_count) const
    {
        return ((long int)file_idx * (long int)page_count) + (long int)m_page_index;
    }

    inline void set_full(bool full) { m_flags = (full ? (m_flags | 0x01) : (m_flags & 0xFE)); }
    inline void set_out_of_order(bool ooo) { m_flags = (ooo ? (m_flags | 0x02) : (m_flags & 0xFD)); }

    inline FileIndex get_next_file() const { return m_next_file; }
    inline HeaderIndex get_next_header() const { return m_next_header; }

    char *c_str(char *buff, size_t size)
    {
        snprintf(buff, size, "off=%d size=%d flags=%x idx=%d from=%d to=%d",
            m_offset, m_size, m_flags, m_page_index, m_tstamp_from, m_tstamp_to);
        return buff;
    }
};


struct __attribute__ ((__packed__)) append_log_entry
{
    MetricId mid;
    TimeSeriesId tid;
    Timestamp tstamp;
    PageSize offset;
    uint8_t start;
    uint8_t is_ooo;
    FileIndex file_idx;
    HeaderIndex header_idx;
};


class __attribute__ ((__packed__)) PageInMemory
{
public:
    PageInMemory(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, PageSize actual_size = 0);
    PageInMemory(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx);
    ~PageInMemory();

    // prepare to be used to represent a different page
    bool is_full();
    bool is_empty();
    inline bool is_out_of_order() { return get_page_header()->is_out_of_order(); }

    Timestamp get_last_tstamp(MetricId mid, TimeSeriesId tid) const;
    TimeRange get_time_range();
    int in_range(Timestamp tstamp) const;
    inline Tsdb *get_tsdb() const { return m_tsdb; }

    int get_dp_count() const;
    PageCount get_file_id() const;
    int get_page_order() const;

    // 'dps' will NOT be cleared first; data points will be accumulated
    // into 'dps';
    void get_all_data_points(DataPointVector& dps);

    // existing compressor, if any, will be destroyed, and new one created
    void setup_compressor(const TimeRange& range, PageSize page_size, int compressor_version);

    void update_indices(PageInMemory *info);

    void init(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, PageSize actual_size = 0);
    void init(MetricId mid, TimeSeriesId tid, Tsdb *tsdb, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx);
    PageSize flush(MetricId mid, TimeSeriesId tid, bool compact = false);  // return next page size
    void append(MetricId mid, TimeSeriesId tid, FILE *file);
    void restore(Timestamp tstamp, uint8_t *buff, PageSize offset, uint8_t start);

    // return true if dp is added; false if page is full;
    bool add_data_point(Timestamp tstamp, double value);
    PageIndex get_global_page_index() { return TT_INVALID_PAGE_INDEX - 1; }

    inline struct page_info_on_disk *get_page_header()
    {
        return &m_page_header;
    }

    inline struct compress_info_on_disk *get_compress_header()
    {
        return reinterpret_cast<struct compress_info_on_disk*>(m_page);
    }

private:
    friend class DataFile;
    //friend class page_info_index_less;

    struct page_info_on_disk m_page_header;

protected:
    Tsdb *m_tsdb;
    void *m_page;
    Timestamp m_start;
    Compressor *m_compressor;

};  // class PageInMemory


#if 0
// This is used to write data.
class __attribute__ ((__packed__)) PageInMemory : public PageInfo
{
public:
    PageInMemory(TimeSeriesId id, Tsdb *tsdb, bool is_ooo, PageSize actual_size = 0);

    void init(TimeSeriesId id, Tsdb *tsdb, bool is_ooo, PageSize actual_size = 0);
    PageSize flush(TimeSeriesId id, bool compact = false);  // return next page size
    void append(TimeSeriesId id, FILE *file);

    // return true if dp is added; false if page is full;
    bool add_data_point(Timestamp tstamp, double value);
    PageIndex get_global_page_index() { return TT_INVALID_PAGE_INDEX - 1; }

    inline struct page_info_on_disk *get_page_header()
    {
        return &m_page_header;
    }

private:

    struct page_info_on_disk m_page_header;
};
#endif


#if 0
// This is used to read data.
class PageOnDisk : public PageInfo, public Recyclable
{
public:
    void init(Tsdb *tsdb,
              struct page_info_on_disk *header,
              FileIndex file_idx,
              HeaderIndex header_idx,
              void *page,
              bool is_ooo);
    bool recycle() override;    // called by MemoryManager when going back to free pool
    PageIndex get_global_page_index();

    inline struct page_info_on_disk *get_page_header()
    {
        return m_page_header;
    }

private:
    FileIndex m_file_index;     // of this page
    HeaderIndex m_header_index; // of this page
    struct page_info_on_disk *m_page_header;
};
#endif


/*
class page_info_index_less
{
public:
    bool operator()(PageInfo *info1, PageInfo *info2)
    {
        return info1->get_page_header()->m_page_index < info2->get_page_header()->m_page_index;
    }
};
*/


}
