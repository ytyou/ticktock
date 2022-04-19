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
#include "range.h"
#include "recycle.h"
#include "serial.h"
#include "type.h"


namespace tt
{


class Compressor;
class MemoryManager;
class PageManager;
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
struct tsdb_header
{
    uint8_t m_major_version;    //  8-bit
    uint16_t m_minor_version;   // 16-bit
    uint8_t m_flags;            // 8-bit
    PageCount m_page_count;     // 32-bit
    PageCount m_header_index;   // 32-bit
    PageCount m_page_index;     // 32-bit
    Timestamp m_start_tstamp;   // 64-bit
    Timestamp m_end_tstamp;     // 64-bit
    PageCount m_actual_pg_cnt;  // 32-bit

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

    inline void set_millisecond(bool milli)
    {
        m_flags = milli ? (m_flags | 0x40) : (m_flags & 0xBF);
    }
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
struct page_info_on_disk
{
    PageSize m_offset;          // 16-bit
    PageSize m_size;            // 16-bit
    PageSize m_cursor;          // 16-bit
    uint8_t m_start;            //  8-bit
    uint8_t m_flags;            //  8-bit
    PageCount m_page_index;     // 32-bit
    uint32_t m_tstamp_from;     // 32-bit
    uint32_t m_tstamp_to;       // 32-bit

    void init(const TimeRange& range)
    {
        m_offset = m_size = m_cursor = 0;
        m_start = m_flags = 0;
        m_page_index = 0;
        m_tstamp_from = 0;
        m_tstamp_to = range.get_duration();
    }

    void init(PageSize cursor, uint8_t start, bool is_full, uint32_t from, uint32_t to)
    {
        m_cursor = cursor;
        m_start = start;
        m_tstamp_from = from;
        m_tstamp_to = to;
        set_full(is_full);
    }

    inline bool is_full() const { return ((m_flags & 0x01) != 0); }
    inline bool is_out_of_order() const { return ((m_flags & 0x02) != 0); }
    inline bool is_empty() const { return ((m_cursor == 0) && (m_start == 0)); }

    inline void set_full(bool full) { m_flags = (full ? (m_flags | 0x01) : (m_flags & 0xFE)); }
    inline void set_out_of_order(bool ooo) { m_flags = (ooo ? (m_flags | 0x02) : (m_flags & 0xFD)); }

    char *c_str(char *buff, size_t size)
    {
        snprintf(buff, size, "off=%d size=%d curr=%d start=%d flags=%x idx=%d from=%d to=%d",
            m_offset, m_size, m_cursor, m_start, m_flags, m_page_index, m_tstamp_from, m_tstamp_to);
        return buff;
    }
};


// these are what we keep in memory for a page;
// part of it (see struct page_info_on_disk) will be
// persisted on disk;
class PageInfo : public Serializable, public Recyclable
{
public:
    PageInfo();

    bool recycle(); // called by MemoryManager when going back to free pool

    // this one initialize PageInfo to represent a new page on disk
    void init_for_disk(PageManager *pm,
                       struct page_info_on_disk *header,
                       PageCount page_idx,
                       PageSize size,
                       bool is_ooo);

    // init a page info representing an existing page on disk
    void init_from_disk(PageManager *pm, struct page_info_on_disk *header);

    inline void set_ooo(bool ooo) { m_header->set_out_of_order(ooo); }

    // prepare to be used to represent a different page
    void flush();
    void reset();
    void shrink_to_fit();
    bool is_full() const;
    bool is_empty() const;
    inline bool is_on_disk() const { return true; }
    inline bool is_out_of_order() const { return m_header->is_out_of_order(); }
    void ensure_dp_available(DataPointVector *dps = nullptr);

    Timestamp get_last_tstamp() const;

    inline const TimeRange& get_time_range()
    {
        return m_time_range;
    }

    int get_dp_count() const;
    PageCount get_id() const;
    PageCount get_file_id() const;
    int get_page_order() const;

    inline PageCount get_page_index() const
    {
        return m_header->m_page_index;
    }

    // return true if dp is added; false if page is full;
    bool add_data_point(Timestamp tstamp, double value);

    // 'dps' will NOT be cleared first; data points will be accumulated
    // into 'dps';
    void get_all_data_points(DataPointVector& dps);

    // existing compressor, if any, will be destroyed, and new one created
    void setup_compressor(const TimeRange& range, int compressor_version);
    void persist(bool copy_data = false);   // persist page to disk
    void merge_after(PageInfo *dst);        // share page with 'dst' (compaction)
    void copy_to(PageCount dst_id);

    inline size_t c_size() const override { return 64; }
    const char *c_str(char *buff) const override;

private:
    friend class PageManager;
    friend class MemoryManager;
    friend class SanityChecker;
    friend class page_info_index_less;

    void *get_page();

    TimeRange m_time_range;     // range of actual data points in this page
    PageManager *m_page_mgr;    // this is null for in-memory page
    Compressor *m_compressor;   // this is null except for in-memory page

    struct page_info_on_disk *m_header;

};  // class PageInfo


class page_info_index_less
{
public:
    bool operator()(PageInfo *info1, PageInfo *info2) const
    {
        return info1->m_header->m_page_index < info2->m_header->m_page_index;
    }
};


/* Each PageManager represent a mmapp'ed file on disk. Each Tsdb represents
 * a segment (by time) of the database, which could consist of multiple
 * mmapp'ed files. Hence each Tsdb can contain more than one PageManager.
 */
class PageManager
{
public:
    PageManager(TimeRange& range, PageCount id = 0, bool temp = false);
    PageManager(const PageManager& copy) = delete;
    virtual ~PageManager();

    PageInfo *get_free_page_on_disk(Tsdb *tsdb, bool ooo);
    PageInfo *get_free_page_for_compaction(Tsdb *tsdb); // used during compaction
    PageInfo *get_the_page_on_disk(PageCount header_index);
    PageCount calc_page_info_index(struct page_info_on_disk *piod) const;

    inline const TimeRange& get_time_range()
    {
        return m_time_range;
    }

    inline PageCount get_id() const
    {
        return m_id;
    }

    inline PageCount get_data_page_count() const    // no. data pages currently in use
    {
        ASSERT(m_actual_pg_cnt != nullptr);
        return *m_actual_pg_cnt - calc_first_page_info_index(*m_page_count);
    }

    inline uint8_t get_compressor_version() const
    {
        return m_compressor_version;
    }

    void flush(bool sync);
    void close_mmap();
    bool reopen();  // return false if reopen() failed
    void persist();
    void shrink_to_fit();

    inline bool is_open() const { return (m_pages != nullptr); }
    inline bool is_full() const
    {
        ASSERT(m_page_index != nullptr);
        ASSERT(m_page_count != nullptr);
        return ((*m_page_index) >= (*m_page_count));
    }
    inline bool is_compacted() const { return m_compacted; }
    double get_page_percent_used() const;

    inline uint8_t *get_first_page() const { return static_cast<uint8_t*>(m_pages); }
    inline PageCount get_page_count() const { return *m_page_count; }
    inline static int32_t get_mmap_file_count() { return m_total.load(); }

private:
    bool open_mmap(PageCount page_count);
    void persist_compacted_flag(bool compacted);
    bool resize(TsdbSize old_size);     // resize (shrink) the data file
    void init_headers();    // zero-out headers
    struct page_info_on_disk *get_page_info_on_disk(PageCount index);
    static PageCount calc_first_page_info_index(PageCount page_count);
    static std::atomic<int32_t> m_total;    // total number of open mmap files

    std::mutex m_lock;
    int m_fd;       // file-descriptor of the mmapp'ed file
    void *m_pages;  // points to the beginning of the mmapp'ed file

    // If one mmapp'ed file is not big enough to hold all the dps for
    // this Tsdb, we will create multiple of them, and m_id is a
    // zero-based index of these mmapp'ed files.
    PageCount m_id;
    std::string m_file_name;

    // these are serialized
    uint8_t m_major_version;
    uint16_t m_minor_version;
    uint8_t m_compressor_version;
    bool m_compacted;

    // number of pages in the file when it is newly created;
    // after compaction, the actual page count could change,
    // but this one will NOT change; we need this to remain
    // constant because it determines the physical layout of
    // the page.
    PageCount *m_page_count;

    // the index of the next free page info header in this file
    PageCount *m_header_index;

    // the index of the next free page in this file; initially
    // it will be the index of the first page (the one right
    // after all the page info headers); note that it will never
    // be 0 because the first few pages in the file are always
    // occupied by tsdb header and page info headers;
    // TODO: choose a better name
    PageCount *m_page_index;

    // usually this is the same as m_page_count; after compaction
    // this could be different than m_page_count;
    PageCount *m_actual_pg_cnt;

    // total size of the data file, in bytes. this is usually
    // m_page_count * page_size, but after compaction, it should
    // be m_actual_pg_cnt * page_size;
    TsdbSize m_total_size;

    // time range in which all data points in this data file should
    // fall into. time unit used in it should be the same as specified
    // in the config file.
    TimeRange m_time_range;

    struct page_info_on_disk *m_page_info;

};  // class PageManager


}
