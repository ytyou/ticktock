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

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "lock.h"
#include "range.h"
#include "type.h"
#include "utils.h"


namespace tt
{


class Tsdb;
class QueryTask;
class RollupDataFile;


class MmapFile
{
public:
    MmapFile(const std::string& file_name);
    virtual ~MmapFile();

    bool remap();
    bool resize(int64_t length);
    virtual void open(bool for_read) = 0;
    virtual void close();
    virtual void flush(bool sync);
    virtual void ensure_open(bool for_read);

    void dont_need(void *addr, size_t length);

    inline void *get_pages() { return m_pages; }
    inline size_t get_length() const { return m_length; }

    virtual bool is_open(bool for_read) const;
    inline bool is_read_only() const { return m_read_only; }
    inline bool exists() const { return file_exists(m_name); }
    void remove() { rm_file(m_name); }

protected:
    void open(int64_t length, bool read_only, bool append_only, bool resize);
    void open_existing(bool read_only, bool append_only);

    std::mutex m_lock;
    std::string m_name;

private:
    int64_t m_length;
    void *m_pages;
    int m_fd;
    bool m_read_only;
};


/* The first set of indices points to the 1st header of the time series;
 * the second set of indices points to the header of the first page, for
 * the time series, whose data falls into the second half of the Tsdb
 * time range.
 */
struct __attribute__ ((__packed__)) index_entry
{
    uint8_t flags;
    FileIndex file_index;       // points to the first header
    HeaderIndex header_index;   // points to the first header
    FileIndex file_index2;      // points to the second header
    HeaderIndex header_index2;  // points to the second header
};


class IndexFile : public MmapFile
{
public:
    IndexFile(const std::string& file_name);
    void open(bool for_read) override;
    bool close_if_idle(Timestamp threshold_sec, Timestamp now_sec);
    void ensure_open(bool for_read) override;

    bool set_indices(TimeSeriesId id, FileIndex file_index, HeaderIndex page_index);
    bool set_indices2(TimeSeriesId id, FileIndex file_index, HeaderIndex page_index);
    void get_indices(TimeSeriesId id, FileIndex& file_index, HeaderIndex& page_index);
    void get_indices2(TimeSeriesId id, FileIndex& file_index, HeaderIndex& page_index);

    bool get_out_of_order(TimeSeriesId id);
    void set_out_of_order(TimeSeriesId id, bool ooo);

    // for rollup data
    bool get_out_of_order2(TimeSeriesId id);
    void set_out_of_order2(TimeSeriesId id, bool ooo);

private:
    bool expand(int64_t new_len);

    std::atomic<Timestamp> m_last_access;
};


class HeaderFile : public MmapFile
{
public:
    HeaderFile(const std::string& file_name, FileIndex id, PageCount page_count, PageSize page_size);
    ~HeaderFile();

    void init_tsdb_header(PageSize page_size);
    void open(bool for_read) override;
    bool close_if_idle(Timestamp threshold_sec, Timestamp now_sec);

    PageSize get_page_size();
    PageCount get_page_index();
    inline PageCount get_page_count() const { return m_page_count; }
    HeaderIndex new_header_index(Tsdb *tsdb);
    struct tsdb_header *get_tsdb_header();
    struct page_info_on_disk *get_page_header(HeaderIndex header_idx);
    inline FileIndex get_id() const { return m_id; }
    bool is_full();
    void update_next(HeaderIndex prev_header_idx, FileIndex this_file_idx, HeaderIndex this_header_idx);

    static HeaderFile *restore(const std::string& file_name);

    // for testing only
    int count_pages(bool ooo);

private:
    HeaderFile(FileIndex id, const std::string& file_name);

    PageCount m_page_count;
    FileIndex m_id;
    Timestamp m_last_access;
};


class DataFile : public MmapFile
{
public:
    DataFile(const std::string& file_name, FileIndex id, PageSize size, PageCount count);
    ~DataFile();

    void open(bool read_only) override;
    void close() override;
    void close(int rw);
    bool close_if_idle(Timestamp threshold_sec, Timestamp now_sec);
    void flush(bool sync) override;

    PageCount append(const void *page, PageSize size);
    inline FileIndex get_id() const { return m_id; }
    inline PageSize get_offset() const { return m_offset; }
    inline PageSize get_next_page_size() const
    { return m_offset?(m_page_size-m_offset):m_page_size; }
    void *get_page(PageIndex page_idx);
    inline FILE *get_file() const { return m_file; }
    bool is_open(bool for_read) const override;

    inline pthread_rwlock_t *get_lock() { return &m_lock; }
    inline Timestamp get_last_read() const { return m_last_read; }
    inline Timestamp get_last_write() const { return m_last_write; }

private:
    FILE *m_file;
    PageSize m_page_size;
    PageSize m_offset;
    PageCount m_page_count;
    FileIndex m_id;
    PageCount m_page_index;
    HeaderFile *m_header_file;
    Timestamp m_last_read;
    Timestamp m_last_write;

    pthread_rwlock_t m_lock;
};


struct __attribute__ ((__packed__)) rollup_entry
{
    TimeSeriesId tid;
    uint32_t cnt;
    double min;
    double max;
    double sum;
};


// Used for shutdown/restart only
struct __attribute__ ((__packed__)) rollup_entry_ext
{
    TimeSeriesId tid;
    uint32_t cnt;
    double min;
    double max;
    double sum;
    Timestamp tstamp;   // this must be the last entry

    rollup_entry_ext() :
        tid(TT_INVALID_TIME_SERIES_ID),
        cnt(0),
        min(0.0),
        max(0.0),
        sum(0.0),
        tstamp(TT_INVALID_TIMESTAMP)
    {
    }

    rollup_entry_ext(struct rollup_entry *entry) :
        tid(entry->tid),
        cnt(entry->cnt),
        min(entry->min),
        max(entry->max),
        sum(entry->sum),
        tstamp(TT_INVALID_TIMESTAMP)
    {
    }
};


struct __attribute__ ((__packed__)) rollup_append_entry
{
    uint32_t cnt;
    double min;
    double max;
    double sum;
    Timestamp tstamp;
};


class RollupDataFileCursor
{
    RollupDataFileCursor() : m_index(0), m_size(0) {}

private:
    friend class RollupDataFile;

    int m_index;    // index to m_buff[]
    int m_size;     // number of bytes in m_buff[]
    uint8_t m_buff[4096];
    struct rollup_entry m_entry;
};


class RollupDataFile
{
public:
    RollupDataFile(int bucket, Timestamp tstamp);   // create 1d data file
    RollupDataFile(const std::string& name, Timestamp begin);
    RollupDataFile(MetricId mid, Timestamp begin, bool monthly);
    ~RollupDataFile();

    void open(bool for_read);
    void close();
    bool close_if_idle(Timestamp threshold_sec, Timestamp now_sec);
    bool is_open(bool for_read) const;
    inline int64_t size() const { return m_size; }
    inline Timestamp get_begin_timestamp() const { return m_begin; }

    inline bool empty() const { return (m_index == 0) && !file_exists(m_name); }
    inline void remove() { rm_file(m_name); }

    // iterate through the file, forward only
    // returns nullptr when no more entry left
    struct rollup_entry *first_entry(RollupDataFileCursor& cursor);
    struct rollup_entry *next_entry(RollupDataFileCursor& cursor);

    void add_data_point(TimeSeriesId tid, uint32_t cnt, double min, double max, double sum);
    void add_data_point(TimeSeriesId tid, Timestamp tstamp, uint32_t cnt, double min, double max, double sum);  // called during shutdown
    void add_data_points(std::unordered_map<TimeSeriesId,std::vector<struct rollup_entry_ext>>& data);
    void query(const TimeRange& range, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup);  // query hourly rollup
    void query2(const TimeRange& range, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup); // query daily rollup
    // used by Tsdb::rollup() for offline processing
    void query(const TimeRange& range, std::unordered_map<TimeSeriesId,struct rollup_entry_ext>& map);
    void query_ext(const TimeRange& range, std::unordered_map<TimeSeriesId,struct rollup_entry_ext>& map);
    // used by Tsdb::rollup()
    void query(std::unordered_map<TimeSeriesId,std::vector<struct rollup_entry_ext>>& data);

    void dec_ref_count();
    void inc_ref_count();

    static std::string get_name_by_mid_1h(MetricId mid, int year, int month);
    static std::string get_name_by_bucket_1h(int bucket, int year, int month);
    static std::string get_name_by_mid_1d(MetricId mid, int year);
    static std::string get_name_by_bucket_1d(int bucket, int year);

private:
    void flush();
    int query_entry(const TimeRange& range, struct rollup_entry *entry, std::unordered_map<TimeSeriesId,QueryTask*>& map, RollupType rollup);

    std::string m_name;
    FILE *m_file;
    Timestamp m_begin;
    int m_index;    // index of m_buff[]
    char m_buff[4096];
    Timestamp m_last_access;
    int64_t m_size;
    std::mutex m_lock;
    int m_ref_count;        // prevent unload when in use
    short m_compressor_version;
    double m_compressor_precision;
    bool m_monthly;         // true: monthly; false: annually
    bool m_for_read;
};


class data_less
{
public:
    bool operator()(DataFile* data1, DataFile* data2) const
    {
        // Make sure "! (a < a)"
        if (data1->get_id() == data2->get_id())
            return false;
        else
            return data1->get_id() < data2->get_id();
    }
};


class header_less
{
public:
    bool operator()(HeaderFile* header1, HeaderFile* header2) const
    {
        // Make sure "! (a < a)"
        if (header1->get_id() == header2->get_id())
            return false;
        else
            return header1->get_id() < header2->get_id();
    }
};


}
