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
#include "lock.h"
#include "range.h"
#include "type.h"
#include "utils.h"


namespace tt
{


class Tsdb;


class MmapFile
{
public:
    MmapFile(const std::string& file_name);
    virtual ~MmapFile();

    bool remap();
    bool resize(off_t length);
    virtual void open(bool for_read) = 0;
    virtual void close();
    virtual void flush(bool sync);
    void ensure_open(bool for_read);

    void dont_need(void *addr, size_t length);

    inline void *get_pages() { return m_pages; }
    inline size_t get_length() const { return m_length; }

    virtual bool is_open(bool for_read) const;
    inline bool is_read_only() const { return m_read_only; }
    inline bool exists() const { return file_exists(m_name); }

protected:
    void open(off_t length, bool read_only, bool append_only, bool resize);
    void open_existing(bool read_only, bool append_only);

    std::string m_name;

private:
    off_t m_length;
    void *m_pages;
    std::mutex m_lock;
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
    RollupIndex rollup_index;   // index of rollup-index
};


class IndexFile : public MmapFile
{
public:
    IndexFile(const std::string& file_name);
    void open(bool for_read) override;

    bool set_indices(TimeSeriesId id, FileIndex file_index, HeaderIndex page_index);
    bool set_indices2(TimeSeriesId id, FileIndex file_index, HeaderIndex page_index);
    void get_indices(TimeSeriesId id, FileIndex& file_index, HeaderIndex& page_index);
    void get_indices2(TimeSeriesId id, FileIndex& file_index, HeaderIndex& page_index);
    void set_rollup_index(TimeSeriesId tid, RollupIndex idx);
    RollupIndex get_rollup_index(TimeSeriesId tid);

    bool get_out_of_order(TimeSeriesId id);
    void set_out_of_order(TimeSeriesId id, bool ooo);

private:
    bool expand(off_t new_len);
};


class HeaderFile : public MmapFile
{
public:
    HeaderFile(const std::string& file_name, FileIndex id, PageCount page_count, PageSize page_size);
    ~HeaderFile();

    void init_tsdb_header(PageSize page_size);
    void open(bool for_read) override;

    PageSize get_page_size();
    PageCount get_page_index();
    inline PageCount get_page_count() const { return m_page_count; }
    HeaderIndex new_header_index(Tsdb *tsdb);
    struct tsdb_header *get_tsdb_header();
    struct page_info_on_disk *get_page_header(HeaderIndex header_idx);
    int get_compressor_version();
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
};


class DataFile : public MmapFile
{
public:
    DataFile(const std::string& file_name, FileIndex id, PageSize size, PageCount count);
    ~DataFile();

    void open(bool read_only) override;
    void close() override;
    void close(int rw);
    void flush(bool sync) override;
    //void init(HeaderFile *header_file);

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


class RollupHeaderFile : public MmapFile
{
public:
    RollupHeaderFile(const std::string& file_name);

    void open(bool for_read) override;

    RollupIndex new_header(int entries);
    void add_index(RollupIndex header_idx, uint32_t data_idx, int entries);

private:
    bool expand(off_t new_len);
    uint32_t *get_header(RollupIndex header_idx, int entries);

    RollupIndex m_header_count;
};


struct __attribute__ ((__packed__)) rollup_entry
{
    uint32_t count;
    double min;
    double max;
    double sum;
};


class RollupDataFile : public MmapFile
{
public:
    RollupDataFile(const std::string& file_name);

    void open(bool read_only) override;
    void close() override;
    bool is_open(bool for_read) const override;

    uint32_t add_data_point(uint32_t cnt, double min, double max, double sum);

    inline Timestamp get_last_read() const { return m_last_read; }
    inline Timestamp get_last_write() const { return m_last_write; }

private:
    FILE *m_file;
    Timestamp m_last_read;
    Timestamp m_last_write;
    uint32_t m_entry_index;
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
