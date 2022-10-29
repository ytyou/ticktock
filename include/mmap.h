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
#include "range.h"
#include "type.h"


namespace tt
{


class Tsdb;


class MmapFile
{
public:
    MmapFile(const std::string& file_name);
    virtual ~MmapFile();

    bool open(off_t length, bool read_only, bool append_only);  // return true if new file
    bool fopen(const char *mode, std::FILE * (&file));  // return true if new file
    bool resize(off_t length);
    virtual bool open(bool for_read) = 0;
    virtual void close();
    virtual void flush(bool sync);
    void ensure_open(bool for_read);

    void dont_need(void *addr, size_t length);

    inline void *get_pages() { return m_pages; }
    inline size_t get_length() { return m_length; }

    inline virtual bool is_open(bool for_read) const;
    inline bool is_read_only() const { return m_read_only; }

private:
    std::string m_name;
    off_t m_length;
    void *m_pages;
    std::mutex m_lock;
    int m_fd;
    bool m_read_only;
};


struct __attribute__ ((__packed__)) index_entry
{
    FileIndex file_index;
    HeaderIndex header_index;
};


class IndexFile : public MmapFile
{
public:
    IndexFile(const std::string& file_name);
    bool open(bool for_read) override;

    bool set_indices(TimeSeriesId id, FileIndex file_index, HeaderIndex page_index);
    void get_indices(TimeSeriesId id, FileIndex& file_index, HeaderIndex& page_index);

private:
    bool expand(off_t new_len);
};


class HeaderFile : public MmapFile
{
public:
    HeaderFile(const std::string& file_name, FileIndex id, PageCount page_count, Tsdb *tsdb);

    void init_tsdb_header(Tsdb *tsdb);
    bool open(bool for_read) override;

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

private:
    HeaderFile(const std::string& file_name);

    PageCount m_page_count;
    FileIndex m_id;
};


class DataFile : public MmapFile
{
public:
    DataFile(const std::string& file_name, FileIndex id, PageSize size, PageCount count);
    DataFile(const std::string& file_name);

    bool open(bool read_only) override;
    void close() override;
    void flush(bool sync) override;
    void init(HeaderFile *header_file);

    PageCount append(const void *page);
    inline FileIndex get_id() const { return m_id; }
    void *get_page(PageIndex page_idx);
    inline FILE *get_file() const { return m_file; }
    bool is_open(bool for_read) const override;

private:
    FILE *m_file;
    PageSize m_page_size;
    PageCount m_page_count;
    FileIndex m_id;
    PageCount m_page_index;
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
