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

#include <string.h>
#include <utility>
#include <vector>
#include "dp.h"
#include "page.h"
#include "recycle.h"
#include "serial.h"
#include "tag.h"
#include "meta.h"
#include "leak.h"


namespace tt
{


class Downsampler;
class Tsdb;


class TimeSeries : public Serializable, public TagOwner, public Recyclable
{
public:
    ~TimeSeries();

    void init(const char *metric, const char *key, Tag *tags, Tsdb *tsdb, bool read_only);
    void flush(bool close);
    bool recycle();
    bool compact(MetaFile& meta_file);
    void append_meta_all(MetaFile &meta);
    void set_check_point();

    void add_page_info(PageInfo *page_info);
    bool add_data_point(DataPoint& dp);
    bool add_ooo_data_point(DataPoint& dp);
    bool add_batch(DataPointSet& dps);

    // collect all pages to be compacted
    void get_all_pages(std::vector<PageInfo*>& pages);

    inline const char* get_key() const
    {
        return m_key;
    }

    inline void set_key(const char *key)
    {
        ASSERT(key != nullptr);
        m_key = STRDUP(key);    // TODO: better memory management than strdup()???
    }

    inline void set_metric(const char *metric)
    {
        ASSERT(metric != nullptr);
        m_metric = STRDUP(metric);
    }

    inline void set_tsdb(Tsdb *tsdb)
    {
        ASSERT(tsdb != nullptr);
        m_tsdb = tsdb;
    }

    inline const char *get_metric() const
    {
        return m_metric;
    }

    Tag *find_tag_by_name(const char *name) const;

    void query(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_with_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps, PageInfo *page_info);

    int get_dp_count();
    int get_page_count(bool ooo);   // for testing only

    inline size_t c_size() const override { return 1024; }
    const char* c_str(char* buff) const override;

private:
    friend class MemoryManager;
    friend class SanityChecker;

    TimeSeries();
    PageInfo *get_free_page_on_disk(bool is_out_of_order);

    // used during compaction
    void add_data_point_with_dedup(DataPointPair &dp, DataPointPair &prev, PageInfo* &info);

    char *m_key;        // this defines the time-series
    std::mutex m_lock;

    // this will be null unless Tsdb is in read-write mode
    PageInfo *m_buff;   // in-memory buffer; if m_id is 0, it's contents are
                        // not on disk; otherwise it's contents are at least
                        // partially on the page indexed by m_id
    std::vector<PageInfo*> m_pages; // set of on disk pages

    PageInfo *m_ooo_buff;
    std::vector<PageInfo*> m_ooo_pages; // out-of-order pages

    char *m_metric;
    Tsdb *m_tsdb;
};


class DataPointContainer : public Recyclable
{
public:
    void init(PageInfo *info)
    {
        info->ensure_dp_available();
        m_out_of_order = info->is_out_of_order();
        m_page_index = info->get_page_order();
        m_dps.reserve(700);
        info->get_all_data_points(m_dps);
    }

    inline size_t size() const { return m_dps.size(); }
    inline DataPointPair& get_data_point(int i) { return m_dps[i]; }
    inline PageCount get_page_index() const { return m_page_index; }
    inline bool is_out_of_order() const { return m_out_of_order; }

private:
    bool m_out_of_order;
    PageCount m_page_index;
    DataPointVector m_dps;
};


}
