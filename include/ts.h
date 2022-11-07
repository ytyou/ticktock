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


class TimeSeries : public TagOwner
{
public:
    TimeSeries(const char *metric, const char *key, Tag *tags);
    TimeSeries(TimeSeriesId id, const char *metric, const char *key, Tag *tags);
    ~TimeSeries();

    void init(TimeSeriesId id, const char *metric, const char *key, Tag *tags);
    void restore(Tsdb *tsdb, PageSize offset, uint8_t start, char *buff, bool is_ooo);

    inline TimeSeriesId get_id() const { return m_id; }
    static inline TimeSeriesId get_next_id() { return m_next_id.load(std::memory_order_relaxed); }

    void flush(bool accessed = false);
    void flush_no_lock(bool accessed = false);
    bool compact(MetaFile& meta_file);
    void set_check_point();

    bool add_data_point(DataPoint& dp);
    bool add_ooo_data_point(DataPoint& dp);

    void append(FILE *file);

    inline const char* get_key() const { return m_key; }
    inline void set_key(const char *key)
    {
        ASSERT(key != nullptr);
        m_key = STRDUP(key);    // TODO: better memory management than strdup()???
    }

    inline const char *get_metric() const { return m_metric; }
    inline void set_metric(const char *metric)
    {
        ASSERT(metric != nullptr);
        m_metric = STRDUP(metric);
    }

    Tag *find_tag_by_name(const char *name) const;

    void query(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_with_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps);
    void query_without_ooo(TimeRange& range, Downsampler *downsampler, DataPointVector& dps, PageInfo *page_info);

private:
    char *m_key;            // this uniquely defines the time-series
    std::mutex m_lock;

    PageInMemory *m_buff;   // in-memory buffer; if m_id is 0, it's contents are
                            // not on disk; otherwise it's contents are at least
                            // partially on the page indexed by m_id

    PageInMemory *m_ooo_buff;

    char *m_metric;
    //Tsdb *m_tsdb;           // current tsdb we are writing into

    static std::atomic<TimeSeriesId> m_next_id;
    TimeSeriesId m_id;      // global, unique, permanent id starting at 0
};


class DataPointContainer : public Recyclable
{
public:
    void init(PageInfo *info)
    {
        m_dps.clear();
        m_dps.reserve(700);
        m_out_of_order = info->is_out_of_order();
        m_page_index = info->get_global_page_index();
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
