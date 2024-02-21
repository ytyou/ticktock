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
#include "rollup.h"
#include "serial.h"
#include "tag.h"
#include "meta.h"
#include "leak.h"


namespace tt
{


class DataPointContainer;
class Downsampler;
class Tsdb;


class __attribute__ ((__packed__)) TimeSeries : public BaseType
{
public:
    TimeSeries(TagBuilder& builder);
    TimeSeries(const char *metric, TagBuilder& builder);
    TimeSeries(const char *metric, const char *key, Tag *tags);
    TimeSeries(TimeSeriesId id, const char *metric, const char *key, Tag *tags);
    ~TimeSeries();

    static void init();     // called by Tsdb::init()
    static void cleanup();  // called by Tsdb::shutdown()
    void init(TimeSeriesId id, const char *metric, const char *key, Tag *tags);
    void restore(Tsdb *tsdb, MetricId mid, Timestamp tstamp, PageSize offset, uint8_t start, uint8_t *buff, int size, bool is_ooo, FileIndex file_idx, HeaderIndex header_idx);
    void restore_rollup_mgr(const struct rollup_entry_ext& mgr);

    inline TimeSeriesId get_id() const { return m_id; }
    static inline TimeSeriesId get_next_id() { return m_next_id.load(std::memory_order_relaxed); }

    void close(MetricId mid);   // called during TT shutdown
    void flush(MetricId mid);
    void flush_no_lock(MetricId mid, bool close = false);
    //bool compact(MetaFile& meta_file);
    void set_check_point();
    void archive(MetricId mid, Timestamp now_sec, Timestamp threshold_sec);

    bool add_data_point(MetricId mid, DataPoint& dp);
    bool add_ooo_data_point(MetricId mid, DataPoint& dp);

    void append(MetricId mid, FILE *file);

    inline Tag *get_tags() const { return m_tags.get_v1_tags(); }
    inline Tag *get_cloned_tags(StringBuffer& strbuf) const
    { return m_tags.get_cloned_v1_tags(strbuf); }
    inline TagCount get_tag_count() const { return m_tags.get_count(); }

    inline Tag_v2& get_v2_tags() { return m_tags; }

    void get_keys(std::set<std::string>& keys) const { m_tags.get_keys(keys); }
    void get_values(std::set<std::string>& values) const { m_tags.get_values(values); }

    bool query_for_data(Tsdb *tsdb, TimeRange& range, std::vector<DataPointContainer*>& data);
    void query_for_rollup(const TimeRange& range, QueryTask *qt, RollupType rollup, bool ms);

    inline bool is_type(int type) const override
    { return TT_TYPE_TIME_SERIES == type; }

    TimeSeries *m_next;

private:
    //char *m_key;            // this uniquely defines the time-series
    //std::mutex m_lock;
    RollupManager m_rollup;

    PageInMemory *m_buff;   // in-memory buffer; if m_id is 0, it's contents are
                            // not on disk; otherwise it's contents are at least
                            // partially on the page indexed by m_id

    PageInMemory *m_ooo_buff;

    //char *m_metric;
    //Tsdb *m_tsdb;           // current tsdb we are writing into

    Tag_v2 m_tags;

    TimeSeriesId m_id;      // global, unique, permanent id starting at 0

    static uint32_t m_lock_count;
    static std::mutex *m_locks;

    static std::atomic<TimeSeriesId> m_next_id;
};


/*
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
*/


}
