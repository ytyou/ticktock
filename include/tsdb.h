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
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>
#include "dp.h"
#include "meta.h"
#include "ts.h"
#include "http.h"
#include "page.h"
#include "part.h"
#include "range.h"
#include "recycle.h"
#include "rw.h"
#include "serial.h"
#include "sync.h"
#include "type.h"
#include "utils.h"
#include "tsl/robin_map.h"


namespace tt
{


// In read-write mode, the last page of each time series resides in
// memory until it's full, and then flushed to disk via mmapp'ed file;
// a compressor is attached to each of these in-memory pages;
//
// In read-only mode, all pages are loaded into memory on demand, via
// mmapp'ed file; Any write will go to out-of-order buffer; no compressor
// is attached to any page-info; at query time, a compressor is created
// and used to uncompress the data points;
//
// In archive mode, nothing is loaded in memory, not even the mappings
// from metric and tag names to time series; any write will be dropped;
// queries will take longer because we need to load everything from disk
// on the fly, starting from reading meta file and build the mappings
// from metric and tag names to time series;
//
// If TSDB_MODE_COMPACTED is set, it means the data file was compacted.

#define TSDB_MODE_NONE          0x00000000
#define TSDB_MODE_READ          0x00000001
#define TSDB_MODE_WRITE         0x00000002
#define TSDB_MODE_COMPACTED     0x00000004
#define TSDB_MODE_CHECKPOINT    0x00000008

#define TSDB_MODE_READ_WRITE    (TSDB_MODE_READ | TSDB_MODE_WRITE)
#define TSDB_MODE_WRITE_CHECKPOINT (TSDB_MODE_CHECKPOINT | TSDB_MODE_WRITE)


class Tsdb;

// one per metric
class Mapping : public Recyclable
{
    friend class Tsdb;
    friend class MemoryManager;
    friend class SanityChecker;

private:
    Mapping();
    ~Mapping();

    void init(const char *name, Tsdb* tsdb);
    void unload();
    void unload_no_lock();
    void flush(bool accessed);
    bool recycle() override;
    void set_check_point();

    char *m_metric;
    bool add(DataPoint& dp);
    bool add_data_point(DataPoint& dp, bool forward);
    bool add_batch(DataPointSet& dps);
    TimeSeries *get_ts(TagOwner& to);
    TimeSeries *get_ts2(DataPoint& dp);
    void query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv);
    void add_ts(Tsdb *tsdb, std::string& metric, std::string& key, PageInfo *page_info);

    inline Tsdb*& get_tsdb() { return (Tsdb*&)Recyclable::next(); }
    inline Tsdb *get_tsdb_const() const { return (Tsdb*)Recyclable::next_const(); }

    int get_dp_count();
    int get_ts_count();
    int get_page_count(bool ooo);   // for testing only

    //std::mutex m_lock;
    default_contention_free_shared_mutex m_lock;
    tsl::robin_map<const char*,TimeSeries*,hash_func,eq_func> m_map;
    //std::unordered_map<const char*,TimeSeries*,hash_func,eq_func> m_map;
    //std::map<const char*,TimeSeries*,cstr_less> m_map;

    //Tsdb *m_tsdb;
    Partition *m_partition;

    std::atomic<int> m_ref_count;
    inline void inc_ref_count() { m_ref_count++; }
    inline void dec_ref_count()
    {
        ASSERT(m_ref_count > 0);
        if (--m_ref_count == 0) MemoryManager::free_recyclable(this);
    }
};


/* Each instance of Tsdb represents all data points in a specific time range.
 */
class Tsdb : public Serializable, public Counter
{
public:
    // this must be called before anythng else
    // this is not thread-safe
    static void init();

    static Tsdb* inst(Timestamp tstamp, bool create = true);
    static void insts(const TimeRange& range, std::vector<Tsdb*>& tsdbs);
    static void shutdown();
    static std::string get_file_name(const TimeRange& range, std::string ext, bool temp = false);
    static Tsdb* search(Timestamp tstamp);
    static void purge_oldest(int threshold);
    static bool compact(TaskData& data);
    static void compact2(); // last compaction step

    bool add(DataPoint& dp);
    bool add_batch(DataPointSet& dps);

    bool add_data_point(DataPoint& dp, bool forward);
    inline bool submit_data_points()
    {
        ASSERT(m_partition_mgr != nullptr);
        return m_partition_mgr->submit_data_points();
    }

    inline Partition *get_partition(const char *metric) const
    {
        return (m_partition_mgr == nullptr) ? nullptr : m_partition_mgr->get_partition(metric);
    }

    void query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts);
    void ensure_readable(bool count = false);   // 'count' keep tsdb loaded until it's decremented

    void flush(bool sync);
    void set_check_point();

    inline void append_meta(TimeSeries *ts, PageInfo *pi)
    {
        m_meta_file.append(ts, pi);
    }

    void append_meta_all();     // write all meta info to an empty file
    void add_ts(std::string& metric, std::string& key, PageCount file_id, PageCount page_index);

    std::string get_partition_defs() const;

    PageInfo *get_free_page(bool out_of_order);
    PageInfo *get_free_page_on_disk(bool out_of_order);
    PageInfo *get_free_page_for_compaction();   // used during compaction
    // Caller should acquire m_pm_lock before calling this method
    PageInfo *get_the_page_on_disk(PageCount id, PageCount header_index);
    //bool is_mmapped(PageInfo *page_info) const;

    // -1 is an invalid id, which means we should generate the next
    // valid id for the new page manager.
    PageManager *create_page_manager(int id = -1);

    inline PageManager *get_page_manager(PageCount id)
    {
        ASSERT(id >= 0);
        return (id < m_page_mgrs.size()) ? m_page_mgrs[id] : nullptr;
    }

    inline const TimeRange& get_time_range() const
    {
        return m_time_range;
    }

    inline int get_compressor_version()
    {
        ASSERT(! m_page_mgrs.empty());
        return m_page_mgrs.back()->get_compressor_version();
    }

    inline size_t size() const
    {
        return m_map.size();
    }

    inline bool in_range(Timestamp tstamp) const
    {
        return m_time_range.in_range(tstamp);
    }

    inline bool in_range(const TimeRange& range) const
    {
        return m_time_range.has_intersection(range);
    }

    inline bool is_read_only() const
    {
        return ((m_mode & TSDB_MODE_WRITE) == 0);
    }

    inline bool is_archived() const
    {
        return ((m_mode & TSDB_MODE_READ_WRITE) == 0);
    }

    inline bool is_compacted() const
    {
        return ((m_mode & TSDB_MODE_COMPACTED));
    }

    // http add data-point request handler
    static bool http_api_put_handler(HttpRequest& request, HttpResponse& response);
    static bool http_api_put_handler_json(HttpRequest& request, HttpResponse& response);
    static bool http_api_put_handler_plain(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_suggest_handler(HttpRequest& request, HttpResponse& response);

    static int get_metrics_count();
    static int get_dp_count();
    static int get_ts_count();
    static int get_page_count(bool ooo);    // for testing only
    static int get_data_page_count();       // for testing only
    static bool validate(Tsdb *tsdb);
    double get_page_percent_used();
    inline size_t c_size() const override { return m_time_range.c_size() + 24; }
    const char *c_str(char *buff) const override;

private:
    friend class tsdb_less;
    friend class SanityChecker;

    //Tsdb(Timestamp start, Timestamp end);
    Tsdb(TimeRange& range, bool existing);
    virtual ~Tsdb();
    //void open_meta();
    bool load_from_disk();          // return false if load failed
    bool load_from_disk_no_lock();  // return false if load failed
    void unload();
    void unload_no_lock();
    uint32_t mode_of() const;

    Mapping *get_or_add_mapping(TagOwner& dp);
    Mapping *get_or_add_mapping2(DataPoint& dp);

    static bool rotate(TaskData& data);

    static void get_range(Timestamp tstamp, TimeRange& range);

    static Tsdb *create(TimeRange& range, bool existing);   // caller needs to acquire m_tsdb_lock!

    //static std::mutex m_tsdb_lock;
    static default_contention_free_shared_mutex m_tsdb_lock;
    static std::vector<Tsdb*> m_tsdbs;  // ordered by m_start_tstamp

    // This time range will use the time unit specified in the config.
    TimeRange m_time_range;

    std::mutex m_pm_lock;
    // There needs to be at least 1 PageManager created for every new Tsdb.
    // The compressor version should be the same for ALL PageManagers in a
    // single Tsdb.
    std::vector<PageManager*> m_page_mgrs;
    std::vector<PageManager*> m_temp_page_mgrs; // used during compaction

    // in archive-mode, this map will be populated from meta-file on-demand
    std::mutex m_lock;
    //std::map<const char*,Mapping*,cstr_less> m_map;
    //std::unordered_map<const char*,Mapping*,hash_func,eq_func> m_map;
    tsl::robin_map<const char*,Mapping*,hash_func,eq_func> m_map;

    //std::FILE *m_meta_file;
    MetaFile m_meta_file;

    // this is true if, 1. m_map is populated; 2. m_page_mgr is open; 3. m_meta_file is open;
    // this is false if all the above are not true;
    uint32_t m_mode;
    std::atomic<Timestamp> m_load_time; // epoch time in sec
    default_contention_free_shared_mutex m_load_lock;

    PartitionManager *m_partition_mgr;
};


class tsdb_less
{
public:
    bool operator()(Tsdb* tsdb1, Tsdb* tsdb2) const
    {
        // Make sure "! (a < a)"
        if (tsdb1->m_time_range.equals(tsdb2->m_time_range))
            return false;
        else
            return tsdb1->m_time_range.get_from() < tsdb2->m_time_range.get_to();
    }
};


}
