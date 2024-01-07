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
#include "lock.h"
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
// If TSDB_MODE_ROLLED_UP is set, it means the level2 rollup data is ready.
// If TSDB_MODE_CRASHED is set, it means the last shutdown was abnormal.
// If TSDB_MODE_OUT_OF_ORDER is set, it means its rollup data (level1 & level2) is not ready.

#define TSDB_MODE_NONE          0x00000000
#define TSDB_MODE_READ          0x00000001
#define TSDB_MODE_WRITE         0x00000002
#define TSDB_MODE_COMPACTED     0x00000004
#define TSDB_MODE_ROLLED_UP     0x00000008
#define TSDB_MODE_OUT_OF_ORDER  0x00000010
#define TSDB_MODE_CRASHED       0x80000000

#define TSDB_MODE_READ_WRITE    (TSDB_MODE_READ | TSDB_MODE_WRITE)


class Tsdb;
class Mapping;
class DataPointContainer;
class QuerySuperTask;


class Measurement : public BaseType
{
public:
    Measurement();
    Measurement(uint32_t ts_count);
    ~Measurement();

    void add_ts(int idx, TimeSeries *ts);
    TimeSeries *get_ts(bool add, Mapping *mapping);
    TimeSeries *add_ts(const char *field, Mapping *mapping);
    void append_ts(TimeSeries *ts);
    bool add_data_points(std::vector<DataPoint>& dps, Timestamp ts, Mapping *mapping);
    TimeSeries *get_ts(int idx, const char *field);
    bool get_ts(std::vector<DataPoint>& dps, std::vector<TimeSeries*>& tsv);
    void get_all_ts(std::vector<TimeSeries*>& tsv);
    inline uint32_t get_ts_count() const { return m_ts_count; }
    inline void add_ts_count(uint32_t ts_count);
    inline void set_ts_count(uint32_t ts_count);
    inline bool is_initialized() const { return m_time_series != nullptr; }

    inline bool is_type(int type) const override
    { return TT_TYPE_MEASUREMENT == type; }

    std::mutex m_lock;
    //pthread_rwlock_t m_lock;
    //default_contention_free_shared_mutex m_lock;

private:
    TimeSeries *get_ts_no_lock(bool add, Mapping *mapping);
    TimeSeries *get_ts_no_lock(int idx, const char *field, bool swap);

    TimeSeries **m_time_series;
    uint32_t m_ts_count;
};


// one per metric
class Mapping
{
public:
    char *get_metric() { return m_metric; }
    void get_all_ts(std::vector<TimeSeries*>& tsv);
    void add_ts(TimeSeries *ts);    // add 'ts' to the list headed by 'm_ts_head'
    MetricId get_id() const { return m_id; }

    static MetricId get_metric_count() { return m_next_id.load(std::memory_order_relaxed); }

    friend class Tsdb;

private:
    Mapping(const char *name);              // new
    Mapping(MetricId id, const char *name); // restore
    ~Mapping();

    void flush();
    void close();   // called during TT shutdown

    char *m_metric;
    bool add(DataPoint& dp);
    bool add_data_point(DataPoint& dp, bool forward);
    bool add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint>& dps);
    TimeSeries *get_ts(DataPoint& dp);
    TimeSeries *get_ts_in_measurement(DataPoint& dp, Tag *field);
    Measurement *get_measurement(char *raw_tags, TagOwner& owner, const char *measurement, std::vector<DataPoint>& dps);
    void init_measurement(Measurement *mm, const char *measurement, char *tags, TagOwner& owner, std::vector<DataPoint>& dps);
    void query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv, const char *key, bool explicit_tags);
    TimeSeries *restore_ts(std::string& metric, std::string& key, TimeSeriesId id);
    void restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv);
    void set_tag_count(int tag_count);
    TimeSeries *get_ts_head();

    int get_dp_count();
    int get_ts_count();

    //std::mutex m_lock;
    pthread_rwlock_t m_lock;
    //default_contention_free_shared_mutex m_lock;

    // Keys of the m_map are of the following format:
    //  <tag1>=<val1>,<tag2>=<val2>,...,<tagN>=<valN>
    // One special key, consisting of just a semicolon (";"),
    // reprensets the case where there's no tag at all.
    tsl::robin_map<const char*,BaseType*,hash_func,eq_func> m_map;
    //std::unordered_map<const char*,BaseType*,hash_func,eq_func> m_map;

    //default_contention_free_shared_mutex m_lock2;
    //tsl::robin_map<const char*,Measurement*,hash_func,eq_func> m_map2;

    std::atomic<TimeSeries*> m_ts_head;
    int16_t m_tag_count;    // -1: uninitialized; -2: inconsistent;

    MetricId m_id;
    static std::atomic<MetricId> m_next_id;
    //Partition *m_partition;
};


class Metric
{
public:
    Metric(const std::string& dir, PageSize page_size, PageCount page_cnt);
    ~Metric();

    void close();
    void flush(bool sync);
    bool rotate(Timestamp now_sec, Timestamp thrashing_threshold);
    //bool rollup(IndexFile *idx_file, int no_entries);

    MetricId get_id() const { return m_id; }
    std::string get_metric_dir(std::string& tsdb_dir);
    static std::string get_metric_dir(std::string& tsdb_dir, MetricId id);
    std::string get_data_file_name(std::string& tsdb_dir, FileIndex idx);
    std::string get_header_file_name(std::string& tsdb_dir, FileIndex idx);

    DataFile *get_last_data() { return m_data_files.back(); };  // call get_last_header() first
    HeaderFile *get_last_header(std::string& tsdb_dir, PageCount page_cnt, PageSize page_size);

    DataFile *get_data_file(FileIndex file_idx);
    HeaderFile *get_header_file(FileIndex file_idx);

    //RollupDataFile *get_rollup_data_file() { return &m_rollup_data_file; }
    //RollupHeaderFile *get_rollup_header_file() { return &m_rollup_header_file; }

    //void add_rollup_point(TimeSeriesId tid, uint32_t cnt, double min, double max, double sum);
    void get_rollup_point(RollupIndex header_idx, int entry_idx, int entries, uint32_t& cnt, double& min, double& max, double& sum);

    // for testing only
    int get_page_count(bool ooo);
    int get_data_page_count();
    int get_open_data_file_count(bool for_read);
    int get_open_header_file_count(bool for_read);

    std::mutex m_rollup_lock;

private:
    void restore_header(const std::string& file);
    void restore_data(const std::string& file, PageSize page_size, PageCount page_cnt);

    MetricId m_id;
    //RollupHeaderFile m_rollup_header_file;
    //RollupHeaderTmpFile m_rollup_header_tmp_file;
    //RollupDataFile m_rollup_data_file;
    std::vector<HeaderFile*> m_header_files;
    std::vector<DataFile*> m_data_files;
};


/* Each instance of Tsdb represents all data points in a specific time range.
 */
class Tsdb : public Serializable
{
public:
    // this must be called before anythng else
    // this is not thread-safe
    static void init();

    static Tsdb* inst(Timestamp tstamp, bool create = true);
    static void insts(const TimeRange& range, std::vector<Tsdb*>& tsdbs);
    static void shutdown();
    static Tsdb* search(Timestamp tstamp);
    static void purge_oldest(int threshold);
    static bool compact(TaskData& data);
    static void compact2(); // last compaction step
    static bool rollup(TaskData& data);
    static void write_to_compacted(MetricId mid, QuerySuperTask& super_task, Tsdb *compacted, PageSize& next_size);
    static bool add_data_point(DataPoint& dp, bool forward);
    static void restore_metrics(MetricId id, std::string& metric);
    static TimeSeries *restore_ts(std::string& metric, std::string& key, TimeSeriesId id);
    static void restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv);
    static void restore_rollup_mgr(std::unordered_map<TimeSeriesId,RollupManager>& map);
    static void get_all_ts(std::vector<TimeSeries*>& tsv);
    static void get_all_mappings(std::vector<Mapping*>& mappings);

    bool add(DataPoint& dp);
    //void add_rollup_point(MetricId mid, TimeSeriesId tid, uint32_t cnt, double min, double max, double sum);

    static MetricId query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts, const char *key, bool explicit_tags);
    //bool query_for_data(TimeSeriesId id, TimeRange& range, std::vector<DataPointContainer*>& data);
    //bool query_for_data_no_lock(TimeSeriesId id, TimeRange& range, std::vector<DataPointContainer*>& data);

    void query_for_data_no_lock(MetricId mid, QueryTask *task);
    void query_for_data(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, bool compact = false);
    void query_for_data_no_lock(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, bool compact = false);

    void flush(bool sync);
    void flush_for_test();  // for testing only
    void dec_ref_count();
    void dec_ref_count_no_lock();
    void inc_ref_count();

    inline PageSize get_page_size() const { return m_page_size; }
    PageCount get_page_count() const;
    int get_compressor_version();

    bool can_use_rollup(bool level2);
    bool can_use_rollup(TimeSeriesId tid);
    Timestamp get_last_tstamp(MetricId mid, TimeSeriesId tid);
    bool get_out_of_order(TimeSeriesId tid);
    void set_out_of_order(TimeSeriesId tid, bool ooo);
    bool get_out_of_order2(TimeSeriesId tid);
    void set_out_of_order2(TimeSeriesId tid, bool ooo);
    void get_last_header_indices(MetricId mid, TimeSeriesId tid, FileIndex& file_idx, HeaderIndex& header_idx);
    void set_indices(MetricId mid, TimeSeriesId tid, FileIndex prev_file_idx, HeaderIndex prev_header_idx,
                     FileIndex this_file_idx, HeaderIndex this_header_idx, bool crossed);
    PageSize append_page(MetricId mid,          // return next page size
                         TimeSeriesId tid,
                         FileIndex prev_file_idx,
                         HeaderIndex prev_header_idx,
                         struct page_info_on_disk *header,
                         uint32_t tstamp_from,
                         void *page,
                         bool compact);
    DataFile *get_data_file(MetricId mid, FileIndex file_idx);
    HeaderFile *get_header_file(MetricId mid, FileIndex file_idx);

    inline int get_rollup_entries() const
    {
        return std::ceil((double)m_time_range.get_duration_sec() / (double)g_rollup_interval);
    }

    inline const TimeRange& get_time_range() const
    {
        return m_time_range;
    }

    inline int in_range(Timestamp tstamp) const
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

    inline bool is_rolled_up() const
    {
        return ((m_mode & TSDB_MODE_ROLLED_UP));
    }

    inline bool is_crashed() const
    {
        return ((m_mode & TSDB_MODE_CRASHED));
    }

    inline void set_crashed()
    {
        m_mode |= TSDB_MODE_CRASHED;
    }

    static void set_crashes(Tsdb *oldest_tsdb);

    // http add data-point request handler
    static bool http_api_put_handler(HttpRequest& request, HttpResponse& response); // json or plain
    static bool http_api_put_handler_json(HttpRequest& request, HttpResponse& response);
    static bool http_api_put_handler_plain(HttpRequest& request, HttpResponse& response);
    static bool http_api_write_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_suggest_handler(HttpRequest& request, HttpResponse& response);

    // parse 1-line of the InfluxDB line protocol
    static bool parse_line(char* &line, const char* &measurement, char* &tags, Timestamp& ts, std::vector<DataPoint>& dps);
    static bool add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint>& dps);

    static int get_metrics_count();
    static int get_dp_count();
    static int get_ts_count();
    static int get_page_count(bool ooo);    // for testing only
    static int get_data_page_count();       // for testing only
    static int get_active_tsdb_count();
    static int get_total_tsdb_count();
    static int get_open_data_file_count(bool for_read);
    static int get_open_header_file_count(bool for_read);
    static int get_open_index_file_count(bool for_read);
    static bool validate(Tsdb *tsdb);
    double get_page_percent_used();
    inline size_t c_size() const override { return m_time_range.c_size() + 4; }
    const char *c_str(char *buff) const override;

private:
    friend class tsdb_less;

    //Tsdb(Timestamp start, Timestamp end);
    Tsdb(TimeRange& range, bool existing, const char *suffix = nullptr);
    virtual ~Tsdb();
    void unload();
    void unload_no_lock();
    uint32_t mode_of() const;

    Metric *get_metric(MetricId mid);
    Metric *get_or_create_metric(MetricId mid);
    struct page_info_on_disk *get_page_header(FileIndex file_idx, PageIndex page_idx);

    static Mapping *get_or_add_mapping(const char *metric);
    static bool rotate(TaskData& data);
    static bool archive_ts(TaskData& data);
    static void get_range(Timestamp tstamp, TimeRange& range);
    static Tsdb *create(TimeRange& range, bool existing, const char *suffix = nullptr); // caller needs to acquire m_tsdb_lock!
    static void restore_tsdb(const std::string& dir);

    void add_config(const std::string& name, const std::string& value);
    void write_config(const std::string& dir);
    void restore_config(const std::string& dir);
    void reload_header_data_files(const std::string& dir);

    static std::string get_tsdb_dir_name(const TimeRange& range, const char *suffix = nullptr);
    static std::string get_index_file_name(const TimeRange& range, const char *suffix = nullptr);
    static std::string get_header_file_name(const TimeRange& range, FileIndex id, const char *suffix = nullptr);
    static std::string get_data_file_name(const TimeRange& range, FileIndex id, const char *suffix = nullptr);

    //static std::mutex m_tsdb_lock;
    //static default_contention_free_shared_mutex m_tsdb_lock;
    static pthread_rwlock_t m_tsdb_lock;
    static std::vector<Tsdb*> m_tsdbs;  // ordered by m_start_tstamp

    // This time range will use the time unit specified in the config.
    TimeRange m_time_range;
    std::mutex m_lock;
    int m_ref_count;        // prevent compaction when in use

    IndexFile m_index_file;
    //std::vector<HeaderFile*> m_header_files;
    //std::vector<DataFile*> m_data_files;
    std::mutex m_metrics_lock;
    uint32_t m_mbucket_count;   // max size of m_metrics[]
    std::vector<Metric*> m_metrics; // indexed by MetricId

    // this is true if, 1. m_map is populated; 2. m_page_mgr is open; 3. m_meta_file is open;
    // this is false if all the above are not true;
    uint32_t m_mode;
    //std::atomic<Timestamp> m_load_time; // epoch time in sec
    //Timestamp m_load_time; // epoch time in sec
    //default_contention_free_shared_mutex m_load_lock;

    //PartitionManager *m_partition_mgr;
    PageSize m_page_size;
    PageCount m_page_count;
    int m_compressor_version;
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
