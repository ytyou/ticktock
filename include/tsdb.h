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

#define TSDB_MODE_READ_WRITE    (TSDB_MODE_READ | TSDB_MODE_WRITE)


class Tsdb;
class Mapping;
class DataPointContainer;


class Measurement
{
public:
    Measurement();
    Measurement(uint32_t ts_count);
    ~Measurement();

    void add_ts(int idx, TimeSeries *ts);
    TimeSeries *add_ts(const char *field, Mapping *mapping);
    bool add_data_points(std::vector<DataPoint*>& dps, Timestamp ts, Mapping *mapping);
    TimeSeries *get_ts(int idx, const char *field);
    inline uint32_t get_ts_count() const { return m_ts_count; }
    inline void set_ts_count(uint32_t ts_count);
    inline bool is_initialized() const { return m_time_series != nullptr; }

    std::mutex m_lock;

private:
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

    friend class Tsdb;

private:
    Mapping(const char *name);
    ~Mapping();

    void flush(bool close);

    char *m_metric;
    bool add(DataPoint& dp);
    bool add_data_point(DataPoint& dp, bool forward);
    bool add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint*>& dps);
    TimeSeries *get_ts(DataPoint& dp);
    Measurement *get_measurement(char *raw_tags, TagOwner& owner);
    void query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv, const char *key);
    TimeSeries *restore_ts(std::string& metric, std::string& key, TimeSeriesId id);
    void restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv);
    void set_tag_count(int tag_count);
    TimeSeries *get_ts_head();

    int get_dp_count();
    int get_ts_count();

    //std::mutex m_lock;
    default_contention_free_shared_mutex m_lock;
    tsl::robin_map<const char*,TimeSeries*,hash_func,eq_func> m_map;
    //std::unordered_map<const char*,TimeSeries*,hash_func,eq_func> m_map;

    default_contention_free_shared_mutex m_lock2;
    tsl::robin_map<const char*,Measurement*,hash_func,eq_func> m_map2;

    std::atomic<TimeSeries*> m_ts_head;
    int16_t m_tag_count;    // -1: uninitialized; -2: inconsistent;

    //Partition *m_partition;
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
    static bool add_data_point(DataPoint& dp, bool forward);
    static TimeSeries *restore_ts(std::string& metric, std::string& key, TimeSeriesId id);
    static void restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv);
    static void get_all_ts(std::vector<TimeSeries*>& tsv);
    static void get_all_mappings(std::vector<Mapping*>& mappings);

    bool add(DataPoint& dp);

    static void query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts, const char *key);
    bool query_for_data(TimeSeriesId id, TimeRange& range, std::vector<DataPointContainer*>& data);
    bool query_for_data_no_lock(TimeSeriesId id, TimeRange& range, std::vector<DataPointContainer*>& data);

    void flush(bool sync);
    void flush_for_test();  // for testing only

    inline PageSize get_page_size() const { return m_page_size; }
    PageCount get_page_count() const;
    int get_compressor_version();

    void get_last_header_indices(TimeSeriesId id, FileIndex& file_idx, HeaderIndex& header_idx);
    void set_indices(TimeSeriesId id, FileIndex prev_file_idx, HeaderIndex prev_header_idx,
                     FileIndex this_file_idx, HeaderIndex this_header_idx);
    PageSize append_page(TimeSeriesId id,       // return next page size
                         FileIndex prev_file_idx,
                         HeaderIndex prev_header_idx,
                         struct page_info_on_disk *header,
                         void *page,
                         bool compact);
    HeaderFile *get_header_file(FileIndex file_idx);

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

    // http add data-point request handler
    static bool http_api_put_handler_json(HttpRequest& request, HttpResponse& response);
    static bool http_api_put_handler_plain(HttpRequest& request, HttpResponse& response);
    static bool http_api_write_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_suggest_handler(HttpRequest& request, HttpResponse& response);

    // parse 1-line of the InfluxDB line protocol
    static bool parse_line(char* &line, const char* &measurement, char* &tags, Timestamp& ts, std::vector<DataPoint*>& dps, std::vector<DataPoint*>& tmp);
    static bool add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint*>& dps);

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

    struct page_info_on_disk *get_page_header(FileIndex file_idx, PageIndex page_idx);

    static Mapping *get_or_add_mapping(const char *metric);
    static bool rotate(TaskData& data);
    static bool archive_ts(TaskData& data);
    static void get_range(Timestamp tstamp, TimeRange& range);
    static Tsdb *create(TimeRange& range, bool existing, const char *suffix = nullptr); // caller needs to acquire m_tsdb_lock!
    static void restore_tsdb(const std::string& dir);

    void restore_data(const std::string& file);
    void restore_header(const std::string& file);

    static std::string get_tsdb_dir_name(const TimeRange& range, const char *suffix = nullptr);
    static std::string get_index_file_name(const TimeRange& range, const char *suffix = nullptr);
    static std::string get_header_file_name(const TimeRange& range, FileIndex id, const char *suffix = nullptr);
    static std::string get_data_file_name(const TimeRange& range, FileIndex id, const char *suffix = nullptr);

    //static std::mutex m_tsdb_lock;
    static default_contention_free_shared_mutex m_tsdb_lock;
    static std::vector<Tsdb*> m_tsdbs;  // ordered by m_start_tstamp

    // This time range will use the time unit specified in the config.
    TimeRange m_time_range;
    std::mutex m_lock;

    IndexFile m_index_file;
    std::vector<HeaderFile*> m_header_files;
    std::vector<DataFile*> m_data_files;

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
