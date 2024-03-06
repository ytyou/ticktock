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

#include <queue>
#include <unordered_map>
#include "dp.h"
#include "mmap.h"
#include "type.h"


namespace tt
{


class QueryTask;
class Tsdb;


class __attribute__ ((__packed__)) RollupManager
{
public:
    RollupManager();
    RollupManager(const RollupManager& copy);   // copy constructor
    RollupManager(Timestamp tstmap, uint32_t cnt, double min, double max, double sum);
    ~RollupManager();

    void copy_from(const struct rollup_entry_ext& entry);
    void copy_from(const RollupManager& other);
    RollupManager& operator=(const RollupManager& other);

    // save (restore) to (from) append log (WAL)
    void append(FILE *file);
    void restore(struct rollup_append_entry *entry);

    static void init();
    static void shutdown();

    // process in-order dps only
    void add_data_point(Tsdb *tsdb, MetricId mid, TimeSeriesId tid, DataPoint& dp);
    void flush(MetricId mid, TimeSeriesId tid);
    void close(TimeSeriesId tid);   // called during TT shutdown

    inline Timestamp get_tstamp() const { return m_tstamp; }

    // return true if data-point found; false if no data
    bool get(struct rollup_entry_ext& entry);
    bool query(RollupType type, DataPointPair& dp);

    static void add_data_file_size(off_t size);
    static off_t get_rollup_data_file_size(bool monthly);
    static int get_rollup_bucket(MetricId mid);
    static RollupDataFile *get_data_file(MetricId mid, Timestamp tstamp);   // get monthly data files
    static RollupDataFile *get_data_file2(MetricId mid, Timestamp tstamp);  // get annual data files
    static RollupDataFile *get_data_file2_no_lock(MetricId mid, Timestamp tstamp);  // get annual data files
    static RollupDataFile *get_data_file_by_bucket_1h(int bucket, Timestamp begin); // get monthly data files
    static RollupDataFile *get_or_create_data_file(MetricId mid, Timestamp tstamp);   // get monthly data files
    static RollupDataFile *get_or_create_data_file_by_bucket_1d(int bucket, Timestamp begin); // get annual data files
    static void get_data_files_1h(MetricId mid, const TimeRange& range, std::vector<RollupDataFile*>& files);   // monthly
    static void get_data_files_1d(MetricId mid, const TimeRange& range, std::vector<RollupDataFile*>& files);   // annual
    static void query(MetricId mid, const TimeRange& range, std::vector<QueryTask*>& tasks, RollupType rollup);
    static void query(MetricId mid, const TimeRange& range, std::unordered_map<TimeSeriesId,struct rollup_entry_ext>& output);
    static double query(struct rollup_entry *entry, RollupType type);
    static void rotate();

private:
    static Timestamp step_down(Timestamp tstamp);
    static RollupDataFile *get_data_file(MetricId mid, Timestamp tstamp, std::unordered_map<uint64_t, RollupDataFile*>& map, bool monthly);
    static RollupDataFile *get_or_create_data_file(MetricId mid, Timestamp tstamp, std::unordered_map<uint64_t, RollupDataFile*>& map, bool monthly);

    uint32_t m_cnt;
    double m_min;
    double m_max;
    double m_sum;
    Timestamp m_tstamp;

    RollupDataFile * m_data_file;       // currently being written

    // used during shutdown/restart of TT
    static RollupDataFile *m_wal_data_file;

    static std::mutex m_lock;
    static std::unordered_map<uint64_t, RollupDataFile*> m_data_files;  // monthly
    static std::mutex m_lock2;
    static std::unordered_map<uint64_t, RollupDataFile*> m_data_files2; // annually
    static std::queue<off_t> m_sizes;   // sizes of 'recent' monthly data files
    static off_t m_size_hint;
};


}
