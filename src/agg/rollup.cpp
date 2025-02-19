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

#include <iomanip>
#include <limits.h>
#include <unordered_map>
#include "config.h"
#include "compress.h"
#include "limit.h"
#include "logger.h"
#include "query.h"
#include "rollup.h"
#include "tsdb.h"
#include "utils.h"
#include "cal.h"


namespace tt
{


std::mutex RollupManager::m_lock;
std::unordered_map<uint64_t, RollupDataFile*> RollupManager::m_data_files;  // monthly
std::mutex RollupManager::m_lock2;
std::unordered_map<uint64_t, RollupDataFile*> RollupManager::m_data_files2; // annually
RollupDataFile *RollupManager::m_wal_data_file = nullptr;
std::queue<int64_t> RollupManager::m_sizes;
int64_t RollupManager::m_size_hint;
std::mutex RollupManager::m_cfg_lock;
std::unordered_map<uint32_t, Config*> RollupManager::m_configs;


RollupManager::RollupManager() :
    m_cnt(0),
    m_min(std::numeric_limits<double>::max()),
    m_max(std::numeric_limits<double>::lowest()),
    m_sum(0.0),
    m_tstamp(TT_INVALID_TIMESTAMP),
    m_data_file(nullptr)
{
}

// Copy Constructor
RollupManager::RollupManager(const RollupManager& copy)
{
    copy_from(copy);
}

RollupManager::RollupManager(Timestamp tstamp, uint32_t cnt, double min, double max, double sum) :
    m_cnt(cnt),
    m_min(min),
    m_max(max),
    m_sum(sum),
    m_tstamp(tstamp),
    m_data_file(nullptr)
{
    ASSERT(cnt != 0);
    ASSERT(tstamp != TT_INVALID_TIMESTAMP);
}

RollupManager::~RollupManager()
{
}

void
RollupManager::copy_from(const RollupManager& other)
{
    m_cnt = other.m_cnt;
    m_min = other.m_min;
    m_max = other.m_max;
    m_sum = other.m_sum;
    m_tstamp = other.m_tstamp;
    m_data_file = other.m_data_file;
    m_prev = other.m_prev;
}

void
RollupManager::copy_from(const struct rollup_entry_ext2& entry)
{
    m_cnt = entry.cnt;
    m_min = entry.min;
    m_max = entry.max;
    m_sum = entry.sum;
    m_tstamp = to_sec(entry.tstamp);

    m_prev.m_prev_cnt = entry.prev_cnt;
    m_prev.m_prev_min = entry.prev_min;
    m_prev.m_prev_min_delta = entry.prev_min_delta;
    m_prev.m_prev_max = entry.prev_max;
    m_prev.m_prev_max_delta = entry.prev_max_delta;
    m_prev.m_prev_sum = entry.prev_sum;
    m_prev.m_prev_sum_delta = entry.prev_sum_delta;
}

RollupManager&
RollupManager::operator=(const RollupManager& other)
{
    if (this == &other)
        return *this;

    copy_from(other);
    return *this;
}

void
RollupManager::init()
{
    std::string wal_dir = Config::get_wal_dir();
    std::string wal_file_name = wal_dir + "/rollup.data";

    create_dir(wal_dir);
    m_wal_data_file = new RollupDataFile(wal_file_name, 0);

    // restore if necessary
    if (! m_wal_data_file->empty())
    {
        std::unordered_map<TimeSeriesId,struct rollup_entry_ext2> map;

        m_wal_data_file->open(true);
        m_wal_data_file->query_ext2(TimeRange::MAX, map);

        Tsdb::restore_rollup_mgr(map);
        m_wal_data_file->close();
        rm_file(wal_file_name);
    }
}

void
RollupManager::shutdown()
{
    {
        std::lock_guard<std::mutex> guard(m_lock);

        for (const auto& entry: m_data_files)
        {
            RollupDataFile *file = entry.second;
            delete file;
        }

        m_data_files.clear();
    }

    {
        std::lock_guard<std::mutex> guard(m_lock2);

        for (const auto& entry: m_data_files2)
        {
            RollupDataFile *file = entry.second;
            delete file;
        }

        m_data_files2.clear();
    }

    if (m_wal_data_file != nullptr)
    {
        m_wal_data_file->close();
        delete m_wal_data_file;
        m_wal_data_file = nullptr;
    }

    {
        std::lock_guard<std::mutex> guard(m_cfg_lock);

        for (const auto& entry: m_configs)
        {
            Config *cfg = entry.second;
            delete cfg;
        }

        m_configs.clear();
    }
}

// Here we only handle in-order dps!
void
RollupManager::add_data_point(Tsdb *tsdb, MetricId mid, TimeSeriesId tid, DataPoint& dp)
{
    ASSERT(tsdb != nullptr);
    Timestamp tstamp = to_sec(dp.get_timestamp());
    Timestamp interval = g_rollup_interval_1h;
    double value = dp.get_value();

    if (m_tstamp == TT_INVALID_TIMESTAMP)
        m_tstamp = Calendar::begin_month_of(tstamp);

    // step-down
    ASSERT(interval > 0);
    Timestamp tstamp1 = tstamp - (tstamp % interval);

    if (tstamp1 > m_tstamp)
    {
        flush(mid, tid);

        Timestamp end = Calendar::end_month_of(m_tstamp);

        for (m_tstamp += interval; (m_tstamp < end) && (m_tstamp < tstamp1); m_tstamp += interval)
            flush(mid, tid);

        if (m_tstamp >= end)
        {
            for (m_tstamp = Calendar::begin_month_of(tstamp); m_tstamp < tstamp1; m_tstamp += interval)
                flush(mid, tid);
        }
    }
    else if (tstamp1 < m_tstamp)
    {
        // out-of-order!!!
        tsdb->set_out_of_order2(tid, true);
    }

    m_cnt++;
    m_min = std::min(m_min, value);
    m_max = std::max(m_max, value);
    m_sum += value;
}

void
RollupManager::flush(MetricId mid, TimeSeriesId tid)
{
    ASSERT(m_tstamp != TT_INVALID_TIMESTAMP);

    if (m_tstamp == TT_INVALID_TIMESTAMP)
        return;

    // write to rollup files
    if ((m_data_file == nullptr) || (m_data_file->get_begin_timestamp() != Calendar::begin_month_of(m_tstamp)))
    {
        if (m_data_file != nullptr)
            m_data_file->dec_ref_count();
        m_data_file = get_or_create_data_file(mid, m_tstamp);
    }

    ASSERT(m_data_file != nullptr);
    ASSERT(m_data_file->get_begin_timestamp() == begin_month(m_tstamp));

    int size;
    uint8_t buff[128];

    short version = m_data_file->get_compressor_version();
    double precision = m_data_file->get_compressor_precision();

    if (version == 1)
        size = RollupCompressor_v1::compress(buff, tid, m_cnt, m_min, m_max, m_sum, precision);
    else
        size = RollupCompressor_v2::compress(buff, tid, m_cnt, m_min, m_max, m_sum, precision, this);

    m_data_file->add_data_point(buff, size);

    // reset
    m_cnt = 0;
    m_min = std::numeric_limits<double>::max();
    m_max = std::numeric_limits<double>::lowest();
    m_sum = 0.0;
}

void
RollupManager::close(TimeSeriesId tid)
{
    if (m_tstamp == TT_INVALID_TIMESTAMP || m_cnt == 0)
        return;

    if (m_wal_data_file != nullptr)
    {
        m_wal_data_file->add_data_point_to_wal(tid, m_tstamp, m_cnt, m_min, m_max, m_sum,
            m_prev.m_prev_cnt, m_prev.m_prev_min, m_prev.m_prev_min_delta,
            m_prev.m_prev_max, m_prev.m_prev_max_delta, m_prev.m_prev_sum, m_prev.m_prev_sum_delta);
    }
}

void
RollupManager::encode(uint32_t cnt, double min, double max, double sum, int64_t& cnt_delta, double& min_dod, double& max_dod, double& sum_dod)
{
    cnt_delta = cnt - m_prev.m_prev_cnt;
    m_prev.m_prev_cnt = cnt;

    if (cnt == 0) return;

    double min_delta = min - m_prev.m_prev_min;
    min_dod = min_delta - m_prev.m_prev_min_delta;

    double max_delta = max - m_prev.m_prev_max;
    max_dod = max_delta - m_prev.m_prev_max_delta;

    double sum_delta = sum - m_prev.m_prev_sum;
    sum_dod = sum_delta - m_prev.m_prev_sum_delta;

    m_prev.m_prev_min = min;
    m_prev.m_prev_min_delta = min_delta;
    m_prev.m_prev_max = max;
    m_prev.m_prev_max_delta = max_delta;
    m_prev.m_prev_sum = sum;
    m_prev.m_prev_sum_delta = sum_delta;
}

uint32_t
RollupManager::decode_cnt(int64_t cnt_delta)
{
    m_prev.m_prev_cnt += cnt_delta;
    ASSERT(0 <= m_prev.m_prev_cnt && m_prev.m_prev_cnt <= UINT32_MAX);
    return (uint32_t)m_prev.m_prev_cnt;
}

double
RollupManager::query(struct rollup_entry *entry, RollupType type)
{
    ASSERT(entry != nullptr);
    ASSERT(entry->cnt != 0);
    ASSERT(type != RollupType::RU_NONE);
    ASSERT((type & RollupType::RU_LEVEL2) == 0);

    double val = 0;

    switch (type)
    {
        case RollupType::RU_AVG:
            val = entry->sum / (double)entry->cnt;
            break;

        case RollupType::RU_CNT:
            val = (double)entry->cnt;
            break;

        case RollupType::RU_MAX:
            val = entry->max;
            break;

        case RollupType::RU_MIN:
            val = entry->min;
            break;

        case RollupType::RU_SUM:
            val = entry->sum;
            break;

        default:
            ASSERT(false);
    }

    return val;
}

// return false if no data will be returned;
bool
RollupManager::query(RollupType type, DataPointPair& dp)
{
    if (m_cnt == 0) return false;

    switch (type)
    {
        case RollupType::RU_AVG:
            dp.second = m_sum / (double)m_cnt;
            break;

        case RollupType::RU_CNT:
            dp.second = (double)m_cnt;
            break;

        case RollupType::RU_MAX:
            dp.second = m_max;
            break;

        case RollupType::RU_MIN:
            dp.second = m_min;
            break;

        case RollupType::RU_SUM:
            dp.second = m_sum;
            break;

        default:
            return false;
    }

    dp.first = m_tstamp;
    return true;
}

/* Retrieve rollup data currently in cache.
 *
 * @return false if no data will be returned;
 */
bool
RollupManager::get(struct rollup_entry_ext& entry)
{
    if (m_cnt == 0) return false;

    entry.cnt = m_cnt;
    entry.max = m_max;
    entry.min = m_min;
    entry.sum = m_sum;
    entry.tstamp = m_tstamp;

    return true;
}

/* Query rollup data stored in rollup files. Data currently in cache will not be returned.
 * It will query either 1h rollup data or 1d rollup data, depending on the 'rollup' argument.
 */
void
RollupManager::query(MetricId mid, const TimeRange& range, std::vector<QueryTask*>& tasks, RollupType rollup)
{
    ASSERT(! tasks.empty());
    ASSERT(rollup != RollupType::RU_NONE);

    std::vector<RollupDataFile*> data_files;

    bool level2 = is_rollup_level2(rollup);

    if (level2)
        get_data_files_1d(mid, range, data_files);
    else
        get_data_files_1h(mid, range, data_files);

    for (auto file: data_files)
    {
        if (level2)
        {
            std::unordered_map<TimeSeriesId,QueryTask*> map;
            for (auto task: tasks) map[task->get_ts_id()] = task;
            file->query2(range, map, rollup);
        }
        else
            file->query(range, tasks, rollup);
        file->dec_ref_count();
    }
}

/* @return Stepped down timestamp of the input, in seconds.
 */
Timestamp
RollupManager::step_down(Timestamp tstamp)
{
    tstamp = to_sec(tstamp);
    return tstamp - (tstamp % g_rollup_interval_1h);
}

int
RollupManager::get_rollup_bucket(MetricId mid)
{
    // TODO: CFG_TSDB_ROLLUP_BUCKETS shouldn't be changed
    return mid % Config::inst()->get_int(CFG_TSDB_ROLLUP_BUCKETS,CFG_TSDB_ROLLUP_BUCKETS_DEF);
}

// get monthly data file
RollupDataFile *
RollupManager::get_or_create_data_file(MetricId mid, Timestamp tstamp)
{
    Timestamp begin = Calendar::begin_month_of(tstamp);
    std::lock_guard<std::mutex> guard(m_lock);
    return get_or_create_data_file(mid, begin, m_data_files, true);
}

RollupDataFile *
RollupManager::get_or_create_data_file(MetricId mid, Timestamp tstamp, std::unordered_map<uint64_t, RollupDataFile*>& map, bool monthly)
{
    // calc a unique 'bucket' for each year/month
    uint64_t bucket = tstamp * MAX_ROLLUP_BUCKET_COUNT + get_rollup_bucket(mid);
    auto search = map.find(bucket);
    RollupDataFile *data_file = nullptr;

    if (search == map.end())
    {
        data_file = new RollupDataFile(mid, tstamp, monthly);
        map.insert(std::make_pair(bucket, data_file));
    }
    else
        data_file = search->second;

    ASSERT(data_file != nullptr);
    data_file->inc_ref_count();

    return data_file;
}

RollupDataFile *
RollupManager::get_data_file(MetricId mid, Timestamp tstamp, std::unordered_map<uint64_t, RollupDataFile*>& map, bool monthly)
{
    // calc a unique 'bucket' for each year/month
    uint64_t bucket = tstamp * MAX_ROLLUP_BUCKET_COUNT + get_rollup_bucket(mid);
    auto search = map.find(bucket);
    RollupDataFile *data_file = nullptr;

    if (search == map.end())
    {
        int year, month;
        get_year_month(tstamp, year, month);

        std::string name =
            monthly ?
                RollupDataFile::get_name_by_mid_1h(mid, year, month) :
                RollupDataFile::get_name_by_mid_1d(mid, year);

        if (file_exists(name))
        {
            data_file = new RollupDataFile(mid, tstamp, monthly);
            map.insert(std::make_pair(bucket, data_file));
        }
    }
    else
        data_file = search->second;

    if (data_file != nullptr)
        data_file->inc_ref_count();

    return data_file;
}

void
RollupManager::get_data_files_1h(MetricId mid, const TimeRange& range, std::vector<RollupDataFile*>& files)
{
    int year, month;
    Timestamp end = range.get_to_sec();

    get_year_month(range.get_from_sec(), year, month);
    month--;
    year -= 1900;

    if (year < 70)
    {
        year = 70;
        month = 0;
    }

    std::lock_guard<std::mutex> guard(m_lock);

    for ( ; ; )
    {
        Timestamp ts = begin_month(year, month);
        if (end <= ts) break;

        RollupDataFile *data_file = get_data_file(mid, ts, m_data_files, true);

        if (data_file != nullptr)
        {
            if (data_file->empty())
                data_file->dec_ref_count();
            else
                files.push_back(data_file);
        }

        month++;
        if (month >= 12)
        {
            month = 0;
            year++;
        }
        if (year > 400) break;
    }
}

void
RollupManager::get_data_files_1d(MetricId mid, const TimeRange& range, std::vector<RollupDataFile*>& files)
{
    int year, month;
    Timestamp end = range.get_to_sec();

    get_year_month(range.get_from_sec(), year, month);
    year -= 1900;

    if (year < 70)
        year = 70;

    std::lock_guard<std::mutex> guard(m_lock2);

    for ( ; ; )
    {
        Timestamp ts = begin_month(year, 0);
        if (end <= ts) break;

        RollupDataFile *data_file = get_data_file(mid, ts, m_data_files2, false);

        if (data_file != nullptr)
        {
            if (data_file->empty())
                data_file->dec_ref_count();
            else
                files.push_back(data_file);
        }

        year++;
        if (year > 400) break;
    }
}

/* @param tstamp beginning of month, in seconds
 */
RollupDataFile *
RollupManager::get_data_file_by_bucket_1h(int bucket, Timestamp tstamp)
{
    ASSERT(bucket >= 0);
    ASSERT(is_sec(tstamp));

    uint64_t key = tstamp * MAX_ROLLUP_BUCKET_COUNT + bucket;
    std::lock_guard<std::mutex> guard(m_lock);
    auto search = m_data_files.find(key);
    RollupDataFile *data_file = nullptr;

    if (search == m_data_files.end())
    {
        int year, month;
        get_year_month(tstamp, year, month);
        std::string name = RollupDataFile::get_name_by_bucket_1h(bucket, year, month);

        if (file_exists(name))
        {
            data_file = new RollupDataFile(name, tstamp);
            m_data_files.insert(std::make_pair(key, data_file));
        }
    }
    else
    {
        data_file = search->second;
        ASSERT(data_file != nullptr);
    }

    if (data_file != nullptr)
        data_file->inc_ref_count();

    return data_file;
}

RollupDataFile *
RollupManager::get_or_create_data_file_by_bucket_1d(int bucket, Timestamp tstamp)
{
    ASSERT(bucket >= 0);
    ASSERT(is_sec(tstamp));

    uint64_t key = tstamp * MAX_ROLLUP_BUCKET_COUNT + bucket;
    std::lock_guard<std::mutex> guard(m_lock2);
    auto search = m_data_files2.find(key);
    RollupDataFile *data_file = nullptr;

    if (search == m_data_files2.end())
    {
        data_file = new RollupDataFile(bucket, tstamp);
        m_data_files2.insert(std::make_pair(key, data_file));
    }
    else
        data_file = search->second;

    ASSERT(data_file != nullptr);
    data_file->inc_ref_count();

    return data_file;
}

// get annual data file
RollupDataFile *
RollupManager::get_data_file2(MetricId mid, Timestamp tstamp)
{
    Timestamp begin = begin_year(tstamp);
    std::lock_guard<std::mutex> guard(m_lock2);
    return get_data_file(mid, begin, m_data_files2, false);
}

void
RollupManager::rotate()
{
    uint64_t thrashing_threshold = g_rollup_interval_1h;    // 1 hour
    Timestamp now = ts_now_sec();

    {
        std::lock_guard<std::mutex> guard(m_lock);

        for (auto it = m_data_files.begin(); it != m_data_files.end(); )
        {
            RollupDataFile *file = it->second;

            if (file->close_if_idle(thrashing_threshold, now))
            {
                RollupManager::add_data_file_size(file->size());
                it = m_data_files.erase(it);
                delete file;
            }
            else
                it++;
        }
    }

    {
        std::lock_guard<std::mutex> guard(m_lock2);

        for (auto it = m_data_files2.begin(); it != m_data_files2.end(); )
        {
            RollupDataFile *file = it->second;

            if (file->close_if_idle(thrashing_threshold, now))
            {
                it = m_data_files2.erase(it);
                delete file;
            }
            else
                it++;
        }
    }
}

// Remember sizes of recent rollup data files
void
RollupManager::add_data_file_size(int64_t size)
{
    if (m_sizes.size() >= 10)   // keep 10 most recent sizes
    {
        m_size_hint -= m_sizes.front();
        m_sizes.pop();
    }

    m_sizes.push(size);
    m_size_hint += size;
}

int64_t
RollupManager::get_rollup_data_file_size(bool monthly)
{
    //std::lock_guard<std::mutex> guard(m_lock);
    int64_t monthly_size;

    if (m_sizes.empty())
    {
        // estimate rollup data file size
        monthly_size = TimeSeries::get_next_id();
        monthly_size *= 24 * 30 * 10;
        monthly_size /= Config::inst()->get_int(CFG_TSDB_ROLLUP_BUCKETS, CFG_TSDB_ROLLUP_BUCKETS_DEF);
    }
    else
        monthly_size = m_size_hint / m_sizes.size();

    if (monthly)
        return monthly_size;

    return (monthly_size / 28) * sizeof(struct rollup_entry_ext);
}

void
RollupManager::append(FILE *file)
{
    struct rollup_append_entry entry;

    entry.cnt = m_cnt;
    entry.min = m_min;
    entry.max = m_max;
    entry.sum = m_sum;
    entry.tstamp = m_tstamp;

    int ret;
    ret = fwrite(&entry, 1, sizeof(entry), file);
    if (ret != sizeof(entry))
        Logger::error("RollupManager::append() failed, expected=%d, actual=%d", sizeof(entry), ret);
}

void
RollupManager::restore(struct rollup_append_entry *entry)
{
    ASSERT(entry != nullptr);

    m_cnt = entry->cnt;
    m_min = entry->min;
    m_max = entry->max;
    m_sum = entry->sum;
    m_tstamp = entry->tstamp;
}

Config *
RollupManager::get_rollup_config(int year, bool create)
{
    return get_rollup_config(year, 0, create);
}

Config *
RollupManager::get_rollup_config(int year, int month, bool create)
{
    ASSERT(1970 <= year && year < 3000);
    ASSERT(0 <= month && month <= 12);

    uint32_t key = year * 100 + month;
    std::lock_guard<std::mutex> guard(m_cfg_lock);
    auto search = m_configs.find(key);
    Config *cfg = nullptr;
    bool monthly;

    if (search == m_configs.end())
    {
        std::string dir_name;

        if (month == 0)
        {
            // 1d
            monthly = false;
            std::ostringstream oss;
            oss << Config::get_data_dir() << "/"
                << std::to_string(year) << "/rollup";
            dir_name = oss.str();
        }
        else
        {
            // 1h
            monthly = true;
            std::ostringstream oss;
            oss << Config::get_data_dir() << "/"
                << std::to_string(year) << "/"
                << std::setfill('0') << std::setw(2) << month << "/rollup";
            dir_name = oss.str();
        }

        if (create)
        {
            if (! file_exists(dir_name))
            {
                create_dir(dir_name);

                // create config file
                cfg = new Config(dir_name + "/config");
                int compressor = monthly ? CFG_TSDB_ROLLUP_COMPRESSOR_VERSION_DEF : 0;
                    //Config::inst()->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_VERSION, CFG_TSDB_ROLLUP_COMPRESSOR_VERSION_DEF);
                int precision =
                    Config::inst()->get_int(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION_DEF);
                cfg->set_value(CFG_TSDB_ROLLUP_COMPRESSOR_VERSION, std::to_string(compressor));
                cfg->set_value(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION, std::to_string(precision));
                cfg->set_value(CFG_TSDB_ROLLUP_BUCKETS,
                    std::to_string(Config::inst()->get_int(CFG_TSDB_ROLLUP_BUCKETS, CFG_TSDB_ROLLUP_BUCKETS_DEF)));
                cfg->persist();
            }
            else
            {
                // load config file
                cfg = new Config(dir_name + "/config");
                cfg->load(false);
            }
        }
        else if (file_exists(dir_name + "/config"))
        {
            // load config file
            cfg = new Config(dir_name + "/config");
            cfg->load(false);
        }

        if (cfg != nullptr)
            m_configs.insert(std::make_pair(key, cfg));
    }
    else
    {
        cfg = search->second;
    }

    return cfg;
}


}
