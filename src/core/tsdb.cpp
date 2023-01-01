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

#include <fstream>
#include <cctype>
#include <algorithm>
#include <cassert>
#include <dirent.h>
#include <functional>
#include <glob.h>
#include <stdio.h>
#include "admin.h"
#include "config.h"
#include "cp.h"
#include "hash.h"
#include "limit.h"
#include "memmgr.h"
#include "meter.h"
#include "mmap.h"
#include "query.h"
#include "timer.h"
#include "tsdb.h"
#include "part.h"
#include "logger.h"
#include "json.h"
#include "stats.h"
#include "utils.h"
#include "leak.h"


namespace tt
{


#define BACK_SUFFIX     ".back"
#define DONE_SUFFIX     ".done"
#define TEMP_SUFFIX     ".temp"

default_contention_free_shared_mutex Tsdb::m_tsdb_lock;
std::vector<Tsdb*> Tsdb::m_tsdbs;
static uint64_t tsdb_rotation_freq = 0;

// maps metrics => Mapping;
std::mutex g_metric_lock;
tsl::robin_map<const char*,Mapping*,hash_func,eq_func> g_metric_map;
thread_local tsl::robin_map<const char*, Mapping*, hash_func, eq_func> thread_local_cache;


Mapping::Mapping(const char *name) :
    //m_partition(nullptr),
    m_ts_head(nullptr),
    //m_ts_head(TT_INVALID_META_POSITION),
    //m_meta_pos(TT_INVALID_META_POSITION),
    m_tag_count(-1)
{
    m_metric = STRDUP(name);
    ASSERT(m_metric != nullptr);
    //ASSERT(m_ts_head.load() == TT_INVALID_META_POSITION);
}

Mapping::~Mapping()
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }
}

void
Mapping::flush(bool close)
{
    //ReadLock guard(m_lock);
    //std::lock_guard<std::mutex> guard(m_lock);

    for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
    {
        Logger::trace("Flushing ts: %T", ts);
        ts->flush(close);
    }
}

TimeSeries *
Mapping::get_ts_head()
{
    return m_ts_head.load();
}

TimeSeries *
Mapping::get_ts(DataPoint& dp)
{
    TimeSeries *ts = nullptr;
    char *raw_tags = dp.get_raw_tags();
    char raw_buff[MAX_TOTAL_TAG_LENGTH];

    if (raw_tags != nullptr)
    {
        TimeSeriesId id = SuperMap::instance()->get(raw_tags);
        if (id != TT_INVALID_TIME_SERIES_ID)
            return TimeSeries::get_ts(id);

        std::strncpy(raw_buff, raw_tags, MAX_TOTAL_TAG_LENGTH);
        dp.parse_raw_tags();
        dp.set_raw_tags(nullptr);
    }

    char ordered_buff[MAX_TOTAL_TAG_LENGTH];
    dp.get_ordered_tags(ordered_buff, MAX_TOTAL_TAG_LENGTH, m_metric);

    std::lock_guard<std::mutex> guard(m_lock);
    ts = SuperMap::instance()->set(ordered_buff, dp);

    ts->m_next = m_ts_head.load();
    m_ts_head = ts;

    if (std::strcmp(raw_buff, ordered_buff) != 0)
        SuperMap::instance()->set_raw(raw_buff, ts->get_id());

    return ts;
}

#if 0

TimeSeries *
Mapping::get_ts3(DataPoint& dp)
{
    TimeSeries *ts = nullptr;
    char *raw_tags = dp.get_raw_tags();
    //std::lock_guard<std::mutex> guard(m_lock);

    if (raw_tags != nullptr)
    {
        ReadLock guard(m_lock);
        auto result = m_map.find(raw_tags);
        if (result != m_map.end())
            ts = result->second;
    }

    if (ts == nullptr)
    {
        if (raw_tags != nullptr)
        {
            raw_tags = STRDUP(raw_tags);
            dp.parse_raw_tags();
            dp.set_raw_tags(raw_tags);
        }

        char buff[MAX_TOTAL_TAG_LENGTH];
        //dp.get_ordered_tags(buff, MAX_TOTAL_TAG_LENGTH);

        int n = std::strlen(m_metric);
        strncpy(buff, m_metric, n);
        buff[n] = ',';
        n++;
        dp.get_ordered_tags(&buff[n], MAX_TOTAL_TAG_LENGTH-n);

        WriteLock guard(m_lock);

        {
            auto result = m_map.find(buff);
            if (result != m_map.end())
                ts = result->second;
        }

        if (ts == nullptr)
        {
            TimeSeriesId id = TT_INVALID_TIME_SERIES_ID;
            MetaPosition pos;
            while (TT_INVALID_META_POSITION == (pos = MetaFile::instance()->put(buff, m_ts_next, id)))
                MetaFile::instance()->expand();
            m_ts_next = pos;

            ASSERT(id != TT_INVALID_TIME_SERIES_ID);
            ts = new TimeSeries(id, m_metric, buff, dp.get_cloned_tags());
            m_map[ts->get_key()] = ts;

            ts->m_next = m_ts_head.load();
            m_ts_head = ts;

            set_tag_count(dp.get_tag_count());
        }

        if ((raw_tags != nullptr) && (strcmp(buff, raw_tags) != 0))
        {
            m_map[raw_tags] = ts;
            TimeSeriesId id = ts->get_id();
            ASSERT(id != TT_INVALID_TIME_SERIES_ID);
            while (TT_INVALID_META_POSITION == MetaFile::instance()->put(raw_tags, TT_INVALID_META_POSITION, id))
                MetaFile::instance()->expand();
        }
    }

    return ts;
}
#endif

void
Mapping::get_all_ts(std::vector<TimeSeries*>& tsv)
{
    for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
        tsv.push_back(ts);
/*
    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        const char *key = it->first;
        TimeSeries *ts = it->second;

        if (ts->get_key() == key)
            tsv.push_back(ts);
    }
*/
}

bool
Mapping::add(DataPoint& dp)
{
    TimeSeries *ts = get_ts(dp);
    if (UNLIKELY(ts == nullptr)) return false;
    bool success = ts->add_data_point(dp);
    return success;
}

bool
Mapping::add_data_point(DataPoint& dp, bool forward)
{
    return add(dp);
#if 0
    bool success = true;

    if (m_partition == nullptr)
    {
        success = add(dp);
    }
    else
    {
        if (m_partition->is_local())
            success = add(dp);

        if (forward)
            success = m_partition->add_data_point(dp) && success;
    }

    return success;
#endif
}

void
Mapping::query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv, const char *key)
{
    int tag_count = TagOwner::get_tag_count(tags);

    if ((key != nullptr) && (tag_count == m_tag_count))
    {
        //ReadLock guard(m_lock);
        //auto result = m_map.find(key);
        //if (result != m_map.end())
            //tsv.insert(result->second);

        //char buff[MAX_TOTAL_TAG_LENGTH];
        //int n = std::strlen(m_metric);
        //std::strncpy(buff, m_metric, n);
        //buff[n] = ',';
        //n++;
        //std::strncpy(&buff[n], key, MAX_TOTAL_TAG_LENGTH-n);

        TimeSeriesId id = SuperMap::instance()->get(key);

        if (id != TT_INVALID_TIME_SERIES_ID)
            tsv.insert(TimeSeries::get_ts(id));
    }

    if (tsv.empty())
    {
        for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
        {
            bool match = true;

            for (Tag *tag = tags; tag != nullptr; tag = tag->next())
            {
                if (! Tag::match_value(ts->get_tags(), tag->m_key, tag->m_value))
                {
                    match = false;
                    break;
                }
            }

            if (match) tsv.insert(ts);
        }
    }
}

TimeSeries *
Mapping::restore_ts(std::string& metric, std::string& keys, TimeSeriesId id)
{
/*
    auto search = m_map.find(keys.c_str());

    if (search != m_map.end())
    {
        Logger::fatal("Duplicate entry in meta file: %s %s %u",
            metric.c_str(), keys.c_str(), id);
        throw std::runtime_error("Duplicate entry in meta file");
    }

    Tag *tags = Tag::parse_multiple(keys);
    TimeSeries *ts = new TimeSeries(id, metric.c_str(), keys.c_str(), tags);
    m_map[ts->get_key()] = ts;
    ts->m_next = m_ts_head.load();
    m_ts_head = ts;
    set_tag_count(std::count(keys.begin(), keys.end(), ';'));
    return ts;
*/
    return nullptr;
}

void
Mapping::set_tag_count(int tag_count)
{
    if (tag_count != m_tag_count)
        m_tag_count = (m_tag_count == -1) ? tag_count : -2;
}

int
Mapping::get_ts_count()
{
    //std::lock_guard<std::mutex> guard(m_lock);
    //ReadLock guard(m_lock);
    //return m_map.size();
    return 0;
}


Tsdb::Tsdb(TimeRange& range, bool existing, const char *suffix) :
    m_time_range(range),
    m_index_file(Tsdb::get_index_file_name(range, suffix)),
    //m_load_time(ts_now_sec()),
    m_partition_mgr(nullptr),
    m_page_size(g_page_size),
    m_page_count(g_page_count)
{
    ASSERT(g_tstamp_resolution_ms ? is_ms(range.get_from()) : is_sec(range.get_from()));

    m_compressor_version =
        Config::get_int(CFG_TSDB_COMPRESSOR_VERSION,CFG_TSDB_COMPRESSOR_VERSION_DEF);

    m_mode = mode_of();
    //m_partition_mgr = new PartitionManager(this, existing);

    Logger::debug("tsdb %T created (mode=%d)", &range, m_mode);
}

Tsdb::~Tsdb()
{
    unload();

    if (m_partition_mgr != nullptr)
    {
        delete m_partition_mgr;
        m_partition_mgr = nullptr;
    }
}

Tsdb *
Tsdb::create(TimeRange& range, bool existing, const char *suffix)
{
    ASSERT(! ((suffix != nullptr) && existing));

    Tsdb *tsdb = new Tsdb(range, existing, suffix);
    std::string dir = get_tsdb_dir_name(range, suffix);

    if (suffix == nullptr)
        m_tsdbs.push_back(tsdb);

    if (existing)
    {
        // restore Header/Data files...
        std::vector<std::string> files;
        get_all_files(dir + "/header.*", files);
        for (auto file: files) tsdb->restore_header(file);
        std::sort(tsdb->m_header_files.begin(), tsdb->m_header_files.end(), header_less());

        // restore page-size & page-count
        if (! tsdb->m_header_files.empty())
        {
            HeaderFile *header_file = tsdb->m_header_files.front();
            header_file->ensure_open(true);
            struct tsdb_header *tsdb_header = header_file->get_tsdb_header();
            ASSERT(tsdb_header != nullptr);
            tsdb->m_page_size = tsdb_header->m_page_size;
            tsdb->m_page_count = tsdb_header->m_page_count;
            tsdb->m_compressor_version = tsdb_header->get_compressor_version();
            if (tsdb_header->is_compacted())
                tsdb->m_mode |= TSDB_MODE_COMPACTED;
            header_file->close();
        }

        files.clear();
        get_all_files(dir + "/data.*", files);
        for (auto file: files) tsdb->restore_data(file);
        std::sort(tsdb->m_data_files.begin(), tsdb->m_data_files.end(), data_less());

        //if ((tsdb->m_mode & TSDB_MODE_READ_WRITE) != 0)
            //tsdb->load_from_disk_no_lock((tsdb->m_mode & TSDB_MODE_WRITE) == 0);

        // restore page-size/page-count
        if (! tsdb->m_header_files.empty())
        {
            HeaderFile *header_file = tsdb->m_header_files.front();
            header_file->ensure_open(true);
            tsdb->m_page_size = header_file->get_page_size();
            tsdb->m_page_count = header_file->get_page_count();

            if ((tsdb->m_mode & TSDB_MODE_READ_WRITE) == 0)
                header_file->close();
        }
    }
    else    // new one
    {
        // create the folder
        create_dir(dir);

        if (suffix == nullptr)
            std::sort(m_tsdbs.begin(), m_tsdbs.end(), tsdb_less());
    }

    return tsdb;
}

// 'dir' should be in the form of /.../<from-sec>.<to-sec>
void
Tsdb::restore_tsdb(const std::string& dir)
{
    std::vector<std::string> tokens;
    tokenize(dir, tokens, '/');
    std::string last = tokens.back();

    tokens.clear();
    tokenize(last, tokens, '.');

    if (tokens.size() != 2)
    {
        // not necessarily an error; could be artifacts of compaction
        Logger::info("tsdb dir %s ignored during restore_tsdb()", dir.c_str());
        return;
    }

    Timestamp start = std::stoull(tokens[0].c_str());
    Timestamp end = std::stoull(tokens[1].c_str());

    if (g_tstamp_resolution_ms)
    {
        start *= 1000L;
        end *= 1000L;
    }

    TimeRange range(start, end);
    Tsdb *tsdb = Tsdb::create(range, true);
}

void
Tsdb::restore_data(const std::string& file)
{
    FileIndex id = get_file_suffix(file);
    ASSERT(id != TT_INVALID_FILE_INDEX);
    DataFile *data_file = new DataFile(file, id, m_page_size, m_page_count);
    //HeaderFile *header_file = get_header_file(id);
    //data_file->init(header_file);
    m_data_files.push_back(data_file);
}

void
Tsdb::restore_header(const std::string& file)
{
    m_header_files.push_back(HeaderFile::restore(file));
}

void
Tsdb::get_all_ts(std::vector<TimeSeries*>& tsv)
{
    for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
    {
        Mapping *mapping = it->second;
        mapping->get_all_ts(tsv);
    }
}

void
Tsdb::get_all_mappings(std::vector<Mapping*>& mappings)
{
    std::lock_guard<std::mutex> guard(g_metric_lock);
    for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
        mappings.push_back(it->second);
}

/* The following configurations determine what mode a Tsdb should be in.
 *   tsdb.archive.threshold - After this amount of time has passed, go into archive mode;
 *   tsdb.read_only.threshold - After this amount of time has passed, go into read-only mode;
 * The input 'count' is the ranking of the Tsdb among all Tsdbs.
 */
uint32_t
Tsdb::mode_of() const
{
    uint32_t mode = TSDB_MODE_NONE;
    Timestamp now = ts_now_sec();
    Timestamp threshold_sec =
        Config::get_time(CFG_TSDB_ARCHIVE_THRESHOLD, TimeUnit::SEC, CFG_TSDB_ARCHIVE_THRESHOLD_DEF);

    if (now < threshold_sec)
    {
        threshold_sec = now;
    }

    if (! m_time_range.older_than_sec(now - threshold_sec))
    {
        threshold_sec =
            Config::get_time(CFG_TSDB_READ_ONLY_THRESHOLD, TimeUnit::SEC, CFG_TSDB_READ_ONLY_THRESHOLD_DEF);

        if (now < threshold_sec)
        {
            threshold_sec = now;
        }

        if (m_time_range.older_than_sec(now - threshold_sec))
        {
            // read-only
            mode = TSDB_MODE_READ;
        }
        else
        {
            // read-write
            mode = TSDB_MODE_READ_WRITE;
        }
    }
    else
    {
        Logger::debug("mode_of: time_range=%T, now=%" PRIu64 ", mode=%x",
            &m_time_range, now, mode);
    }

    return mode;
}

std::string
Tsdb::get_partition_defs() const
{
    //std::string part_file = Tsdb::get_file_name(m_time_range, "part");
    //if (! file_exists(part_file)) return EMPTY_STD_STRING;

    //std::FILE *f = std::fopen(part_file.c_str(), "r");
    //char buff[1024];
    std::string defs;
    //if (std::fgets(buff, sizeof(buff), f) != nullptr)
        //defs.assign(buff);
    //std::fclose(f);
    return defs;
}

PageCount
Tsdb::get_page_count() const
{
    return m_page_count;
}

int
Tsdb::get_compressor_version()
{
    return m_compressor_version;
}

void
Tsdb::get_range(Timestamp tstamp, TimeRange& range)
{
    Timestamp start = (tstamp / tsdb_rotation_freq) * tsdb_rotation_freq;
    range.init(start, start + tsdb_rotation_freq);
}

Mapping *
Tsdb::get_or_add_mapping(DataPoint& dp)
{
    const char *metric = dp.get_metric();
    ASSERT(metric != nullptr);

    auto result = thread_local_cache.find(metric);
    if (result != thread_local_cache.end())
        return result->second;

    Mapping *mapping;

    {
        std::lock_guard<std::mutex> guard(g_metric_lock);
        auto result1 = g_metric_map.find(metric);

        if (result1 == g_metric_map.end())
        {
            mapping = new Mapping(metric);
            g_metric_map[mapping->m_metric] = mapping;
        }
        else
        {
            mapping = result1->second;
        }
    }

    ASSERT(mapping->m_metric != nullptr);
    ASSERT(strcmp(mapping->m_metric, metric) == 0);
    thread_local_cache[mapping->m_metric] = mapping;

    return mapping;
}

bool
Tsdb::add(DataPoint& dp)
{
    ASSERT(m_time_range.in_range(dp.get_timestamp()) == 0);
    Mapping *mapping = get_or_add_mapping(dp);
    if (mapping == nullptr) return false;
    return mapping->add(dp);
}

bool
Tsdb::add_data_point(DataPoint& dp, bool forward)
{
    Mapping *mapping = get_or_add_mapping(dp);
    if (mapping == nullptr) return false;
    return mapping->add_data_point(dp, forward);
}

TimeSeries *
Tsdb::restore_ts(std::string& metric, std::string& key, TimeSeriesId id)
{
    Mapping *mapping;

    auto search = g_metric_map.find(metric.c_str());

    if (search == g_metric_map.end())
    {
        mapping = new Mapping(metric.c_str());
        g_metric_map[mapping->m_metric] = mapping;
    }
    else
    {
        mapping = search->second;
        ASSERT(search->first == mapping->m_metric);
    }

    return mapping->restore_ts(metric, key, id);
}

void
Tsdb::query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts, const char *key)
{
    Mapping *mapping = nullptr;

    {
        std::lock_guard<std::mutex> guard(g_metric_lock);
        auto result = g_metric_map.find(metric);
        if (result != g_metric_map.end())
        {
            mapping = result->second;
            ASSERT(result->first == mapping->m_metric);
        }
    }

    if (mapping != nullptr)
        mapping->query_for_ts(tags, ts, key);
}

bool
Tsdb::query_for_data(TimeSeriesId id, TimeRange& query_range, std::vector<DataPointContainer*>& data)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return query_for_data_no_lock(id, query_range, data);
}

bool
Tsdb::query_for_data_no_lock(TimeSeriesId id, TimeRange& query_range, std::vector<DataPointContainer*>& data)
{
    FileIndex file_idx;
    HeaderIndex header_idx;
    Timestamp from = m_time_range.get_from();
    bool has_ooo = false;

    m_mode |= TSDB_MODE_READ;
    m_index_file.ensure_open(true);
    m_index_file.get_indices(id, file_idx, header_idx);

    while (file_idx != TT_INVALID_FILE_INDEX)
    {
        ASSERT(header_idx != TT_INVALID_HEADER_INDEX);
        ASSERT(file_idx < m_header_files.size());
        ASSERT(m_data_files.size() == m_header_files.size());

        HeaderFile *header_file = m_header_files[file_idx];
        header_file->ensure_open(true);
        struct tsdb_header *tsdb_header = header_file->get_tsdb_header();
        struct page_info_on_disk *page_header = header_file->get_page_header(header_idx);
        TimeRange range(from + page_header->m_tstamp_from, from + page_header->m_tstamp_to);

        if (query_range.has_intersection(range))
        {
            DataFile *data_file = m_data_files[file_idx];
            data_file->ensure_open(true);
            void *page = data_file->get_page(page_header->m_page_index, page_header->m_offset);
            ASSERT(page != nullptr);
            //int compressor_version = header_file->get_compressor_version();
            //ASSERT(! header->is_out_of_order() || (compressor_version == 0));

            DataPointContainer *container = (DataPointContainer*)
                MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
            container->set_out_of_order(page_header->is_out_of_order());
            container->set_page_index(page_header->get_global_page_index(file_idx, m_page_count));
            container->collect_data(from, tsdb_header, page_header, page);
            ASSERT(container->size() > 0);
            data.push_back(container);

            if (page_header->is_out_of_order()) has_ooo = true;
        }

        file_idx = page_header->get_next_file();
        header_idx = page_header->get_next_header();
    }

    return has_ooo;
}

// prepare this Tsdb for query (AND writes too!)
void
Tsdb::ensure_readable(bool count)
{
    bool readable = true;
    //std::lock_guard<std::mutex> guard(m_load_lock);

    {
        ReadLock guard(m_load_lock);

        if ((m_mode & TSDB_MODE_READ) == 0)
            readable = false;
        //else
            //m_load_time = ts_now_sec();

        if (count)
            inc_count();
    }

    if (! readable)
    {
        WriteLock guard(m_load_lock);

        if ((m_mode & TSDB_MODE_READ) == 0)
        {
            m_mode |= TSDB_MODE_READ;
            load_from_disk_no_lock(true);
        }
    }
}

// This will make tsdb read-only!
void
Tsdb::flush(bool sync)
{
    //for (DataFile *file: m_data_files) file->flush(sync);
    for (HeaderFile *file: m_header_files) file->flush(sync);
    m_index_file.flush(sync);
}

// for testing only
void
Tsdb::flush_for_test()
{
    std::vector<TimeSeries*> tsv;
    get_all_ts(tsv);
    for (auto ts: tsv) ts->flush(false);

    //for (DataFile *file: m_data_files) file->flush(sync);
    for (HeaderFile *file: m_header_files) file->flush(sync);
    m_index_file.flush(sync);
}

void
Tsdb::shutdown()
{
    {
        std::lock_guard<std::mutex> guard(g_metric_lock);

        for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
        {
            Mapping *mapping = it->second;
            mapping->flush(true);
        }

        g_metric_map.clear();
    }

    WriteLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        //WriteLock guard(tsdb->m_lock);
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        tsdb->flush(true);
        for (DataFile *file: tsdb->m_data_files) file->close();
        for (HeaderFile *file: tsdb->m_header_files) file->close();
        tsdb->m_index_file.close();
    }

    m_tsdbs.clear();
    CheckPointManager::close();
    MetaFile::instance()->close();
    Logger::info("Tsdb::shutdown complete");
}

PageInfo *
Tsdb::get_free_page_for_compaction()
{
    PageInfo *info = nullptr;

/*
    if (! m_temp_page_mgrs.empty())
        info = m_temp_page_mgrs.back()->get_free_page_for_compaction(this);

    if (info == nullptr)
    {
        int id = m_temp_page_mgrs.empty() ? 0 : m_temp_page_mgrs.back()->get_id() + 1;
        PageManager *pm = new PageManager(m_time_range, id, true);
        m_temp_page_mgrs.push_back(pm);
        info = pm->get_free_page_for_compaction(this);
    }
*/

    return info;
}

PageInfo *
Tsdb::get_the_page_on_disk(PageCount id, PageCount header_index)
{
    return nullptr;     // TODO: implement it
}

void
Tsdb::get_last_header_indices(TimeSeriesId id, FileIndex& file_idx, HeaderIndex& header_idx)
{
    ASSERT(id != TT_INVALID_TIME_SERIES_ID);

    FileIndex fidx;
    HeaderIndex hidx;

    file_idx = TT_INVALID_FILE_INDEX;
    header_idx = TT_INVALID_HEADER_INDEX;

    //ReadLock guard(m_lock);
    std::lock_guard<std::mutex> guard(m_lock);

    m_mode |= TSDB_MODE_READ;
    m_index_file.ensure_open(false);
    m_index_file.get_indices(id, fidx, hidx);

    while (fidx != TT_INVALID_FILE_INDEX)
    {
        ASSERT(hidx != TT_INVALID_HEADER_INDEX);

        file_idx = fidx;
        header_idx = hidx;

        HeaderFile *header_file = get_header_file(fidx);
        ASSERT(header_file != nullptr);
        ASSERT(header_file->get_id() == fidx);

        header_file->ensure_open(true);
        struct page_info_on_disk *header = header_file->get_page_header(hidx);
        fidx = header->m_next_file;
        hidx = header->m_next_header;
    }
}

void
Tsdb::set_indices(TimeSeriesId id, FileIndex prev_file_idx, HeaderIndex prev_header_idx, FileIndex this_file_idx, HeaderIndex this_header_idx)
{
    ASSERT(TT_INVALID_FILE_INDEX != this_file_idx);
    ASSERT(TT_INVALID_HEADER_INDEX != this_header_idx);

    if ((prev_file_idx == TT_INVALID_FILE_INDEX) || (this_header_idx == TT_INVALID_HEADER_INDEX))
        m_index_file.set_indices(id, this_file_idx, this_header_idx);
    else
    {
        // update header
        HeaderFile *header_file = get_header_file(prev_file_idx);
        ASSERT(header_file != nullptr);
        ASSERT(header_file->get_id() == prev_file_idx);
        header_file->ensure_open(false);
        header_file->update_next(prev_header_idx, this_file_idx, this_header_idx);
    }
}

HeaderFile *
Tsdb::get_header_file(FileIndex file_idx)
{
    for (auto file: m_header_files)
    {
        if (file->get_id() == file_idx)
            return file;
    }

    return nullptr;
}

PageSize
Tsdb::append_page(TimeSeriesId id, FileIndex prev_file_idx, HeaderIndex prev_header_idx, struct page_info_on_disk *header, void *page, bool compact)
{
    ASSERT(page != nullptr);
    ASSERT(header != nullptr);

    HeaderFile *header_file;
    const char *suffix = compact ? TEMP_SUFFIX : nullptr;
    std::lock_guard<std::mutex> guard(m_lock);
    //WriteLock guard(m_lock);

    //m_load_time = ts_now_sec();
    m_mode |= TSDB_MODE_READ_WRITE;

    if (m_header_files.empty())
    {
        ASSERT(m_data_files.empty());
        header_file = new HeaderFile(get_header_file_name(m_time_range, 0, suffix), 0, get_page_count(), this),
        m_header_files.push_back(header_file);
        m_data_files.push_back(new DataFile(get_data_file_name(m_time_range, 0, suffix), 0, get_page_size(), get_page_count()));
    }
    else
    {
        header_file = m_header_files.back();
        header_file->ensure_open(false);

        if (header_file->is_full())
        {
            FileIndex id = m_header_files.size();
            header_file = new HeaderFile(get_header_file_name(m_time_range, id, suffix), id, get_page_count(), this);
            m_header_files.push_back(header_file);
            m_data_files.push_back(new DataFile(get_data_file_name(m_time_range, id, suffix), id, get_page_size(), get_page_count()));
        }
    }

    DataFile *data_file = m_data_files.back();

    data_file->ensure_open(false);
    //header_file->ensure_open(false);
    m_index_file.ensure_open(false);

    ASSERT(! m_data_files.empty());
    ASSERT(! header_file->is_full());
    ASSERT(m_header_files.size() == m_data_files.size());
    ASSERT(header_file->get_id() == data_file->get_id());

    header->m_offset = data_file->get_offset();
    header->m_page_index = data_file->append(page, compact?header->get_size():get_page_size());

    struct tsdb_header *tsdb_header = header_file->get_tsdb_header();
    ASSERT(tsdb_header != nullptr);

    if (tsdb_header->m_page_index < header->m_page_index)
        tsdb_header->m_page_index = header->m_page_index;

    if (UNLIKELY(compact))
        tsdb_header->set_compacted(true);

    HeaderIndex header_idx = header_file->new_header_index(this);
    ASSERT(header_idx != TT_INVALID_HEADER_INDEX);
    struct page_info_on_disk *new_header = header_file->get_page_header(header_idx);
    //ASSERT(header->m_page_index == new_header->m_page_index);
    new_header->init(header);

    set_indices(id, prev_file_idx, prev_header_idx, header_file->get_id(), header_idx);

    // passing our own indices back to caller
    header->m_next_file = header_file->get_id();
    header->m_next_header = header_idx;

    return data_file->get_next_page_size();
}

Tsdb *
Tsdb::search(Timestamp tstamp)
{
    if (! m_tsdbs.empty())
    {
        // TODO: binary search?
        for (int i = m_tsdbs.size() - 1; i >= 0; i--)
        {
            Tsdb *tsdb = m_tsdbs[i];
            int n = tsdb->in_range(tstamp);
            if (n == 0) return tsdb;
            if (n > 0) break;
        }
    }

    return nullptr;
}

Tsdb *
Tsdb::inst(Timestamp tstamp, bool create)
{
    Tsdb *tsdb = nullptr;

    {
        ReadLock guard(m_tsdb_lock);
        tsdb = Tsdb::search(tstamp);
    }

    if (tsdb != nullptr) return tsdb;

    // create one
    if (create)
    {
        WriteLock guard(m_tsdb_lock);
        tsdb = Tsdb::search(tstamp);  // search again to avoid race condition
        if (tsdb == nullptr)
        {
            TimeRange range;
            Tsdb::get_range(tstamp, range);
            tsdb = Tsdb::create(range, false);  // create new
            Logger::info("Created %T, tstamp = %" PRIu64, tsdb, tstamp);
            ASSERT(Tsdb::search(tstamp) == tsdb);
        }
    }

    return tsdb;
}

bool
Tsdb::load_from_disk(bool for_read)
{
    std::lock_guard<std::mutex> guard(m_lock);
    //WriteLock guard(m_lock);
    return load_from_disk_no_lock(for_read);
}

bool
Tsdb::load_from_disk_no_lock(bool for_read)
{
    Meter meter(METRIC_TICKTOCK_TSDB_LOAD_TOTAL_MS);
    bool success = true;

    return success;
/*
    if (! m_index_file.is_open())
        m_index_file.open(for_read);

    for (HeaderFile *header : m_header_files)
    {
        if (! header->is_open())
            header->open(for_read);
    }

    for (PageManager *pm: m_page_mgrs)
    {
        if (! pm->is_open())
            success = pm->reopen() && success;
    }

    for (PageCount id = m_page_mgrs.size(); ; id++)
    {
        std::string file_name = get_file_name(m_time_range, std::to_string(id));
        if (! file_exists(file_name)) break;
        PageManager *pm = new PageManager(m_time_range, id);
        success = pm->reopen() && success;
        m_page_mgrs.push_back(pm);
    }

    if (! success) return false;

    // check/set compacted flag
    bool compacted = ! m_page_mgrs.empty();
    for (PageManager *pm: m_page_mgrs)
    {
        if (! pm->is_compacted())
        {
            compacted = false;
            break;
        }
    }

    //m_meta_file.load(this);
    //m_meta_file.open(); // open for append

    m_mode |= TSDB_MODE_READ;
    if (compacted) m_mode |= TSDB_MODE_COMPACTED;
    m_load_time = ts_now_sec();
    Logger::info("Loaded %T (lt=%" PRIu64 ")", this, m_load_time.load());
    return true;
*/
}

void
Tsdb::insts(const TimeRange& range, std::vector<Tsdb*>& tsdbs)
{
    ReadLock guard(m_tsdb_lock);
    int i, size = m_tsdbs.size();
    if (size == 0) return;

    Timestamp begin = m_tsdbs[0]->m_time_range.get_from();
    Timestamp end = m_tsdbs[size-1]->m_time_range.get_to();

    if (end < range.get_from()) return;
    if (range.get_to() < begin) return;

    Timestamp duration = (end - begin) / (Timestamp)size;

    int lower, upper;

    // estimate lower bound
    if (range.get_from() < begin)
        lower = 0;
    else
    {
        lower = (range.get_from() - begin) / duration;
        if (lower >= size) lower = size - 1;
    }

    // estimate upper bound
    if (end <= range.get_to())
        upper = size - 1;
    else
    {
        upper = (range.get_to() - begin) / duration;
        if (upper >= size) upper = size - 1;
    }

    // adjust lower bound
    Tsdb *tsdb = m_tsdbs[lower];

    while (range.get_from() < tsdb->m_time_range.get_from())
    {
        if (lower == 0) break;
        lower--;
        tsdb = m_tsdbs[lower];
    }

    // adjust upper bound
    tsdb = m_tsdbs[upper];

    while (tsdb->m_time_range.get_to() < range.get_to())
    {
        if (upper == (size-1)) break;
        upper++;
        tsdb = m_tsdbs[upper];
    }

    for (i = lower; i <= upper; i++)
    {
        tsdb = m_tsdbs[i];
        if (tsdb->in_range(range))
            tsdbs.push_back(tsdb);
    }
}

bool
Tsdb::http_api_put_handler(HttpRequest& request, HttpResponse& response)
{
    Tsdb* tsdb = nullptr;
    char *curr = request.content;
    bool success = true;

    ReadLock *guard = nullptr;

    while ((curr != nullptr) && (*curr != 0))
    {
        DataPoint dp;

        if (*curr == ';') curr++;
        curr = dp.from_http(curr);

        if ((tsdb == nullptr) || (tsdb->in_range(dp.get_timestamp()) != 0))
        {
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                if (! tsdb->load_from_disk(false))
                {
                    success = false;
                    break;
                }
            }
            else
            {
                //tsdb->m_load_time = ts_now_sec();
                //ASSERT(tsdb->m_meta_file.is_open());
            }
            tsdb->m_mode |= TSDB_MODE_READ_WRITE;
        }

        success = tsdb->add(dp) && success;
    }

    if (guard != nullptr) delete guard;
    response.status_code = (success ? 200 : 500);
    response.content_length = 0;

    return success;
}

bool
Tsdb::http_api_put_handler_json(HttpRequest& request, HttpResponse& response)
{
    char *curr = strchr(request.content, '[');

    if (curr == nullptr)
    {
        response.init(400, HttpContentType::PLAIN);
        return false;
    }

    Tsdb* tsdb = nullptr;
    bool success = true;

    ReadLock *guard = nullptr;

    while ((*curr != ']') && (*curr != 0))
    {
        DataPoint dp;
        curr = dp.from_json(curr+1);
        if (curr == nullptr) break;

        //char buff[1024];
        //Logger::info("dp: %s", dp.c_str(buff, sizeof(buff)));

        if ((tsdb == nullptr) || (tsdb->in_range(dp.get_timestamp()) != 0))
        {
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                if (! tsdb->load_from_disk(false))
                {
                    success = false;
                    break;
                }
            }
            else
            {
                //tsdb->m_load_time = ts_now_sec();
            }
            tsdb->m_mode |= TSDB_MODE_READ_WRITE;
        }

        success = tsdb->add(dp) && success;
        while (isspace(*curr)) curr++;
    }

    if (guard != nullptr) delete guard;
    response.init((success ? 200 : 500), HttpContentType::PLAIN);
    return success;
}

bool
Tsdb::http_api_put_handler_plain(HttpRequest& request, HttpResponse& response)
{
    Logger::trace("Entered http_api_put_handler_plain()...");

    char *curr = request.content;
    bool forward = request.forward;
    bool success = true;

    // handle special requests
    if (request.length <= 10)
    {
        if ((request.length == 8) && (strncmp(curr, "version\n", 8) == 0))
        {
            return HttpServer::http_get_api_version_handler(request, response);
        }
        else if ((request.length == 6) && (strncmp(curr, "stats\n", 6) == 0))
        {
            return HttpServer::http_get_api_stats_handler(request, response);
        }
        else if ((request.length == 5) && (strncmp(curr, "help\n", 5) == 0))
        {
            return HttpServer::http_get_api_help_handler(request, response);
        }
        else if ((request.length == 10) && (strncmp(curr, "diediedie\n", 10) == 0))
        {
            char buff[10];
            std::strcpy(buff, "cmd=stop");
            request.params = buff;
            return Admin::http_post_api_admin_handler(request, response);
        }
    }

    response.content_length = 0;

    // safety measure
    curr[request.length] = ' ';
    curr[request.length+1] = '\n';
    curr[request.length+2] = '0';

    while ((curr != nullptr) && (*curr != 0))
    {
        DataPoint dp;

        if (std::isspace(*curr)) break;
        if (UNLIKELY(std::strncmp(curr, "put ", 4) != 0))
        {
            if (std::strncmp(curr, "version\n", 8) == 0)
            {
                curr += 8;
                HttpServer::http_get_api_version_handler(request, response);
            }
            else if (std::strncmp(curr, "cp ", 3) == 0)
            {
                curr += 3;
                char *cp = curr;
                curr = strchr(curr, '\n');
                if (curr == nullptr) break;
                *curr = 0;
                curr++;

                CheckPointManager::add(cp);
            }
            else if (std::strncmp(curr, DONT_FORWARD, std::strlen(DONT_FORWARD)) == 0)
            {
                int len = std::strlen(DONT_FORWARD);
                curr += len;
                forward = false;
                response.init(200, HttpContentType::PLAIN, len, DONT_FORWARD);
            }
            else
            {
                curr = strchr(curr, '\n');
                if (curr == nullptr) break;
                curr++;
            }
            if ((curr - request.content) > request.length) break;
            continue;
        }

        curr += 4;
        bool ok = dp.from_plain(curr);
        if (! ok) { success = false; break; }

        success = add_data_point(dp, forward) && success;
    }

    // TODO: Now what???
    //if (forward && (tsdb != nullptr))
        //success = tsdb->submit_data_points() && success; // flush
    response.init((success ? 200 : 500), HttpContentType::PLAIN);

    return success;
}

bool
Tsdb::http_api_put_handler_plain2(HttpRequest& request, HttpResponse& response)
{
    Logger::trace("Entered http_api_put_handler_plain()...");

    Tsdb* tsdb = nullptr;
    char *curr = request.content;
    bool forward = request.forward;
    bool success = true;

    // is the the 'version' command?
    if (request.length <= 10)
    {
        if ((request.length == 8) && (strncmp(curr, "version\n", 8) == 0))
        {
            return HttpServer::http_get_api_version_handler(request, response);
        }
        else if ((request.length == 6) && (strncmp(curr, "stats\n", 6) == 0))
        {
            return HttpServer::http_get_api_stats_handler(request, response);
        }
        else if ((request.length == 5) && (strncmp(curr, "help\n", 5) == 0))
        {
            return HttpServer::http_get_api_help_handler(request, response);
        }
        else if ((request.length == 10) && (strncmp(curr, "diediedie\n", 10) == 0))
        {
            char buff[10];
            std::strcpy(buff, "cmd=stop");
            request.params = buff;
            return Admin::http_post_api_admin_handler(request, response);
        }
    }

    response.content_length = 0;

    // safety measure
    curr[request.length] = ' ';
    curr[request.length+1] = '\n';
    curr[request.length+2] = '0';

    ReadLock *guard = nullptr;

    while ((curr != nullptr) && (*curr != 0))
    {
        DataPoint dp;

        if (std::isspace(*curr)) break;
        if (UNLIKELY(std::strncmp(curr, "put ", 4) != 0))
        {
            if (std::strncmp(curr, "version\n", 8) == 0)
            {
                curr += 8;
                HttpServer::http_get_api_version_handler(request, response);
            }
            else if (std::strncmp(curr, "cp ", 3) == 0)
            {
                curr += 3;
                char *cp = curr;
                curr = strchr(curr, '\n');
                if (curr == nullptr) break;
                *curr = 0;
                curr++;

                CheckPointManager::add(cp);
            }
            else if (std::strncmp(curr, DONT_FORWARD, std::strlen(DONT_FORWARD)) == 0)
            {
                int len = std::strlen(DONT_FORWARD);
                curr += len;
                forward = false;
                response.init(200, HttpContentType::PLAIN, len, DONT_FORWARD);
            }
            else
            {
                curr = strchr(curr, '\n');
                if (curr == nullptr) break;
                curr++;
            }
            if ((curr - request.content) > request.length) break;
            continue;
        }

        curr += 4;
        bool ok = dp.from_plain(curr);

        if (! ok) { success = false; break; }

        if ((tsdb == nullptr) || (tsdb->in_range(dp.get_timestamp()) != 0))
        {
            if (tsdb != nullptr)
            {
                if ((curr - request.content) > request.length)
                {
                    // most likely the last line was cut off half way
                    success = false;
                    break;
                }
                if (forward)
                    success = tsdb->submit_data_points() && success; // flush
            }
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                if (! tsdb->load_from_disk(false))
                {
                    success = false;
                    break;
                }
            }
            else
            {
                //tsdb->m_load_time = ts_now_sec();
                //ASSERT(tsdb->m_meta_file.is_open());
            }
            tsdb->m_mode |= TSDB_MODE_READ_WRITE;
        }

        ASSERT(tsdb != nullptr);
        ASSERT((tsdb->m_mode & TSDB_MODE_READ_WRITE) == TSDB_MODE_READ_WRITE);
        success = tsdb->add_data_point(dp, forward) && success;
    }

    if (forward && (tsdb != nullptr))
        success = tsdb->submit_data_points() && success; // flush
    if (guard != nullptr) delete guard;
    response.init((success ? 200 : 500), HttpContentType::PLAIN);

    return success;
}

bool
Tsdb::http_get_api_suggest_handler(HttpRequest& request, HttpResponse& response)
{
    size_t buff_size = MemoryManager::get_network_buffer_size() - 6;

    JsonMap params;
    request.parse_params(params);

    auto search = params.find("type");
    if (search == params.end())
    {
        response.init(400, HttpContentType::PLAIN);
        return false;
    }
    const char *type = search->second->to_string();

    search = params.find("q");
    if (search == params.end())
    {
        response.init(400, HttpContentType::PLAIN);
        return false;
    }
    const char *prefix = search->second->to_string();

    int max = 1000;
    search = params.find("max");
    if (search != params.end())
    {
        max = std::atoi(search->second->to_string());
    }

    JsonParser::free_map(params);
    Logger::debug("type = %s, prefix = %s, max = %d", type, prefix, max);

    std::set<std::string> suggestions;

    if (std::strcmp(type, "metrics") == 0)
    {
        //ReadLock guard(m_tsdb_lock);
        std::lock_guard<std::mutex> guard(g_metric_lock);

        for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
        {
            const char *metric = it->first;

            if (starts_with(metric, prefix))
            {
                suggestions.insert(std::string(metric));
                if (suggestions.size() >= max) break;
            }
        }
    }
    else if (std::strcmp(type, "tagk") == 0)
    {
        //ReadLock guard(m_tsdb_lock);
        std::lock_guard<std::mutex> guard(g_metric_lock);

        for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
        {
            Mapping *mapping = it->second;
            ASSERT(it->first == mapping->m_metric);

            //ReadLock mapping_guard(mapping->m_lock);

            //for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
            for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
                ts->get_keys(suggestions);
        }
    }
    else if (std::strcmp(type, "tagv") == 0)
    {
        //ReadLock guard(m_tsdb_lock);
        std::lock_guard<std::mutex> guard(g_metric_lock);

        for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
        {
            Mapping *mapping = it->second;
            ASSERT(it->first == mapping->m_metric);

            //ReadLock mapping_guard(mapping->m_lock);

            //for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
            for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
                ts->get_values(suggestions);
        }
    }
    else
    {
        response.init(400, HttpContentType::PLAIN);
        Logger::warn("Unrecognized suggest type: %s", type);
        return false;
    }

    char* buff = MemoryManager::alloc_network_buffer();
    int n = JsonParser::to_json(suggestions, buff, buff_size);
    response.init(200, HttpContentType::JSON, n, buff);
    MemoryManager::free_network_buffer(buff);

    return true;
}

void
Tsdb::init()
{
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);
    Logger::info("Loading data from %s", data_dir.c_str());

    CheckPointManager::init();
    //PartitionManager::init();
    SuperMap::init();
    TimeSeries::init();

    tsdb_rotation_freq =
        Config::get_time(CFG_TSDB_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_TSDB_ROTATION_FREQUENCY_DEF);
    if (g_tstamp_resolution_ms)
        tsdb_rotation_freq *= 1000L;
    if (tsdb_rotation_freq < 1) tsdb_rotation_freq = 1;

    // check if we have enough disk space
    PageCount page_count =
        Config::get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    uint64_t avail = get_disk_available_blocks(data_dir);

    if (avail <= page_count)
    {
        Logger::error("Not enough disk space at %s (%" PRIu64 " <= %ld)",
            data_dir.c_str(), avail, page_count);
    }
    else if (avail <= (2 * page_count))
    {
        Logger::warn("Low disk space at %s", data_dir.c_str());
    }

    // restore all tsdbs
    for_all_dirs(data_dir, Tsdb::restore_tsdb, 3);
    std::sort(m_tsdbs.begin(), m_tsdbs.end(), tsdb_less());
    Logger::debug("%d Tsdbs restored", m_tsdbs.size());

    MetaFile::init(Tsdb::restore_ts);

    compact2();

/*
    dir = opendir(data_dir.c_str());

    if (dir != nullptr)
    {
        while (dir_ent = readdir(dir))
        {
            //if (starts_with(dir_ent->d_name, '.')) continue;
            if (! ends_with(dir_ent->d_name, ".meta")) continue;

            // file name should be in the following format:
            // 1564401600.1564408800.meta
            std::vector<std::string> tokens;
            tokenize(dir_ent->d_name, tokens, '.');
            if (tokens.size() != 3) continue;

            // these are always in seconds
            Timestamp start = std::stoull(tokens[0].c_str());
            Timestamp end = std::stoull(tokens[1].c_str());

            if (g_tstamp_resolution_ms)
            {
                start *= 1000L;
                end *= 1000L;
            }

            TimeRange range(start, end);
            Tsdb *tsdb = Tsdb::create(range, true); // create existing
            ASSERT(tsdb != nullptr);
            //m_tsdbs.push_back(tsdb);
        }

        closedir(dir);

        std::sort(m_tsdbs.begin(), m_tsdbs.end(), tsdb_less());
    }
*/

    Task task;
    task.doit = &Tsdb::rotate;
    int freq_sec = Config::get_time(CFG_TSDB_FLUSH_FREQUENCY, TimeUnit::SEC, CFG_TSDB_FLUSH_FREQUENCY_DEF);
    if (freq_sec < 1) freq_sec = 1;
    Timer::inst()->add_task(task, freq_sec, "tsdb_flush");
    Logger::info("Will try to rotate tsdb every %d secs.", freq_sec);

    task.doit = &Tsdb::compact;
    task.data.integer = 0;  // indicates this is from scheduled task (vs. interactive cmd)
    freq_sec = Config::get_time(CFG_TSDB_COMPACT_FREQUENCY, TimeUnit::SEC, CFG_TSDB_COMPACT_FREQUENCY_DEF);
    if (freq_sec > 0)
    {
        Timer::inst()->add_task(task, freq_sec, "tsdb_compact");
        Logger::info("Will try to compact tsdb every %d secs.", freq_sec);
    }
}

std::string
Tsdb::get_tsdb_dir_name(const TimeRange& range, const char *suffix)
{
    time_t sec = range.get_from_sec();
    struct tm timeinfo;
    localtime_r(&sec, &timeinfo);
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/%d/%d/%" PRIu64 ".%" PRIu64 "%s",
        Config::get_str(CFG_TSDB_DATA_DIR,CFG_TSDB_DATA_DIR_DEF).c_str(),
        timeinfo.tm_year+1900,
        timeinfo.tm_mon+1,
        range.get_from_sec(),
        range.get_to_sec(),
        (suffix==nullptr) ? "" : suffix);
    return std::string(buff);
}

std::string
Tsdb::get_index_file_name(const TimeRange& range, const char *suffix)
{
    std::string dir = get_tsdb_dir_name(range, suffix);
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/index", dir.c_str());
    return std::string(buff);
}

std::string
Tsdb::get_header_file_name(const TimeRange& range, FileIndex id, const char *suffix)
{
    std::string dir = get_tsdb_dir_name(range, suffix);
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/header.%u", dir.c_str(), id);
    return std::string(buff);
}

std::string
Tsdb::get_data_file_name(const TimeRange& range, FileIndex id, const char *suffix)
{
    std::string dir = get_tsdb_dir_name(range, suffix);
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/data.%u", dir.c_str(), id);
    return std::string(buff);
}

int
Tsdb::get_metrics_count()
{
    std::lock_guard<std::mutex> guard(g_metric_lock);
    return g_metric_map.size();
}

int
Tsdb::get_dp_count()
{
    return 0;       // TODO: implement it
}

int
Tsdb::get_ts_count()
{
    //return TimeSeries::get_next_id();
    return 0;
}

// for testing only
int
Tsdb::get_page_count(bool ooo)
{
    int total = 0;
    for (auto tsdb: m_tsdbs)
    {
        for (auto header_file: tsdb->m_header_files)
            total += header_file->count_pages(ooo);
    }
    return total;
}

int
Tsdb::get_data_page_count()
{
    int total = 0;
    for (auto tsdb: m_tsdbs)
    {
        for (auto header_file: tsdb->m_header_files)
        {
            header_file->ensure_open(true);
            total = std::max(total, (int)header_file->get_page_index());
        }
    }
    return total + 1;
}

double
Tsdb::get_page_percent_used()
{
    return 0.0;     // TODO: implement it
}

int
Tsdb::get_active_tsdb_count()
{
    int total = 0;
    ReadLock guard(m_tsdb_lock);
    for (Tsdb *tsdb: m_tsdbs)
    {
        if (tsdb->m_index_file.is_open(true))
            total++;
    }
    return total;
}

int
Tsdb::get_total_tsdb_count()
{
    ReadLock guard(m_tsdb_lock);
    return (int)m_tsdbs.size();
}

int
Tsdb::get_open_data_file_count(bool for_read)
{
    int total = 0;
    ReadLock guard(m_tsdb_lock);
    for (Tsdb *tsdb: m_tsdbs)
    {
        for (auto df: tsdb->m_data_files)
        {
            if (df->is_open(for_read))
                total++;
        }
    }
    return total;
}

void
Tsdb::unload()
{
    WriteLock unload_guard(m_load_lock);
    //std::lock_guard<std::mutex> guard(m_load_lock);
    unload_no_lock();
}

// This will archive the tsdb. No write nor read will be possible afterwards.
void
Tsdb::unload_no_lock()
{
    if (! count_is_zero()) return;
    for (DataFile *file: m_data_files) file->close();
    for (HeaderFile *file: m_header_files) file->close();
    m_index_file.close();
    m_mode &= ~TSDB_MODE_READ_WRITE;
}

bool
Tsdb::rotate(TaskData& data)
{
    if (g_shutdown_requested) return false;

    Meter meter(METRIC_TICKTOCK_TSDB_ROTATE_MS);
    Timestamp now = ts_now();
    std::vector<Tsdb*> tsdbs;
    uint64_t disk_avail = Stats::get_disk_avail();

    TimeRange range(0, now);
    Tsdb::insts(range, tsdbs);

    // adjust CFG_TSDB_ARCHIVE_THRESHOLD when system available memory is low
    if (Stats::get_avphys_pages() < 1600)
    {
        Timestamp archive_threshold =
            Config::get_time(CFG_TSDB_ARCHIVE_THRESHOLD, TimeUnit::DAY, CFG_TSDB_ARCHIVE_THRESHOLD_DEF);
        Timestamp rotation_freq =
            Config::get_time(CFG_TSDB_ROTATION_FREQUENCY, TimeUnit::DAY, CFG_TSDB_ROTATION_FREQUENCY_DEF);

        if (rotation_freq < 1) rotation_freq = 1;
        uint64_t days = archive_threshold / rotation_freq;

        if (days > 1)
        {
            // reduce CFG_TSDB_ARCHIVE_THRESHOLD by 1 day
            Config::set_value(CFG_TSDB_ARCHIVE_THRESHOLD, std::to_string(days-1)+"d");
            Logger::info("Reducing %s by 1 day", CFG_TSDB_ARCHIVE_THRESHOLD);
        }
    }

    CheckPointManager::take_snapshot();

    uint64_t now_sec = to_sec(now);
    uint64_t thrashing_threshold =
        Config::get_time(CFG_TSDB_THRASHING_THRESHOLD, TimeUnit::SEC, CFG_TSDB_THRASHING_THRESHOLD_DEF);

    for (Tsdb *tsdb: tsdbs)
    {
        if (g_shutdown_requested) break;

        WriteLock unload_guard(tsdb->m_load_lock);
        //std::lock_guard<std::mutex> unload_guard(tsdb->m_load_lock);
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        if (! (tsdb->m_mode & TSDB_MODE_READ))
        {
            Logger::info("[rotate] %T already archived!", tsdb);
            continue;    // already archived
        }

        uint32_t mode = tsdb->mode_of();
//        uint64_t load_time = tsdb->m_load_time;

        if (disk_avail < 100000000L)    // TODO: get it from config
            mode &= ~TSDB_MODE_WRITE;

        bool all_closed = true;

        for (DataFile *data_file: tsdb->m_data_files)
        {
            if (data_file->is_open(true))
            {
                Timestamp last_read = data_file->get_last_read();
                if (((int64_t)now_sec - (int64_t)last_read) > (int64_t)thrashing_threshold)
                    data_file->close(1);    // close read
                else
                {
                    all_closed = false;
#ifdef _DEBUG
                    Logger::info("data file %u last accessed at %" PRIu64 "; now is %" PRIu64,
                        data_file->get_id(), last_read, now_sec);
#endif
                }
            }

            if (data_file->is_open(false))
            {
                Timestamp last_write = data_file->get_last_write();
                if (((int64_t)now_sec - (int64_t)last_write) > (int64_t)thrashing_threshold)
                {
                    data_file->close(2);    // close write

                    if (! data_file->is_open(true))
                    {
                        // close the header file as well
                        FileIndex id = data_file->get_id();
                        HeaderFile *header_file = tsdb->m_header_files[id];
                        header_file->close();
                    }
                }
                else
                    all_closed = false;
            }
        }

        tsdb->flush(true);

        if (all_closed)
        {
            Logger::info("[rotate] Archiving %T", tsdb);
            tsdb->unload_no_lock();
        }
/*
        if (((int64_t)now_sec - (int64_t)load_time) > (int64_t)thrashing_threshold)
        {
            if (! (mode & TSDB_MODE_READ))
            {
                // archive it
                Logger::info("[rotate] Archiving %T (lt=%" PRIu64 ", now=%" PRIu64 ")", tsdb, load_time, now_sec);
                tsdb->flush(true);
                tsdb->unload_no_lock();
                continue;
            }
            else if (((mode & TSDB_MODE_READ_WRITE) == TSDB_MODE_READ) && (tsdb->m_mode & TSDB_MODE_WRITE))
            {
                // make it read-only
                Logger::debug("[rotate] Flushing tsdb: %T", tsdb);
                tsdb->flush(true);
                continue;
            }
        }
#ifdef _DEBUG
        else if (! (mode & TSDB_MODE_READ))
        {
            Logger::info("%T: now_sec = %" PRIu64 "; load_time = %" PRIu64 "; threshold = %" PRIu64,
                tsdb, now_sec, load_time, thrashing_threshold);
        }
#endif
        //else if (! (mode & TSDB_MODE_READ) && tsdb->count_is_zero())
        //{
            //for (PageManager* pm: tsdb->m_page_mgrs)
                //pm->try_unload();
        //}
*/
    }

    CheckPointManager::persist();
    MetaFile::instance()->flush();

    if (Config::exists(CFG_TSDB_RETENTION_THRESHOLD))
        purge_oldest(Config::get_int(CFG_TSDB_RETENTION_THRESHOLD));

    return false;
}

bool
Tsdb::validate(Tsdb *tsdb)
{
    ReadLock guard(m_tsdb_lock);

    for (Tsdb *t: m_tsdbs)
    {
        if (t == tsdb) return true;
    }

    return false;
}

void
Tsdb::purge_oldest(int threshold)
{
    Tsdb *tsdb = nullptr;

    {
        WriteLock guard(m_tsdb_lock);

        if (m_tsdbs.size() <= threshold) return;

        if (! m_tsdbs.empty())
        {
            tsdb = m_tsdbs.front();
            Timestamp now = ts_now_sec();

/*
            if ((now - tsdb->m_load_time) > 7200)   // TODO: config?
            {
                m_tsdbs.erase(m_tsdbs.begin());
            }
            else
            {
                tsdb = nullptr;
            }
*/
        }
    }

/*
    if (tsdb != nullptr)
    {
        Logger::info("[rotate] Purging %T permenantly", tsdb);

        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        tsdb->flush(true);
        tsdb->unload();

        // purge files on disk
        std::string file_name = Tsdb::get_file_name(tsdb->m_time_range, "meta");
        rm_file(file_name);

        for (int i = 0; ; i++)
        {
            file_name = Tsdb::get_file_name(tsdb->m_time_range, std::to_string(i));
            if (! file_exists(file_name)) break;
            rm_file(file_name);
        }

        delete tsdb;
    }
*/
}

bool
Tsdb::compact(TaskData& data)
{
    Tsdb *tsdb = nullptr;
    Meter meter(METRIC_TICKTOCK_TSDB_COMPACT_MS);

    // called from scheduled task? if so, enforce off-hour rule;
    if ((data.integer == 0) && ! is_off_hour()) return false;

    Logger::info("[compact] Finding tsdbs to compact...");

    // Go through all the Tsdbs, from the oldest to the newest,
    // to find the first uncompacted Tsdb to compact.
    {
        ReadLock guard(m_tsdb_lock);

        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); it++)
        {
            std::lock_guard<std::mutex> guard((*it)->m_lock);
            //WriteLock guard((*it)->m_lock);

            // also make sure it's not readable nor writable while we are compacting
            if ((*it)->m_mode & (TSDB_MODE_COMPACTED | TSDB_MODE_READ_WRITE))
                continue;

            tsdb = *it;
            break;
        }
    }

    if (tsdb != nullptr)
    {
        WriteLock load_lock(tsdb->m_load_lock);
        //std::lock_guard<std::mutex> guard(m_load_lock);

        Logger::info("[compact] Found this tsdb to compact: %T", tsdb);
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        TimeRange range = tsdb->get_time_range();
        //MetaFile meta_file(get_file_name(range, "meta", true));

        // perform compaction
        try
        {
            // cleanup existing temporary tsdb, if any
            std::string dir = get_tsdb_dir_name(range, TEMP_SUFFIX);
            if (ends_with(dir, TEMP_SUFFIX)) // safty check
            {
                rm_all_files(dir + "/*");
                rm_file(dir);   // will do empty dir as well
            }

            // create a temporary tsdb to compact data into
            Tsdb *compacted = Tsdb::create(range, false, TEMP_SUFFIX);
            compacted->m_page_size = g_sys_page_size;
            std::vector<Tsdb*> tsdbs = { tsdb };
            TimeSeriesId max_id = 0; //TimeSeries::get_next_id();
            QueryTask *query =
                (QueryTask*)MemoryManager::alloc_recyclable(RecyclableType::RT_QUERY_TASK);
            PageSize next_size = compacted->get_page_size();

            // for each time series...
            for (TimeSeriesId id = 0; id < max_id; id++)
            {
                // collect dps
                query->init(&tsdbs);
                query->perform(id);

                // save into temp tsdb
                PageInMemory page(id, compacted, false, next_size);
                DataPointVector& dps = query->get_dps();

                for (auto dp: dps)
                {
                    const Timestamp tstamp = dp.first;
                    bool ok = page.add_data_point(tstamp, dp.second);

                    if (! ok)
                    {
                        next_size = page.flush(id, true);
                        ASSERT(next_size > 0);
                        page.init(id, nullptr, false, next_size);
                        ok = page.add_data_point(tstamp, dp.second);
                        ASSERT(ok);
                    }
                }

                if (! page.is_empty())
                {
                    next_size = page.flush(id, true);
                    ASSERT(next_size > 0);
                }

                query->recycle();
            }

            MemoryManager::free_recyclable(query);

            // rename to indicate compaction was successful
            std::string temp_name = get_tsdb_dir_name(range, TEMP_SUFFIX);
            std::string done_name = get_tsdb_dir_name(range, DONE_SUFFIX);
            int rc = std::rename(temp_name.c_str(), done_name.c_str());
            compact2();

            // mark it as compacted
            tsdb->m_mode |= TSDB_MODE_COMPACTED;
            tsdb->unload_no_lock();
            Logger::info("[compact] 1 Tsdb compacted");
        }
        catch (const std::exception& ex)
        {
            Logger::error("[compact] compaction failed: %s", ex.what());
        }
        catch (...)
        {
            Logger::error("[compact] compaction failed for unknown reasons");
        }
    }
    else
    {
        Logger::info("[compact] Did not find any appropriate Tsdb to compact.");
    }

    return false;
}

void
Tsdb::compact2()
{
    glob_t result;
    std::string pattern = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);

    pattern.append("/*/*/*");
    pattern.append(DONE_SUFFIX);
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);

    std::vector<std::string> done_dirs;

    for (unsigned int i = 0; i < result.gl_pathc; i++)
    {
        done_dirs.push_back(std::string(result.gl_pathv[i]));
    }

    globfree(&result);

    for (auto& done_dir: done_dirs)
    {
        Logger::info("Found %s done compactions", done_dir.c_str());

        // format: <data-directory>/<year>/<month>/1614009600.1614038400.done
        ASSERT(ends_with(done_dir, DONE_SUFFIX));
        std::string base = done_dir.substr(0, done_dir.size()-std::strlen(DONE_SUFFIX));
        std::string back = base + BACK_SUFFIX;

        // mv 1614009600.1614038400 to 1614009600.1614038400.back
        if (file_exists(base))
            std::rename(base.c_str(), back.c_str());

        // mv 1614009600.1614038400.done to 1614009600.1614038400
        std::rename(done_dir.c_str(), base.c_str());

        // rm 1614009600.1614038400.back
        if (file_exists(base) && file_exists(back))
        {
            // TODO: enable this
            //rm_all_files(back + "/*");
            //rm_file(back);  // will do empty dir as well
        }
    }
}

const char *
Tsdb::c_str(char *buff) const
{
    strcpy(buff, "tsdb");
    m_time_range.c_str(&buff[4]);
    return buff;
}


}
