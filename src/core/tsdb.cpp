/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),
    Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).

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
#include "append.h"
#include "config.h"
#include "memmgr.h"
#include "meter.h"
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


default_contention_free_shared_mutex Tsdb::m_tsdb_lock;
std::vector<Tsdb*> Tsdb::m_tsdbs;
static long tsdb_rotation_freq = 0;
static thread_local tsl::robin_map<const char*, Mapping*, hash_func, eq_func> thread_local_cache;


Mapping::Mapping() :
    m_metric(nullptr),
    m_tsdb(nullptr),
    m_partition(nullptr)
{
}

void
Mapping::init(const char *name, Tsdb *tsdb)
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
    }

    m_metric = STRDUP(name);
    m_tsdb = tsdb;
    m_partition = tsdb->get_partition(name);

    m_map.clear();
    m_map.rehash(16);
}

Mapping::~Mapping()
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }

    unload(false);
}

void
Mapping::unload(bool release)
{
    std::unordered_set<TimeSeries*> tss;
    WriteLock guard(m_lock);

    // More than one key could be mapped to the same
    // TimeSeries; so we need to dedup first to avoid
    // double free it.
    for (auto it = m_map.begin(); it != m_map.end(); it++)
        tss.insert(it->second);

    for (auto ts: tss)
        MemoryManager::free_recyclable(ts);

    m_map.clear();
    m_tsdb = nullptr;

    if (release)
    {
        MemoryManager::free_recyclable(this);
    }
}

void
Mapping::flush()
{
    ReadLock guard(m_lock);

    if (m_tsdb == nullptr) return;

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        TimeSeries *ts = it->second;
        char buff[128];
        Logger::trace("flushing ts: %s", ts->c_str(buff, sizeof(buff)));
        ts->flush(true);
    }
}

bool
Mapping::recycle()
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }

    m_map.clear();

    return true;
}

void
Mapping::set_check_point()
{
    WriteLock guard(m_lock);

    for (auto it = m_map.begin(); (it != m_map.end()) && (!g_shutdown_requested); it++)
    {
        TimeSeries *ts = it->second;
        ts->set_check_point();
    }
}

TimeSeries *
Mapping::get_ts(TagOwner& to)
{
    char buff[1024];
    to.get_ordered_tags(buff, sizeof(buff));

    TimeSeries *ts = nullptr;

    {
        ReadLock guard(m_lock);

        // work-around for possibly a bug in std::unordered_map
        if (m_map.bucket_count() != 0)
        {
            auto result = m_map.find(buff);
            if (result != m_map.end())
            {
                ts = result->second;
            }
        }
    }

    if (ts == nullptr)
    {
        WriteLock guard(m_lock);

        // work-around for possibly a bug in std::unordered_map
        if (m_map.bucket_count() == 0)
        {
            m_map.rehash(16);
            ASSERT(m_map.bucket_count() != 0);
        }
        else
        {
            auto result = m_map.find(buff);
            if (result != m_map.end())
            {
                ts = result->second;
            }
        }

        if (ts == nullptr)
        {
            ts = (TimeSeries*)MemoryManager::inst()->alloc_recyclable(RecyclableType::RT_TIME_SERIES);
            ts->init(m_metric, buff, to.get_cloned_tags(), m_tsdb, false);
            m_map[ts->get_key()] = ts;
        }
    }

    return ts;
}

TimeSeries *
Mapping::get_ts2(DataPoint& dp)
{
    TimeSeries *ts = nullptr;
    char *raw_tags = dp.get_raw_tags();

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

        char buff[1024];
        dp.get_ordered_tags(buff, sizeof(buff));

        WriteLock guard(m_lock);

        // work-around for possibly a bug in std::unordered_map
        //if (m_map.bucket_count() == 0)
        //{
            //m_map.rehash(16);
            //ASSERT(m_map.bucket_count() != 0);
        //}
        //else
        {
            auto result = m_map.find(buff);
            if (result != m_map.end())
            {
                ts = result->second;
            }
        }

        if (ts == nullptr)
        {
            ts = (TimeSeries*)MemoryManager::inst()->alloc_recyclable(RecyclableType::RT_TIME_SERIES);
            ts->init(m_metric, buff, dp.get_cloned_tags(), m_tsdb, false);
            m_map[ts->get_key()] = ts;
        }

        if (raw_tags != nullptr)
            m_map[raw_tags] = ts;
            //m_map[ts->add_raw_tags(raw_tags)] = ts;
    }

    return ts;
}

bool
Mapping::add(DataPoint& dp)
{
    TimeSeries *ts = get_ts2(dp);
    bool success;

    if (ts != nullptr)
    {
        success = ts->add_data_point(dp);
    }
    else
    {
        success = false;
    }

    return success;
}

bool
Mapping::add_data_point(DataPoint& dp, bool forward)
{
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
}

// TODO: deprecate add_batch()
bool
Mapping::add_batch(DataPointSet& dps)
{
    TimeSeries *ts = get_ts(dps);
    bool success;

    if (ts != nullptr)
    {
        success = ts->add_batch(dps);
    }
    else
    {
        success = false;
    }

    return success;
}

void
Mapping::query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv)
{
    ReadLock guard(m_lock);

    for (auto curr = m_map.begin(); curr != m_map.end(); curr++)
    {
        TimeSeries *ts = curr->second;
        if (curr->first != ts->get_key()) continue;

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

void
Mapping::add_ts(Tsdb *tsdb, std::string& metric, std::string& keys, PageInfo *page_info)
{
    TimeSeries *ts;
    auto result = m_map.find(keys.c_str());

    if (result == m_map.end())
    {
        std::vector<std::string> tokens;
        tokenize(keys, tokens, ';');

        Tag *tags = nullptr;

        for (std::string token: tokens)
        {
            std::tuple<std::string,std::string> kv;
            tokenize(token, kv, '=');

            Tag *tag = (Tag*)MemoryManager::inst()->alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);
            tag->m_key = STRDUP(std::get<0>(kv).c_str());
            tag->m_value = STRDUP(std::get<1>(kv).c_str());
            tag->next() = tags;
            tags = tag;
        }

        ts = (TimeSeries*)MemoryManager::inst()->alloc_recyclable(RecyclableType::RT_TIME_SERIES);
        ts->init(metric.c_str(), keys.c_str(), tags, tsdb, tsdb->is_read_only());
        m_map[ts->get_key()] = ts;

    }
    else
    {
        ts = result->second;
    }

    ts->add_page_info(page_info);
}

int
Mapping::get_dp_count()
{
    int count = 0;
    //std::lock_guard<std::mutex> guard(m_lock);
    ReadLock guard(m_lock);

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        count += it->second->get_dp_count();
    }

    return count;
}

int
Mapping::get_ts_count()
{
    //std::lock_guard<std::mutex> guard(m_lock);
    ReadLock guard(m_lock);
    return m_map.size();
}

int
Mapping::get_page_count(bool ooo)
{
    int count = 0;
    ReadLock guard(m_lock);

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        count += it->second->get_page_count(ooo);
    }

    return count;
}


Tsdb::Tsdb(TimeRange& range, bool existing) :
    m_time_range(range),
    m_meta_file(Tsdb::get_file_name(range, "meta")),
    m_load_time(ts_now_sec()),
    m_partition_mgr(nullptr)
{
    ASSERT(g_tstamp_resolution_ms ? is_ms(range.get_from()) : is_sec(range.get_from()));

    m_map.rehash(16);
    m_mode = mode_of();
    m_page_mgrs.push_back(new PageManager(range, 0));
    m_partition_mgr = new PartitionManager(this, existing);

    char buff[64];
    Logger::debug("tsdb %s created (mode=%d)", range.c_str(buff, sizeof(buff)), m_mode);
}

Tsdb::~Tsdb()
{
    unload();

    if (m_partition_mgr != nullptr)
    {
        delete m_partition_mgr;
        m_partition_mgr = nullptr;
    }
/*
    if (m_meta_file != nullptr)
    {
        std::fflush(m_meta_file);
        std::fclose(m_meta_file);
        m_meta_file = nullptr;
    }

    std::lock_guard<std::mutex> guard(m_lock);

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        Mapping *mapping = it->second;
        delete mapping;
    }

    m_map.clear();
*/
}

Tsdb *
Tsdb::create(TimeRange& range, bool existing)
{
    Tsdb *tsdb = new Tsdb(range, existing);

    if (tsdb->m_mode & TSDB_MODE_READ)
    {
        tsdb->load_from_disk_no_lock();
    }
    else
    {
        // TODO: load read-only
        char buff[1024];
        Logger::trace("tsdb %s mode is: %d", tsdb->c_str(buff, sizeof(buff)), tsdb->m_mode);
    }

    if (! existing)
    {
        const std::string& defs = Config::get_str(CFG_CLUSTER_PARTITIONS);

        if (! defs.empty())
        {
            // save partition config
            std::string part_file = Tsdb::get_file_name(tsdb->m_time_range, "part");
            ASSERT(! file_exists(part_file));
            std::FILE *f = std::fopen(part_file.c_str(), "w");
            std::fprintf(f, "%s\n", defs.c_str());
            std::fclose(f);
        }
    }

    // Caller already acquired the lock
    //WriteLock guard(m_tsdb_lock);
    m_tsdbs.push_back(tsdb);
    std::sort(m_tsdbs.begin(), m_tsdbs.end(), tsdb_less());

    return tsdb;
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
        char buff[128];
        Logger::debug("mode_of: time_range=%s, now=%" PRIu64 ", mode=%x",
            m_time_range.c_str(buff, sizeof(buff)), now, mode);
    }

    return mode;
}

std::string
Tsdb::get_partition_defs() const
{
    std::string part_file = Tsdb::get_file_name(m_time_range, "part");
    if (! file_exists(part_file)) return EMPTY_STD_STRING;

    std::FILE *f = std::fopen(part_file.c_str(), "r");
    char buff[1024];
    std::string defs;
    if (std::fgets(buff, sizeof(buff), f) != nullptr)
        defs.assign(buff);
    std::fclose(f);
    return defs;
}

/*
void
Tsdb::open_meta()
{
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR);
    ASSERT(! data_dir.empty());
    std::string meta_file = Tsdb::get_file_name(m_time_range, "meta");

    int fd = open(meta_file.c_str(), O_CREAT|O_WRONLY|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

    if (fd == -1)
    {
        Logger::error("Failed to open file %s for writing: %d", meta_file.c_str(), errno);
    }
    else
    {
        m_meta_file = fdopen(fd, "a");

        if (m_meta_file == nullptr)
        {
            Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
        }
    }
}
*/

void
Tsdb::get_range(Timestamp tstamp, TimeRange& range)
{
    Timestamp start = (tstamp / tsdb_rotation_freq) * tsdb_rotation_freq;
    range.init(start, start + tsdb_rotation_freq);
}

Mapping *
Tsdb::get_or_add_mapping(TagOwner& dp)
{
    // cache per thread; may need a way (config) to disable it?
    //thread_local static std::map<const char*,Mapping*,cstr_less> cache;
    //thread_local static std::unordered_map<const char*,Mapping*,hash_func,eq_func> cache;
    //thread_local static tsl::robin_map<const char*, Mapping*, hash_func, eq_func> cache;

    const char *metric = dp.get_tag_value(METRIC_TAG_NAME);

    if (metric == nullptr)
    {
        Logger::warn("dp without metric");
        return nullptr;
    }

    auto result = thread_local_cache.find(metric);
    if (result != thread_local_cache.end())
    {
        Mapping *m = result->second;

        if (m->m_tsdb == this)
        {
            return m;
        }
    }

    Mapping *mapping;

    {
        std::lock_guard<std::mutex> guard(m_lock);

        auto result1 = m_map.find(metric);

        // TODO: this is not thread-safe!
        if ((result1 == m_map.end()) && ((m_mode & TSDB_MODE_READ) == 0))
        {
            ensure_readable();
            result1 = m_map.find(metric);
        }

        if (result1 == m_map.end())
        {
            mapping =
                (Mapping*)MemoryManager::alloc_recyclable(RecyclableType::RT_MAPPING);
            mapping->init(metric, this);
            m_map[mapping->m_metric] = mapping;
        }
        else
        {
            mapping = result1->second;
        }
    }

    thread_local_cache[mapping->m_metric] = mapping;

    return mapping;
}

Mapping *
Tsdb::get_or_add_mapping2(DataPoint& dp)
{
    // cache per thread; may need a way (config) to disable it?
    //thread_local static std::map<const char*,Mapping*,cstr_less> cache;
    //thread_local static std::unordered_map<const char*,Mapping*,hash_func,eq_func> cache;
    //thread_local static tsl::robin_map<const char*, Mapping*, hash_func, eq_func> cache;

    const char *metric = dp.get_metric();
    ASSERT(metric != nullptr);

    auto result = thread_local_cache.find(metric);
    if (result != thread_local_cache.end())
    {
        Mapping *m = result->second;

        if (m->m_tsdb == this)
        {
            return m;
        }
    }

    Mapping *mapping;

    {
        std::lock_guard<std::mutex> guard(m_lock);

        auto result1 = m_map.find(metric);

        // TODO: this is not thread-safe!
        if ((result1 == m_map.end()) && ((m_mode & TSDB_MODE_READ) == 0))
        {
            ensure_readable();
            result1 = m_map.find(metric);
        }

        if (result1 == m_map.end())
        {
            mapping =
                (Mapping*)MemoryManager::alloc_recyclable(RecyclableType::RT_MAPPING);
            mapping->init(metric, this);
            m_map[mapping->m_metric] = mapping;
        }
        else
        {
            mapping = result1->second;
        }
    }

    thread_local_cache[mapping->m_metric] = mapping;

    return mapping;
}

bool
Tsdb::add(DataPoint& dp)
{
    ASSERT(m_time_range.in_range(dp.get_timestamp()));

    Mapping *mapping = get_or_add_mapping2(dp);
    bool success;

    if (mapping != nullptr)
    {
        success = mapping->add(dp);
        //m_load_time = ts_now_sec();
    }
    else
    {
        success = false;
    }

    return success;
}

bool
Tsdb::add_batch(DataPointSet& dps)
{
/*
    if ((m_mode & TSDB_MODE_WRITE) == 0)
    {
        char buff[1024];
        Logger::warn("out of order dps dropped: %s", dps.c_str(buff, sizeof(buff)));
        return;
    }
*/
    ASSERT(! m_page_mgrs.empty());

    Mapping *mapping = get_or_add_mapping(dps);
    bool success;

    if (mapping != nullptr)
    {
        success = mapping->add_batch(dps);
        //m_load_time = ts_now_sec();
    }
    else
    {
        success = false;
    }

    return success;
}

bool
Tsdb::add_data_point(DataPoint& dp, bool forward)
{
    ASSERT(m_time_range.in_range(dp.get_timestamp()));

    Mapping *mapping = get_or_add_mapping2(dp);
    bool success;

    if (mapping != nullptr)
        success = mapping->add_data_point(dp, forward);
    else
        success = false;

    return success;
}

void
Tsdb::query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts)
{
    Mapping *mapping = nullptr;

    {
        std::lock_guard<std::mutex> guard(m_lock);

        ASSERT((m_mode & TSDB_MODE_READ) != 0);
/*
        if ((m_mode & TSDB_MODE_READ) == 0)
        {
            m_mode |= TSDB_MODE_READ;
            load_from_disk_no_lock();
        }
*/

        auto result = m_map.find(metric);
        if (result != m_map.end())
        {
            mapping = result->second;
        }
    }

    if (mapping != nullptr)
    {
        mapping->query_for_ts(tags, ts);
    }
}

// prepare this Tsdb for query (AND writes too!)
void
Tsdb::ensure_readable()
{
    bool readable = true;

    {
        ReadLock guard(m_load_lock);

        if ((m_mode & TSDB_MODE_READ) == 0)
            readable = false;
        else
            m_load_time = ts_now_sec();
    }

    if (! readable)
    {
        WriteLock guard(m_load_lock);

        if ((m_mode & TSDB_MODE_READ) == 0)
        {
            m_mode |= TSDB_MODE_READ;
            load_from_disk_no_lock();
        }
    }
}

// This will make tsdb read-only!
void
Tsdb::flush(bool sync)
{
    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        Mapping *mapping = it->second;
        mapping->flush();
    }

    m_meta_file.flush();

    std::lock_guard<std::mutex> guard(m_pm_lock);

    for (PageManager *pm: m_page_mgrs)
    {
        pm->flush(sync);
    }

    m_mode &= ~TSDB_MODE_WRITE;
}

void
Tsdb::set_check_point()
{
    for (auto it = m_map.begin(); (it != m_map.end()) && (!g_shutdown_requested); it++)
    {
        Mapping *mapping = it->second;
        mapping->set_check_point();
    }

    m_meta_file.flush();

    std::lock_guard<std::mutex> guard(m_pm_lock);

    for (PageManager *pm: m_page_mgrs)
    {
        if (g_shutdown_requested) break;
        pm->persist();
    }
}

//bool
//Tsdb::is_mmapped(PageInfo *page_info) const
//{
    //return (m_page_mgr.is_mmapped(page_info));
//}

void
Tsdb::shutdown()
{
    //std::lock_guard<std::mutex> guard(m_tsdb_lock);
    WriteLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        std::lock_guard<std::mutex> tsdb_guard(tsdb->m_lock);

        if (! tsdb->is_read_only())
        {
            tsdb->flush(true);
        }

        delete tsdb;
    }

    m_tsdbs.clear();
}

PageManager *
Tsdb::create_page_manager(int id)
{
    if (id < 0)
        id = m_page_mgrs.empty() ? 0 : m_page_mgrs.back()->get_id() + 1;
    PageManager *pm = new PageManager(m_time_range, id);
    ASSERT(pm != nullptr);
    ASSERT(m_page_mgrs.empty() || (m_page_mgrs.back()->get_id() < pm->get_id()));
    m_page_mgrs.push_back(pm);
    return pm;
}

PageInfo *
Tsdb::get_free_page_on_disk(bool out_of_order)
{
    ASSERT(! m_page_mgrs.empty());
    std::lock_guard<std::mutex> guard(m_pm_lock);
    PageInfo *pi = m_page_mgrs.back()->get_free_page_on_disk(this, out_of_order);

    if (pi == nullptr)
    {
        // We need a new mmapp'ed file!
        PageManager *pm = create_page_manager();
        ASSERT(m_time_range.contains(pm->get_time_range()));
        ASSERT(pm->get_time_range().contains(m_time_range));
        pi = pm->get_free_page_on_disk(this, out_of_order);
    }

    ASSERT(pi != nullptr);
    return pi;
}

PageInfo *
Tsdb::get_free_page_for_compaction()
{
    PageInfo *info = nullptr;

    if (! m_temp_page_mgrs.empty())
        info = m_temp_page_mgrs.back()->get_free_page_for_compaction(this);

    if (info == nullptr)
    {
        int id = m_temp_page_mgrs.empty() ? 0 : m_temp_page_mgrs.back()->get_id() + 1;
        PageManager *pm = new PageManager(m_time_range, id, true);
        m_temp_page_mgrs.push_back(pm);
        info = pm->get_free_page_for_compaction(this);
    }

    return info;
}

PageInfo *
Tsdb::get_the_page_on_disk(PageCount id, PageCount header_index)
{
    PageManager *pm = get_page_manager(id);

    if (pm == nullptr)
        pm = create_page_manager(id);

    ASSERT(pm->get_id() == id);
    PageInfo *pi = pm->get_the_page_on_disk(header_index);

    ASSERT(pi != nullptr);
    return pi;
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
            if (tsdb->in_range(tstamp)) return tsdb;
        }
    }

    return nullptr;
}

Tsdb *
Tsdb::inst(Timestamp tstamp)
{
    Tsdb *tsdb = nullptr;

    {
        ReadLock guard(m_tsdb_lock);
        tsdb = Tsdb::search(tstamp);
    }

    if (tsdb != nullptr) return tsdb;

    // create one
    {
        WriteLock guard(m_tsdb_lock);
        tsdb = Tsdb::search(tstamp);  // search again to avoid race condition
        if (tsdb == nullptr)
        {
            TimeRange range;
            Tsdb::get_range(tstamp, range);
            tsdb = Tsdb::create(range, false);  // create new
        }
    }

    return tsdb;
}

void
Tsdb::load_from_disk()
{
    std::lock_guard<std::mutex> guard(m_lock);
    load_from_disk_no_lock();
}

void
Tsdb::load_from_disk_no_lock()
{
    Meter meter(METRIC_TICKTOCK_TSDB_LOAD_TOTAL_MS);

    if (m_meta_file.is_open())
        return; // already loaded

    for (PageManager *pm: m_page_mgrs)
    {
        if (! pm->is_open())
        {
            pm->reopen();
            ASSERT(pm->is_open());
        }
    }

    for (PageCount id = m_page_mgrs.size(); ; id++)
    {
        std::string file_name = get_file_name(m_time_range, std::to_string(id));
        if (! file_exists(file_name)) break;
        PageManager *pm = new PageManager(m_time_range, id);
        pm->reopen();
        ASSERT(pm->is_open());
        m_page_mgrs.push_back(pm);
    }

    // check/set compacted flag
    bool compacted = true;
    for (PageManager *pm: m_page_mgrs)
    {
        if (! pm->is_compacted())
        {
            compacted = false;
            break;
        }
    }

    m_mode |= TSDB_MODE_READ;
    if (compacted) m_mode |= TSDB_MODE_COMPACTED;

    m_meta_file.load(this);
    m_meta_file.open(); // open for append

    m_load_time = ts_now_sec();
}

void
Tsdb::insts(const TimeRange& range, std::vector<Tsdb*>& tsdbs)
{
    //std::lock_guard<std::mutex> guard(m_tsdb_lock);
    ReadLock guard(m_tsdb_lock);

    for (int i = 0; i < m_tsdbs.size(); i++)
    {
        Tsdb *tsdb = m_tsdbs[i];

        // TODO: make sure 'tsdb' and 'range' are both using the same time unit

        if (tsdb->in_range(range))
        {
            tsdbs.push_back(tsdb);
        }
        else
        {
            char buff1[64], buff2[64];
            Logger::debug("%s has no intersection with %s",
                tsdb->c_str(buff1, sizeof(buff1)), range.c_str(buff2, sizeof(buff2)));
        }
    }
}

bool
Tsdb::http_api_put_handler(HttpRequest& request, HttpResponse& response)
{
    Tsdb* tsdb = nullptr;
    char *curr = request.content;
    bool success = true;

    AppendLog::inst()->append(request.content, request.length);

    ReadLock *guard = nullptr;

    while ((curr != nullptr) && (*curr != 0))
    {
        DataPoint dp;

        if (*curr == ';') curr++;
        curr = dp.from_http(curr);

        if ((tsdb == nullptr) || !(tsdb->in_range(dp.get_timestamp())))
        {
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                tsdb->load_from_disk();
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
            }
        }

        success = tsdb->add(dp) && success;
    }

    if (guard != nullptr) delete guard;
    response.status_code = 200;
    response.content_length = 0;

    return success;
}

bool
Tsdb::http_api_put_handler_json(HttpRequest& request, HttpResponse& response)
{
    Tsdb* tsdb = nullptr;
    char *curr = strchr(request.content, '[');
    bool success = true;

    AppendLog::inst()->append(request.content, request.length);

    ReadLock *guard = nullptr;

    while ((curr != nullptr) && (*curr != ']') && (*curr != 0))
    {
        DataPoint dp;
        curr = dp.from_json(curr+1);

        //char buff[1024];
        //Logger::info("dp: %s", dp.c_str(buff, sizeof(buff)));

        if ((tsdb == nullptr) || !(tsdb->in_range(dp.get_timestamp())))
        {
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                tsdb->load_from_disk();
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
            }
        }

        success = tsdb->add(dp) && success;

        if (curr != nullptr)
        {
            while (isspace(*curr)) curr++;
        }
    }

    if (guard != nullptr) delete guard;
    response.status_code = (success ? 200 : 400);
    response.content_length = 0;

    return success;
}

bool
Tsdb::http_api_put_handler_plain(HttpRequest& request, HttpResponse& response)
{
    Logger::trace("Entered http_api_put_handler_plain()...");

    Tsdb* tsdb = nullptr;
    char *curr = request.content;
    bool forward = request.forward;
    bool success = true;

    // is the the 'version' command?
    if ((request.length == 8) && (strncmp(curr, "version\n", 8) == 0))
    {
        return Stats::http_get_api_version_handler(request, response);
    }
    else if ((request.length == 6) && (strncmp(curr, "stats\n", 6) == 0))
    {
        return Stats::http_get_api_stats_handler(request, response);
    }

    response.content_length = 0;
    AppendLog::inst()->append(request.content, request.length);

    // safety measure
    curr[request.length] = ' ';
    curr[request.length+1] = '\n';
    curr[request.length+2] = '0';

    ReadLock *guard = nullptr;

    while ((curr != nullptr) && (*curr != 0))
    {
        DataPoint dp;

        if (std::isspace(*curr)) break;
        if (std::strncmp(curr, "put ", 4) != 0)
        {
            if (std::strncmp(curr, "version\n", 8) == 0)
            {
                curr += 8;
                Stats::http_get_api_version_handler(request, response);
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

        if ((tsdb == nullptr) || !(tsdb->in_range(dp.get_timestamp())))
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
                tsdb->load_from_disk();
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
            }
        }

        ASSERT(tsdb != nullptr);

        //if (forward)
            //success = tsdb->add_data_point(dp) && success;
        //else
            //success = tsdb->add(dp) && success;
        success = tsdb->add_data_point(dp, forward) && success;
    }

    if (forward && (tsdb != nullptr))
        success = tsdb->submit_data_points() && success; // flush
    if (guard != nullptr) delete guard;
    response.status_code = (success ? 200 : 400);

    return success;
}

bool
Tsdb::http_get_api_suggest_handler(HttpRequest& request, HttpResponse& response)
{
    size_t buff_size = MemoryManager::get_network_buffer_size() - 6;
    char* buff = MemoryManager::alloc_network_buffer();

    JsonMap params;
    request.parse_params(params);

    auto search = params.find("type");
    if (search == params.end()) return false;
    const char *type = search->second->to_string();

    search = params.find("q");
    if (search == params.end()) return false;
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
        ReadLock guard(m_tsdb_lock);

        for (Tsdb *tsdb: m_tsdbs)
        {
            std::lock_guard<std::mutex> tsdb_guard(tsdb->m_lock);

            if ((tsdb->m_mode & TSDB_MODE_READ) == 0) continue;

            for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
            {
                const char *metric = it->first;

                if (starts_with(metric, prefix))
                {
                    suggestions.insert(std::string(metric));
                    if (suggestions.size() >= max) break;
                }
            }

            if (suggestions.size() >= max) break;
        }
    }
    else if (std::strcmp(type, "tagk") == 0)
    {
        ReadLock guard(m_tsdb_lock);

        for (Tsdb *tsdb: m_tsdbs)
        {
            std::lock_guard<std::mutex> tsdb_guard(tsdb->m_lock);

            if ((tsdb->m_mode & TSDB_MODE_READ) == 0) continue;

            for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
            {
                Mapping *mapping = it->second;

                ReadLock mapping_guard(mapping->m_lock);

                for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
                {
                    TimeSeries *ts = it2->second;
                    ts->get_keys(suggestions);
                }
            }

            if (suggestions.size() >= max) break;
        }
    }
    else if (std::strcmp(type, "tagv") == 0)
    {
        ReadLock guard(m_tsdb_lock);

        for (Tsdb *tsdb: m_tsdbs)
        {
            std::lock_guard<std::mutex> tsdb_guard(tsdb->m_lock);

            if ((tsdb->m_mode & TSDB_MODE_READ) == 0) continue;

            for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
            {
                Mapping *mapping = it->second;

                ReadLock mapping_guard(mapping->m_lock);

                for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
                {
                    TimeSeries *ts = it2->second;
                    ts->get_values(suggestions);
                }
            }

            if (suggestions.size() >= max) break;
        }
    }
    else
    {
        Logger::warn("Unrecognized suggest type: %s", type);
        return false;
    }

    int n = JsonParser::to_json(suggestions, buff, buff_size);
    response.init(200, HttpContentType::JSON, n, buff);

    return true;
}

void
Tsdb::append_meta_all()
{
    for (const auto& t: m_map)
    {
        Mapping *mapping = t.second;

        for (const auto& m: mapping->m_map)
        {
            TimeSeries *ts = m.second;
            ts->append_meta_all(m_meta_file);
        }
    }
}

void
Tsdb::init()
{
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR);
    DIR *dir;
    struct dirent *dir_ent;

    PartitionManager::init();

    tsdb_rotation_freq =
        validate_resolution(Config::get_time(CFG_TSDB_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_TSDB_ROTATION_FREQUENCY_DEF));
    if (tsdb_rotation_freq < 1) tsdb_rotation_freq = 1;

    // check if we have enough disk space
    PageCount page_count =
        Config::get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    uint64_t avail = get_disk_available_blocks(data_dir);

    if (avail <= page_count)
    {
        Logger::error("Not enough disk space at %s (%ld <= %ld)",
            data_dir.c_str(), avail, page_count);
    }
    else if (avail <= (2 * page_count))
    {
        Logger::warn("Low disk space at %s", data_dir.c_str());
    }

    compact2();

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
            Timestamp start = atol(tokens[0].c_str());
            Timestamp end = atol(tokens[1].c_str());

            if (g_tstamp_resolution_ms)
            {
                start = to_ms(start);
                end = to_ms(end);
            }

            TimeRange range(start, end);
            Tsdb *tsdb = Tsdb::create(range, true); // create existing
            ASSERT(tsdb != nullptr);
            Logger::trace("loaded tsdb with %d Mappings", tsdb->m_map.size());
            //m_tsdbs.push_back(tsdb);
        }

        closedir(dir);

        std::sort(m_tsdbs.begin(), m_tsdbs.end(), tsdb_less());
    }

    Task task;
    task.doit = &Tsdb::rotate;
    int freq_sec = Config::get_time(CFG_TSDB_FLUSH_FREQUENCY, TimeUnit::SEC, CFG_TSDB_FLUSH_FREQUENCY_DEF);
    if (freq_sec < 1) freq_sec = 1;
    Timer::inst()->add_task(task, freq_sec, "tsdb_flush");
    Logger::info("Will try to rotate tsdb every %d secs.", freq_sec);

    task.doit = &Tsdb::compact;
    task.data.integer = 0;  // indicates this is from scheduled task (vs. interactive cmd)
    freq_sec = Config::get_time(CFG_TSDB_COMPACT_FREQUENCY, TimeUnit::SEC, CFG_TSDB_COMPACT_FREQUENCY_DEF);
    if (freq_sec < 1) freq_sec = 1;
    Timer::inst()->add_task(task, freq_sec, "tsdb_compact");
    Logger::info("Will try to compact tsdb every %d secs.", freq_sec);
}

std::string
Tsdb::get_file_name(const TimeRange& range, std::string ext, bool temp)
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/%" PRIu64 ".%" PRIu64 ".%s%s",
        Config::get_str(CFG_TSDB_DATA_DIR).c_str(),
        range.get_from_sec(),
        range.get_to_sec(),
        ext.c_str(),
        temp ? ".temp" : "");
    return std::string(buff);
}

void
Tsdb::add_ts(std::string& metric, std::string& key, PageCount file_id, PageCount header_index)
{
    Mapping *mapping;

    auto search = m_map.find(metric.c_str());

    if (search != m_map.end())
    {
        // found
        mapping = search->second;
    }
    else
    {
        mapping =
            (Mapping*)MemoryManager::alloc_recyclable(RecyclableType::RT_MAPPING);
        mapping->init(metric.c_str(), this);
        m_map[mapping->m_metric] = mapping;
    }

    PageInfo *info = get_the_page_on_disk(file_id, header_index);

    if (info->is_empty())
        MemoryManager::free_recyclable(info);
    else
        mapping->add_ts(this, metric, key, info);
}


int
Tsdb::get_metrics_count()
{
    int count = 0;
    //std::lock_guard<std::mutex> guard(m_tsdb_lock);
    ReadLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        count += tsdb->m_map.size();
    }

    return count;
}

int
Tsdb::get_dp_count()
{
    int count = 0;
    //std::lock_guard<std::mutex> guard(m_tsdb_lock);
    ReadLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        std::lock_guard<std::mutex> guard(tsdb->m_lock);

        //if (! tsdb->m_loaded) continue;
        if ((tsdb->m_mode & TSDB_MODE_READ) == 0) continue;

        for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
        {
            count += it->second->get_dp_count();
        }
    }

    return count;
}

int
Tsdb::get_ts_count()
{
    int count = 0;
    Tsdb *tsdb = nullptr;

    {
        //std::lock_guard<std::mutex> guard(m_tsdb_lock);
        ReadLock guard(m_tsdb_lock);
        if (! m_tsdbs.empty()) tsdb = m_tsdbs.back();
    }

    if (tsdb != nullptr)
    {
        std::lock_guard<std::mutex> guard(tsdb->m_lock);

        //if (! tsdb->m_loaded) return count;
        if ((tsdb->m_mode & TSDB_MODE_READ) == 0) return count;

        for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
        {
            count += it->second->get_ts_count();
        }
    }

    return count;
}

int
Tsdb::get_page_count(bool ooo)
{
    int count = 0;
    ReadLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        std::lock_guard<std::mutex> guard(tsdb->m_lock);

        for (auto it = tsdb->m_map.begin(); it != tsdb->m_map.end(); it++)
        {
            count += it->second->get_page_count(ooo);
        }
    }

    return count;
}

int
Tsdb::get_data_page_count()
{
    int count = 0;
    ReadLock guard(m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        std::lock_guard<std::mutex> guard1(tsdb->m_lock);
        std::lock_guard<std::mutex> guard2(tsdb->m_pm_lock);

        for (auto pm: tsdb->m_page_mgrs)
        {
            count += pm->get_data_page_count();
        }
    }

    return count;
}

double
Tsdb::get_page_percent_used()
{
    if ((m_mode & TSDB_MODE_READ) == 0) return 0.0;

    std::lock_guard<std::mutex> guard(m_pm_lock);

    if (m_page_mgrs.empty()) return 0.0;

    double pct_used = 0.0;

    for (const PageManager *pm: m_page_mgrs)
    {
        pct_used += pm->get_page_percent_used();
    }

    return pct_used / m_page_mgrs.size();
}

// This will archive the tsdb. No write nor read will be possible afterwards.
void
Tsdb::unload()
{
    WriteLock unload_guard(m_load_lock);

    m_meta_file.close();

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        Mapping *mapping = it->second;
        mapping->unload(true);
    }

    m_map.clear();

    std::lock_guard<std::mutex> guard(m_pm_lock);

    for (PageManager *pm: m_page_mgrs)
    {
        delete pm;
    }

    m_page_mgrs.clear();

    m_mode &= ~TSDB_MODE_READ_WRITE;
}

bool
Tsdb::rotate(TaskData& data)
{
    if (g_shutdown_requested) return false;

    Meter meter(METRIC_TICKTOCK_TSDB_ROTATE_MS);
    Timestamp now = ts_now();
    std::vector<Tsdb*> tsdbs;

    TimeRange range(0, now);
    Tsdb::insts(range, tsdbs);

    Logger::info("[rotate] Checking %d tsdbs.", tsdbs.size());

    for (Tsdb *tsdb: tsdbs)
    {
        if (g_shutdown_requested) break;

        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        char buff[128];  // for logging

        if (! (tsdb->m_mode & TSDB_MODE_READ))
        {
            Logger::info("[rotate] Tsdb %s already archived!", tsdb->c_str(buff, sizeof(buff)));
            continue;    // already archived
        }

        uint32_t mode = tsdb->mode_of();

        if (! (mode & TSDB_MODE_READ))
        {
            long load_time = tsdb->m_load_time;
            long now_sec = to_sec(now);

            // archive it
            if ((now_sec - load_time) > 7200)   // TODO: config?
            {
                Logger::info("[rotate] Archiving tsdb (lt=%ld, now=%ld): %s", load_time, now_sec, tsdb->c_str(buff, sizeof(buff)));
                tsdb->flush(true);
                tsdb->unload();
            }
            else
            {
                Logger::info("[rotate] Archiving tsdb %s SKIPPED to avoid thrashing", tsdb->c_str(buff, sizeof(buff)));
                tsdb->m_meta_file.flush();
            }
        }
        else if ((!(mode & TSDB_MODE_WRITE)) && (tsdb->m_mode & TSDB_MODE_WRITE))
        {
            // make it read-only
            Logger::info("[rotate] Flushing tsdb: %s", tsdb->c_str(buff, sizeof(buff)));
            tsdb->flush(true);
        }
        else
        {
            Logger::debug("[rotate] Active tsdb: %s, mode = %d, tsdb->mode = %d",
                tsdb->c_str(buff, sizeof(buff)), mode, tsdb->m_mode);
            // TODO: do this only if there were writes since last check point
            tsdb->set_check_point();
        }
    }

    if (Config::exists(CFG_TSDB_RETENTION_THRESHOLD))
    {
        purge_oldest(Config::get_int(CFG_TSDB_RETENTION_THRESHOLD));
    }

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

            if ((now - tsdb->m_load_time) > 7200)   // TODO: config?
            {
                m_tsdbs.erase(m_tsdbs.begin());
            }
            else
            {
                tsdb = nullptr;
            }
        }
    }

    if (tsdb != nullptr)
    {
        char buff[64];
        Logger::info("[rotate] Purging %s permenantly", tsdb->c_str(buff,sizeof(buff)));

        std::lock_guard<std::mutex> guard(tsdb->m_lock);

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
}

bool
Tsdb::compact(TaskData& data)
{
    Tsdb *tsdb = nullptr;
    WriteLock *write_guard = nullptr;
    Meter meter(METRIC_TICKTOCK_TSDB_COMPACT_MS);

    Logger::info("[COMPACTION] Finding tsdbs to compact...");

    // called from scheduled task? if so, enforce off-hour rule;
    if ((data.integer == 0) && ! is_off_hour()) return false;

    // Go through all the Tsdbs, from the oldest to the newest,
    // to find the first uncompacted Tsdb to compact.
    {
        ReadLock guard(m_tsdb_lock);

        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); it++)
        {
            std::lock_guard<std::mutex> guard((*it)->m_lock);

            // also make sure it's not readable nor writable while we are compacting
            if ((*it)->m_mode & (TSDB_MODE_COMPACTED | TSDB_MODE_READ_WRITE))
                continue;

            ASSERT(! (*it)->m_meta_file.is_open());
            (*it)->load_from_disk_no_lock();    // load from disk to see if it's already compacted

            if ((*it)->m_mode & TSDB_MODE_COMPACTED)
            {
                (*it)->unload();
            }
            else    // not compacted yet
            {
                tsdb = *it;
                write_guard = new WriteLock(tsdb->m_load_lock);
                //m_tsdbs.erase(it);
                break;
            }
        }
    }

    if (tsdb != nullptr)
    {
        char buff[1024];
        Logger::info("[COMPACTION] Found this tsdb to compact: %s", tsdb->c_str(buff, sizeof(buff)));
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        TimeRange range = tsdb->get_time_range();
        MetaFile meta_file(get_file_name(range, "meta", true));

        ASSERT(tsdb->m_temp_page_mgrs.empty());

        // perform compaction
        try
        {
            // create a temporary data file to compact data into

            // cleanup existing temporary files, if any
            std::string temp_files = get_file_name(tsdb->m_time_range, "*", true);
            rm_all_files(temp_files);

            meta_file.open();
            ASSERT(meta_file.is_open());

            for (const auto& t: tsdb->m_map)
            {
                Mapping *mapping = t.second;

                WriteLock guard(mapping->m_lock);

                for (const auto& m: mapping->m_map)
                {
                    TimeSeries *ts = m.second;

                    if (std::strcmp(ts->get_key(), m.first) == 0)
                        ts->compact(meta_file);
                }
            }

            tsdb->unload();
            Logger::info("1 Tsdb compacted");
        }
        catch (const std::exception& ex)
        {
            Logger::error("compaction failed: %s", ex.what());
        }
        catch (...)
        {
            Logger::error("compaction failed for unknown reasons");
        }

        // mark it as compacted
        tsdb->m_mode |= TSDB_MODE_COMPACTED;
        meta_file.close();

        for (auto mgr: tsdb->m_temp_page_mgrs)
        {
            mgr->shrink_to_fit();
            ASSERT(mgr->is_compacted());
            delete mgr;
        }

        tsdb->m_temp_page_mgrs.clear();
        tsdb->m_temp_page_mgrs.shrink_to_fit();

        try
        {
            // create a file to indicate compaction was successful
            std::string done_name = get_file_name(range, "done", true);
            std::ofstream done_file(done_name);
            done_file.flush();
            done_file.close();
            compact2();
        }
        catch (const std::exception& ex)
        {
            Logger::error("compaction failed: %s", ex.what());
        }
    }
    else
    {
        Logger::info("[COMPACTION] Did not find any appropriate Tsdb to compact.");
    }

    if (write_guard != nullptr) delete write_guard;
    return false;
}

void
Tsdb::compact2()
{
    glob_t result;
    std::string pattern = Config::get_str(CFG_TSDB_DATA_DIR);

    pattern.append("/*.*.done.temp");
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);

    std::vector<std::string> done_files;

    for (unsigned int i = 0; i < result.gl_pathc; i++)
    {
        done_files.push_back(std::string(result.gl_pathv[i]));
    }

    globfree(&result);

    for (auto& done_file: done_files)
    {
        Logger::info("Found %s done compactions", done_file.c_str());

        // format: <data-directory>/1614009600.1614038400.done.temp
        ASSERT(ends_with(done_file, ".done.temp"));
        std::string base = done_file.substr(0, done_file.size()-9);

        if (! file_exists(base + "meta.temp"))
        {
            Logger::error("Compaction failed, file %smeta.temp missing!", base.c_str());
            continue;
        }

        // finish compaction data files
        for (int i = 0; ; i++)
        {
            std::string data_file = base + std::to_string(i);
            std::string temp_file = data_file + ".temp";

            bool data_file_exists = file_exists(data_file);
            bool temp_file_exists = file_exists(temp_file);

            if (! data_file_exists && ! temp_file_exists)
                break;

            if (data_file_exists) rm_file(data_file);
            if (temp_file_exists) std::rename(temp_file.c_str(), data_file.c_str());
        }

        // finish compaction meta file
        std::string meta_file = base + "meta";
        std::string temp_file = meta_file + ".temp";

        ASSERT(file_exists(meta_file));
        ASSERT(file_exists(temp_file));
        rm_file(meta_file);
        std::rename(temp_file.c_str(), meta_file.c_str());

        // remove done file
        rm_file(done_file);
    }

    std::string temp_files = Config::get_str(CFG_TSDB_DATA_DIR);
    temp_files.append("/*.temp");
    rm_all_files(temp_files);
}

const char *
Tsdb::c_str(char *buff, size_t size) const
{
    if ((buff == nullptr) || (size < 5)) return EMPTY_STRING;
    strcpy(buff, "tsdb");
    m_time_range.c_str(&buff[4], size-4);
    return buff;
}


}
