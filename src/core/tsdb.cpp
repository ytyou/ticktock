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
#include "admin.h"
#include "append.h"
#include "config.h"
#include "cp.h"
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
static uint64_t tsdb_rotation_freq = 0;
//static thread_local std::unordered_map<const char*, Mapping*, hash_func, eq_func> thread_local_cache;
static thread_local tsl::robin_map<const char*, Mapping*, hash_func, eq_func> thread_local_cache;


Mapping::Mapping() :
    m_metric(nullptr),
    m_tsdb(nullptr),
    m_partition(nullptr),
    m_ref_count(0)
{
}

void
Mapping::init(const char *name, Tsdb *tsdb)
{
    if (m_metric != nullptr)
        FREE(m_metric);

    m_metric = STRDUP(name);
    ASSERT(m_metric != nullptr);
    m_tsdb = tsdb;
    m_partition = tsdb->get_partition(name);
    m_ref_count = 1;

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

    unload_no_lock();
}

void
Mapping::unload()
{
    WriteLock guard(m_lock);
    unload_no_lock();
}

void
Mapping::unload_no_lock()
{
    // More than one key could be mapped to the same
    // TimeSeries; so we need to dedup first to avoid
    // double free it.
    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        const char *key = it->first;
        TimeSeries *ts = it->second;

        if (ts->get_key() == key)
            MemoryManager::free_recyclable(ts);
        else
            FREE((void*)key);
    }

    m_map.clear();
    m_tsdb = nullptr;
}

void
Mapping::flush()
{
    ReadLock guard(m_lock);

    if (m_tsdb == nullptr) return;

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        TimeSeries *ts = it->second;

        if (it->first == ts->get_key())
        {
            Logger::trace("Flushing ts: %T", ts);
            ts->flush(true);
        }
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

    ASSERT(m_map.size() == 0);
    m_map.clear();
    m_tsdb = nullptr;

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
            ts = (TimeSeries*)MemoryManager::alloc_recyclable(RecyclableType::RT_TIME_SERIES);
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
            ts = (TimeSeries*)MemoryManager::alloc_recyclable(RecyclableType::RT_TIME_SERIES);
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

            Tag *tag = (Tag*)MemoryManager::alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);
            tag->m_key = STRDUP(std::get<0>(kv).c_str());
            tag->m_value = STRDUP(std::get<1>(kv).c_str());
            tag->next() = tags;
            tags = tag;
        }

        ts = (TimeSeries*)MemoryManager::alloc_recyclable(RecyclableType::RT_TIME_SERIES);
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
    if (m_mode & TSDB_MODE_READ_WRITE)
        m_page_mgrs.push_back(new PageManager(range, 0));
    m_partition_mgr = new PartitionManager(this, existing);

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

    if (tsdb->m_mode & TSDB_MODE_READ_WRITE)
    {
        tsdb->load_from_disk_no_lock();
    }
    else
    {
        // TODO: load read-only
        Logger::trace("tsdb %T mode is: %d", tsdb, tsdb->m_mode);
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
        Logger::debug("mode_of: time_range=%T, now=%" PRIu64 ", mode=%x",
            &m_time_range, now, mode);
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
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);
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

    ASSERT(mapping->m_metric != nullptr);
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
        ASSERT(m->m_metric != nullptr);
        ASSERT(std::strcmp(metric, m->m_metric) == 0);
        if (m->m_tsdb == this) return m;
        thread_local_cache.erase(m->m_metric);
        ASSERT(thread_local_cache.find(m->m_metric) == thread_local_cache.end());
        m->dec_ref_count();
    }

    Mapping *mapping;

    {
        std::lock_guard<std::mutex> guard(m_lock);
        auto result1 = m_map.find(metric);

/*
        if ((result1 == m_map.end()) && ((m_mode & TSDB_MODE_READ) == 0))
        {
            //ensure_readable();
            result1 = m_map.find(metric);
        }
*/

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
            ASSERT(mapping->m_ref_count >= 1);
        }

        mapping->inc_ref_count();
        ASSERT(mapping->m_tsdb == this);
        ASSERT(mapping->m_ref_count >= 2);
    }

    ASSERT(mapping->m_tsdb == this);
    ASSERT(mapping->m_metric != nullptr);
    ASSERT(strcmp(mapping->m_metric, metric) == 0);
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
            ASSERT(result->first == mapping->m_metric);
        }
    }

    if (mapping != nullptr)
    {
        mapping->query_for_ts(tags, ts);
    }
}

// prepare this Tsdb for query (AND writes too!)
void
Tsdb::ensure_readable(bool count)
{
    bool readable = true;

    {
        ReadLock guard(m_load_lock);

        if ((m_mode & TSDB_MODE_READ) == 0)
            readable = false;
        else
            m_load_time = ts_now_sec();

        if (count)
            inc_count();
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
        ASSERT(it->first == mapping->m_metric);
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
        ASSERT(it->first == mapping->m_metric);
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
        {
            WriteLock unload_guard(tsdb->m_load_lock);
            std::lock_guard<std::mutex> tsdb_guard(tsdb->m_lock);
            if (! tsdb->is_read_only()) tsdb->flush(true);
        }

        delete tsdb;
    }

    m_tsdbs.clear();
    CheckPointManager::close();
    Logger::info("Tsdb::shutdown complete");
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
    std::lock_guard<std::mutex> guard(m_pm_lock);
    PageManager *pm;

    if (m_page_mgrs.empty())
        pm = create_page_manager();
    else
        pm = m_page_mgrs.back();

    ASSERT(pm->is_open());
    PageInfo *pi = pm->get_free_page_on_disk(this, out_of_order);

    if (pi == nullptr)
    {
        // We need a new mmapp'ed file!
        ASSERT(pm->is_full());
        pm = create_page_manager();
        ASSERT(pm->is_open());
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
        }
    }

    return tsdb;
}

bool
Tsdb::load_from_disk()
{
    std::lock_guard<std::mutex> guard(m_lock);
    return load_from_disk_no_lock();
}

bool
Tsdb::load_from_disk_no_lock()
{
    Meter meter(METRIC_TICKTOCK_TSDB_LOAD_TOTAL_MS);

    if (m_meta_file.is_open())
        return true;    // already loaded

    bool success = true;

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

    ASSERT(m_map.empty());
    m_meta_file.load(this);
    m_meta_file.open(); // open for append

    m_mode |= TSDB_MODE_READ;
    if (compacted) m_mode |= TSDB_MODE_COMPACTED;
    m_load_time = ts_now_sec();
    return true;
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
                if (! tsdb->load_from_disk())
                {
                    success = false;
                    break;
                }
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
                ASSERT(tsdb->m_meta_file.is_open());
            }
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

    AppendLog::inst()->append(request.content, request.length);

    ReadLock *guard = nullptr;

    while ((*curr != ']') && (*curr != 0))
    {
        DataPoint dp;
        curr = dp.from_json(curr+1);
        if (curr == nullptr) break;

        //char buff[1024];
        //Logger::info("dp: %s", dp.c_str(buff, sizeof(buff)));

        if ((tsdb == nullptr) || !(tsdb->in_range(dp.get_timestamp())))
        {
            if (guard != nullptr) delete guard;
            tsdb = Tsdb::inst(dp.get_timestamp());
            guard = new ReadLock(tsdb->m_load_lock);    // prevent from unloading
            if (! (tsdb->m_mode & TSDB_MODE_READ))
            {
                if (! tsdb->load_from_disk())
                {
                    success = false;
                    break;
                }
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
            }
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
                if (! tsdb->load_from_disk())
                {
                    success = false;
                    break;
                }
                tsdb->m_mode |= TSDB_MODE_READ_WRITE;
            }
            else
            {
                tsdb->m_mode |= TSDB_MODE_WRITE;
                tsdb->m_load_time = ts_now_sec();
                ASSERT(tsdb->m_meta_file.is_open());
            }
        }

        ASSERT(tsdb != nullptr);
        ASSERT(tsdb->m_meta_file.is_open());

        //if (forward)
            //success = tsdb->add_data_point(dp) && success;
        //else
            //success = tsdb->add(dp) && success;
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
                ASSERT(it->first == mapping->m_metric);

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
                ASSERT(it->first == mapping->m_metric);

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
Tsdb::append_meta_all()
{
    for (const auto& t: m_map)
    {
        Mapping *mapping = t.second;
        ASSERT(t.first == mapping->m_metric);

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
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);
    DIR *dir;
    struct dirent *dir_ent;

    Logger::info("Loading data from %s", data_dir.c_str());

    CheckPointManager::init();
    PartitionManager::init();

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
    if (freq_sec > 0)
    {
        Timer::inst()->add_task(task, freq_sec, "tsdb_compact");
        Logger::info("Will try to compact tsdb every %d secs.", freq_sec);
    }
}

std::string
Tsdb::get_file_name(const TimeRange& range, std::string ext, bool temp)
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/%" PRIu64 ".%" PRIu64 ".%s%s",
        Config::get_str(CFG_TSDB_DATA_DIR,CFG_TSDB_DATA_DIR_DEF).c_str(),
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
        ASSERT(search->first == mapping->m_metric);
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

void
Tsdb::unload()
{
    WriteLock unload_guard(m_load_lock);
    unload_no_lock();
}

// This will archive the tsdb. No write nor read will be possible afterwards.
void
Tsdb::unload_no_lock()
{
    ASSERT(m_count.load() <= 0);
    m_meta_file.close();

    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        Mapping *mapping = it->second;
        ASSERT(it->first == mapping->m_metric);
        mapping->unload();
        mapping->dec_ref_count();
    }

    m_map.clear();
    ASSERT(m_map.size() == 0);

    std::lock_guard<std::mutex> pm_guard(m_pm_lock);

    for (PageManager *pm: m_page_mgrs)
    {
        delete pm;
    }

    m_page_mgrs.clear();
    m_page_mgrs.shrink_to_fit();

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

    Logger::debug("[rotate] Checking %d tsdbs.", tsdbs.size());

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
        std::lock_guard<std::mutex> guard(tsdb->m_lock);

        if (! (tsdb->m_mode & TSDB_MODE_READ))
        {
            Logger::debug("[rotate] %T already archived!", tsdb);
            continue;    // already archived
        }

        uint32_t mode = tsdb->mode_of();
        uint64_t load_time = tsdb->m_load_time;

        if (disk_avail < 100000000L)    // TODO: get it from config
            mode &= ~TSDB_MODE_WRITE;

        if ((now_sec - load_time) > thrashing_threshold)
        {
            if (! (mode & TSDB_MODE_READ) && (tsdb->m_count <= 0))
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
        else
            Logger::debug("[rotate] %T SKIPPED to avoid thrashing (lt=%" PRIu64 ")", tsdb, load_time);

        if (tsdb->m_mode & TSDB_MODE_WRITE)
        {
            Logger::debug("[rotate] set_check_point for tsdb: %T, mode = %d, tsdb->mode = %d",
                tsdb, mode, tsdb->m_mode);
            // TODO: do this only if there were writes since last check point
            tsdb->set_check_point();
        }
    }

    CheckPointManager::persist();

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
        Logger::info("[rotate] Purging %T permenantly", tsdb);

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

            // also make sure it's not readable nor writable while we are compacting
            if ((*it)->m_mode & (TSDB_MODE_COMPACTED | TSDB_MODE_READ_WRITE))
                continue;

            ASSERT(! (*it)->m_meta_file.is_open());
            // load from disk to see if it's already compacted
            if (! (*it)->load_from_disk_no_lock()) continue;

            if ((*it)->m_mode & TSDB_MODE_COMPACTED)
            {
                (*it)->unload();
            }
            else    // not compacted yet
            {
                tsdb = *it;
                //m_tsdbs.erase(it);
                break;
            }
        }
    }

    if (tsdb != nullptr)
    {
        WriteLock load_lock(tsdb->m_load_lock);

        if (tsdb->m_mode == TSDB_MODE_READ) // make sure it's not unloaded
        {
            Logger::info("[compact] Found this tsdb to compact: %T", tsdb);
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
                    ASSERT(t.first == mapping->m_metric);

                    WriteLock guard(mapping->m_lock);

                    for (const auto& m: mapping->m_map)
                    {
                        TimeSeries *ts = m.second;

                        if (std::strcmp(ts->get_key(), m.first) == 0)
                            ts->compact(meta_file);
                    }
                }

                tsdb->unload();
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
                Logger::error("[compact] compaction failed: %s", ex.what());
            }
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

    std::string temp_files = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);
    temp_files.append("/*.temp");
    rm_all_files(temp_files);
}

const char *
Tsdb::c_str(char *buff) const
{
    strcpy(buff, "tsdb");
    m_time_range.c_str(&buff[4]);
    return buff;
}


}
