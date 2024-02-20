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
#include <chrono>
#include <algorithm>
#include <cassert>
#include <dirent.h>
#include <functional>
#include <glob.h>
#include <stdio.h>
#include <iomanip>
#include <sstream>
#include "admin.h"
#include "config.h"
#include "cp.h"
#include "limit.h"
#include "memmgr.h"
#include "meter.h"
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

//default_contention_free_shared_mutex Tsdb::m_tsdb_lock;
pthread_rwlock_t Tsdb::m_tsdb_lock;
std::vector<Tsdb*> Tsdb::m_tsdbs;
static uint64_t tsdb_rotation_freq = 0;
std::atomic<MetricId> Mapping::m_next_id {0};

// maps metrics => Mapping;
std::mutex g_metric_lock;
tsl::robin_map<const char*,Mapping*,hash_func,eq_func> g_metric_map;
thread_local tsl::robin_map<const char*, Mapping*, hash_func, eq_func> thread_local_cache;


Measurement::Measurement() :
    m_ts_count(0),
    m_time_series(nullptr)
{
}

Measurement::Measurement(uint32_t ts_count) :
    m_ts_count(ts_count)
{
    m_time_series = (TimeSeries**)calloc(ts_count, sizeof(TimeSeries*));
    std::memset(m_time_series, 0, ts_count*sizeof(TimeSeries*));
}

Measurement::~Measurement()
{
    if (m_time_series != nullptr)
    {
        for (int i = 0; i < m_ts_count; i++)
            delete m_time_series[i];
        std::free(m_time_series);
    }
}

void
Measurement::add_ts_count(uint32_t ts_count)
{
    if (ts_count == 0) return;
    uint32_t old_count = m_ts_count;
    m_ts_count += ts_count;
    TimeSeries **tmp = m_time_series;
    m_time_series = (TimeSeries**)calloc(m_ts_count, sizeof(TimeSeries*));
    if (tmp != nullptr)
    {
        std::memcpy(m_time_series, tmp, old_count*sizeof(TimeSeries*));
        std::free(tmp);
    }
}

void
Measurement::set_ts_count(uint32_t ts_count)
{
    ASSERT(ts_count > 0);

    m_ts_count = ts_count;

    if (m_time_series != nullptr)
        std::free(m_time_series);

    m_time_series = (TimeSeries**)calloc(ts_count, sizeof(TimeSeries*));
    std::memset(m_time_series, 0, ts_count*sizeof(TimeSeries*));
}

void
Measurement::add_ts(int idx, TimeSeries *ts)
{
    ASSERT(idx >= 0);
    ASSERT(ts != nullptr);

    if (idx >= m_ts_count)
    {
        m_ts_count = idx + 1;
        TimeSeries **tmp = m_time_series;
        m_time_series = (TimeSeries**)calloc(m_ts_count, sizeof(TimeSeries*));
        if (tmp != nullptr)
        {
            std::memcpy(m_time_series, tmp, (m_ts_count-1)*sizeof(TimeSeries*));
            std::free(tmp);
        }
    }

    m_time_series[idx] = ts;
}

TimeSeries *
Measurement::add_ts(const char *field, Mapping *mapping)
{
    ASSERT(m_ts_count > 0);

    //WriteLock guard(m_lock);

    m_ts_count++;
    TimeSeries **tmp = m_time_series;
    m_time_series = (TimeSeries**)calloc(m_ts_count, sizeof(TimeSeries*));
    std::memcpy(m_time_series, tmp, (m_ts_count-1)*sizeof(TimeSeries*));
    std::free(tmp);

    TimeSeries *ts0 = m_time_series[0];
    TagCount tag_cnt = ts0->get_tag_count();
    TagId ids[2 * tag_cnt];
    TagBuilder builder(tag_cnt, ids);
    builder.init(ts0->get_v2_tags());
    builder.update_last(TT_FIELD_TAG_ID, field);
    TimeSeries *ts = new TimeSeries(mapping->get_metric(), builder);
    mapping->add_ts(ts);
    m_time_series[m_ts_count-1] = ts;
    return ts;
}

void
Measurement::append_ts(TimeSeries *ts)
{
    add_ts((int)m_ts_count, ts);
}

TimeSeries *
Measurement::get_ts(int idx, const char *field)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return get_ts_no_lock(idx, field, false);
}

TimeSeries *
Measurement::get_ts_no_lock(int idx, const char *field, bool swap)
{
    ASSERT(field != nullptr);

    TagId vid;
    TimeSeries *ts = nullptr;

    {
        //ReadLock guard(m_lock);
        vid = Tag_v2::get_or_set_id(field);

        if (idx < m_ts_count)
        {
            // try this one first
            ts = m_time_series[idx];
            Tag_v2& tags = ts->get_v2_tags();
            if (tags.match_last(TT_FIELD_TAG_ID, vid))
                return ts;
        }
    }

    //WriteLock guard(m_lock);

    // look for it
    for (int i = 0; i < m_ts_count; i++)
    {
        ts = m_time_series[i];
        Tag_v2& tags = ts->get_v2_tags();
        if (tags.match_last(TT_FIELD_TAG_ID, vid))
        {
            // swap m_time_series[i] & m_time_series[idx]
            if (swap && (idx < m_ts_count))
            {
                m_time_series[i] = m_time_series[idx];
                m_time_series[idx] = ts;
            }
            return ts;
        }
    }

    return nullptr;
}

// get or add the TimeSeries that has no field name
TimeSeries *
Measurement::get_ts(bool add, Mapping *mapping)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return get_ts_no_lock(add, mapping);
}

// get or add the TimeSeries that has no field name
TimeSeries *
Measurement::get_ts_no_lock(bool add, Mapping *mapping)
{
    TimeSeries *ts = get_ts_no_lock(m_ts_count-1, TT_FIELD_VALUE, false);

    if ((ts == nullptr) && add)
    {
        ASSERT(mapping != nullptr);
        ts = add_ts(TT_FIELD_VALUE, mapping);
    }

    return ts;
}

bool
Measurement::get_ts(std::vector<DataPoint>& dps, std::vector<TimeSeries*>& tsv)
{
    //ReadLock guard(m_lock);

    if (dps.size() != m_ts_count) return false;

    for (int i = 0; i < dps.size(); i++)
    {
        DataPoint& dp = dps[i];
        const char *field = dp.get_raw_tags();
        TagId vid = Tag_v2::get_or_set_id(field);
        TimeSeries *ts = m_time_series[i];
        Tag_v2& tags = ts->get_v2_tags();
        if (! tags.match_last(TT_FIELD_TAG_ID, vid))
            return false;
        tsv.push_back(ts);
    }

    return true;
}

void
Measurement::get_all_ts(std::vector<TimeSeries*>& tsv)
{
    tsv.reserve(m_ts_count);
    for (int i = 0; i < m_ts_count; i++)
        tsv.push_back((TimeSeries*)m_time_series[i]);
}

bool
Measurement::add_data_points(std::vector<DataPoint>& dps, Timestamp tstamp, Mapping *mapping)
{
    bool success;
    std::vector<TimeSeries*> tsv;

    tsv.reserve(dps.size());
    std::lock_guard<std::mutex> guard(m_lock);
    success = get_ts(dps, tsv);

    if (LIKELY(success))
    {
        ASSERT(tsv.size() == dps.size());

        for (int i = 0; i < dps.size(); i++)
        {
            TimeSeries *ts = tsv[i];    //get_ts(i++, dp->get_raw_tags());
            DataPoint& dp = dps[i];
            dp.set_timestamp(tstamp);
            success = ts->add_data_point(mapping->get_id(), dp) && success;
        }
    }
    else
    {
        int i = 0;
        success = true;

        for (DataPoint& dp: dps)
        {
            TimeSeries *ts = get_ts_no_lock(i++, dp.get_raw_tags(), true);

            if (ts == nullptr)
                ts = add_ts(dp.get_raw_tags(), mapping);

            dp.set_timestamp(tstamp);
            success = ts->add_data_point(mapping->get_id(), dp) && success;
        }
    }

    return success;
}


Mapping::Mapping(const char *name) :
    m_ts_head(nullptr),
    m_tag_count(-1)
{
    ASSERT(name != nullptr);
    m_id = m_next_id.fetch_add(1);
    m_metric = STRDUP(name);
    MetaFile::instance()->add_metric(m_id, m_metric);

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_lock, &attr);
}

Mapping::Mapping(MetricId id, const char *name) :
    //m_partition(nullptr),
    m_id(id),
    m_ts_head(nullptr),
    m_tag_count(-1)
{
    ASSERT(name != nullptr);
    m_metric = STRDUP(name);
    ASSERT(m_metric != nullptr);
    ASSERT(m_ts_head.load() == nullptr);

    if (m_id >= m_next_id.load())
        m_next_id = m_id + 1;

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_lock, &attr);
}

Mapping::~Mapping()
{
    if (m_metric != nullptr)
    {
        FREE(m_metric);
        m_metric = nullptr;
    }

    std::set<BaseType*> bases;
    for (auto it = m_map.begin(); it != m_map.end(); it++)
    {
        std::free((char*)it->first);
        bases.insert(it->second);   // to remove any duplicates...
    }
    for (auto b : bases) delete b;
    m_map.clear();

    pthread_rwlock_destroy(&m_lock);
}

void
Mapping::flush()
{
    //ReadLock guard(m_lock);
    //std::lock_guard<std::mutex> guard(m_lock);

    for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
    {
        Logger::trace("Flushing ts: %u", ts->get_id());
        ts->flush(m_id);
    }
}

void
Mapping::close()
{
    for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
    {
        Logger::trace("Closing ts: %u", ts->get_id());
        ts->close(m_id);
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
    BaseType *bt = nullptr;
    TimeSeries *ts = nullptr;
    char *raw_tags = dp.get_raw_tags();
    //std::lock_guard<std::mutex> guard(m_lock);

    if (raw_tags != nullptr)
    {
        PThread_ReadLock guard(&m_lock);
        auto result = m_map.find(raw_tags);
        if (result != m_map.end())
            bt = result->second;
    }

    if (bt == nullptr)
    {
        char raw_tags_copy[MAX_TOTAL_TAG_LENGTH];

        if (raw_tags != nullptr)
        {
            std::strncpy(raw_tags_copy, raw_tags, MAX_TOTAL_TAG_LENGTH);
            raw_tags_copy[MAX_TOTAL_TAG_LENGTH-1] = 0;

            if (UNLIKELY(! dp.parse_raw_tags()))
                return nullptr;
        }

        Tag *field = dp.remove_tag(TT_FIELD_TAG_NAME, false);

        if (field != nullptr)
        {
            // The reserved tag name "_field" was found;
            // switch to Measurement...
            ts = get_ts_in_measurement(dp, field);
            dp.remove_tag(field);
            return ts;
        }

        char buff[MAX_TOTAL_TAG_LENGTH];
        dp.get_ordered_tags(buff, MAX_TOTAL_TAG_LENGTH);

        PThread_WriteLock guard(&m_lock);

        {
            auto result = m_map.find(buff);
            if (result != m_map.end())
                bt = result->second;
        }

        if (bt == nullptr)
        {
            ts = new TimeSeries(m_metric, buff, dp.get_tags());
            bt = static_cast<BaseType*>(ts);
            m_map[STRDUP(buff)] = bt;
            add_ts(ts);
            set_tag_count(dp.get_tag_count(true));
        }

        if (raw_tags != nullptr)
        {
            if (std::strcmp(raw_tags_copy, buff) != 0)
                m_map[STRDUP(raw_tags_copy)] = bt;
        }
    }

    if (bt->is_type(TT_TYPE_MEASUREMENT))
        ts = (static_cast<Measurement*>(bt))->get_ts(true, this);
    else
        ts = static_cast<TimeSeries*>(bt);

    return ts;
}

TimeSeries *
Mapping::get_ts_in_measurement(DataPoint& dp, Tag *field)
{
    ASSERT(field != nullptr);

    TimeSeries *ts = nullptr;
    std::vector<DataPoint> dps;

    dps.emplace_back(dp.get_timestamp(), dp.get_value());
    dps.back().set_raw_tags((char*)field->m_value);
    char buff[MAX_TOTAL_TAG_LENGTH];
    dp.get_ordered_tags(buff, MAX_TOTAL_TAG_LENGTH);
    Measurement *mm = get_measurement(buff, dp, dp.get_metric(), dps);
    ASSERT(mm != nullptr);

    if (mm != nullptr)
        ts = mm->get_ts(0, field->m_value);

    if (ts == nullptr)
        ts = mm->add_ts(field->m_value, this);

    return ts;
}

Measurement *
Mapping::get_measurement(char *raw_tags, TagOwner& owner, const char *measurement, std::vector<DataPoint>& dps)
{
    ASSERT(raw_tags != nullptr);
    BaseType *bt = nullptr;
    TimeSeries *ts = nullptr;
    //std::lock_guard<std::mutex> guard(m_lock);

    {
        PThread_ReadLock guard(&m_lock);
        auto result = m_map.find(raw_tags);
        if (result != m_map.end())
            bt = result->second;
    }

    if (UNLIKELY(bt != nullptr) && UNLIKELY(bt->is_type(TT_TYPE_TIME_SERIES)))
    {
        ts = static_cast<TimeSeries*>(bt);
        bt = nullptr;
    }

    if (UNLIKELY(bt == nullptr))
    {
        char ordered[MAX_TOTAL_TAG_LENGTH];
        char original[MAX_TOTAL_TAG_LENGTH+1];

        //original[0] = 0;    // owner.parse() may check this
        std::strncpy(original, raw_tags, MAX_TOTAL_TAG_LENGTH);

        if (raw_tags[1] == 0)   // no tags
        {
            ordered[0] = raw_tags[0];
            ordered[1] = 0;
        }
        else
        {
            // parse raw tags...
            if (owner.get_tags() == nullptr)
            {
                //if (! owner.parse(&original[1]))
                if (! owner.parse(raw_tags))
                    return nullptr;
            }
            owner.get_ordered_tags(ordered, MAX_TOTAL_TAG_LENGTH);
        }

        PThread_WriteLock guard(&m_lock);

        {
            auto result = m_map.find(ordered);
            if (result != m_map.end())
                bt = result->second;
        }

        if ((bt != nullptr) && (UNLIKELY(bt->is_type(TT_TYPE_TIME_SERIES))))
        {
            ts = static_cast<TimeSeries*>(bt);
            bt = nullptr;
        }

        Measurement *mm = nullptr;

        if (bt == nullptr)
        {
            mm = new Measurement();
            init_measurement(mm, measurement, ordered, owner, dps);
            bt = static_cast<BaseType*>(mm);
            m_map[STRDUP(ordered)] = bt;
        }

        //if (ts == nullptr)
        if (m_map.find(original) == m_map.end())
            m_map[STRDUP(original)] = bt;
        else
            m_map[original] = bt;

        // This is a different time series!?
        if ((ts != nullptr) && (mm != nullptr))
        {
            Tag_v2& tags = ts->get_v2_tags();
            tags.append(TT_FIELD_TAG_ID, TT_FIELD_VALUE_ID);
            mm->append_ts(ts);
        }
    }

    ASSERT(bt->is_type(TT_TYPE_MEASUREMENT));
    return dynamic_cast<Measurement*>(bt);
}

void
Mapping::add_ts(TimeSeries *ts)
{
    ASSERT(ts != nullptr);
    ts->m_next = m_ts_head.exchange(ts);
}

void
Mapping::get_all_ts(std::vector<TimeSeries*>& tsv)
{
    for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
        tsv.push_back(ts);
}

bool
Mapping::add(DataPoint& dp)
{
    TimeSeries *ts = get_ts(dp);
    if (UNLIKELY(ts == nullptr)) return false;
    return ts->add_data_point(m_id, dp);
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

bool
Mapping::add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint>& dps)
{
    ASSERT(measurement != nullptr);
    ASSERT(tags != nullptr);
    ASSERT(! dps.empty());

    char buff[8];

    if (tags == nullptr)
    {
        buff[0] = ';';
        buff[1] = 0;
        tags = &buff[0];
    }

    TagOwner owner(false);
    Measurement *mm = get_measurement(tags, owner, measurement, dps);

    if (UNLIKELY(mm == nullptr))
        return false;

    ASSERT(mm->is_initialized());
    return mm->add_data_points(dps, ts, this);
}

void
Mapping::init_measurement(Measurement *mm, const char *measurement, char *tags, TagOwner& owner, std::vector<DataPoint>& dps)
{
    if (dps.empty()) return;

    TagCount count = owner.get_tag_count(true) + 1;
    TagId ids[2 * count];
    TagBuilder builder(count, ids);

    set_tag_count(count-1);
    builder.init(owner.get_tags());
    mm->set_ts_count(dps.size());

    int i = 0;
    std::vector<std::pair<const char*,TimeSeriesId>> fields;

    for (DataPoint& dp: dps)
    {
        builder.update_last(TT_FIELD_TAG_ID, dp.get_raw_tags());
        TimeSeries *ts = new TimeSeries(builder);
        add_ts(ts);
        mm->add_ts(i++, ts);
        fields.emplace_back(dp.get_raw_tags(), ts->get_id());
    }

    // write meta-file
    MetaFile::instance()->add_measurement(measurement, tags, fields);
}

void
Mapping::query_for_ts(Tag *tags, std::unordered_set<TimeSeries*>& tsv, const char *key, bool explicit_tags)
{
    int tag_count = TagOwner::get_tag_count(tags, true);

    if ((key != nullptr) && (tag_count == m_tag_count))
    {
        PThread_ReadLock guard(&m_lock);
        //std::lock_guard<std::mutex> guard(m_lock);
        auto result = m_map.find(key);
        if (result != m_map.end())
        {
            TimeSeries *ts = nullptr;
            BaseType *bt = result->second;

            if (bt->is_type(TT_TYPE_MEASUREMENT))
            {
                Measurement *mm = dynamic_cast<Measurement*>(bt);
                Tag *tag = TagOwner::find_by_key(tags, TT_FIELD_TAG_NAME);

                if (tag != nullptr)
                    ts = mm->get_ts(0, tag->m_value);
                else
                {
                    std::vector<TimeSeries*> all;
                    mm->get_all_ts(all);
                    for (auto t : all) tsv.insert(t);
                }
            }
            else
                ts = static_cast<TimeSeries*>(bt);

            if (ts != nullptr)
                tsv.insert(ts);
        }
    }

    if (tsv.empty())
    {
        if (tags == nullptr)
        {
            // matches ALL
            for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
                tsv.insert(ts);
        }
        else
        {
            TagMatcher *matcher = (TagMatcher*)
                MemoryManager::alloc_recyclable(RecyclableType::RT_TAG_MATCHER);
            matcher->init(tags);
            int tag_count = TagOwner::get_tag_count(tags, false);

            for (TimeSeries *ts = m_ts_head.load(); ts != nullptr; ts = ts->m_next)
            {
                Tag_v2& tags_v2 = ts->get_v2_tags();
                if (explicit_tags && (tags_v2.get_count() != tag_count))
                    continue;
                if (matcher->match(tags_v2))
                    tsv.insert(ts);
            }

            MemoryManager::free_recyclable(matcher);
        }
    }
}

TimeSeries *
Mapping::restore_ts(std::string& metric, std::string& keys, TimeSeriesId id)
{
    auto search = m_map.find(keys.c_str());

    if (search != m_map.end())
    {
        Logger::fatal("Duplicate entry in meta file: %s %s %u",
            metric.c_str(), keys.c_str(), id);
        throw std::runtime_error("Duplicate entry in meta file");
    }

    Tag *tags = Tag::parse_multiple(keys);
    TimeSeries *ts = new TimeSeries(id, metric.c_str(), keys.c_str(), tags);
    m_map[STRDUP(keys.c_str())] = ts;
    add_ts(ts);
    set_tag_count(TagOwner::get_tag_count(tags, false));
    Tag::free_list(tags, true);
    // for backwards compatibility, tags can be separated by either ',' or ';'
    //if (keys.find_first_of(';') != std::string::npos)   // old style?
        //set_tag_count(std::count(keys.begin(), keys.end(), ';'));
    //else
        //set_tag_count(std::count(keys.begin(), keys.end(), ',')+1);
    return ts;
}

void
Mapping::restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv)
{
    TagOwner owner(false);
    char* buff = MemoryManager::alloc_network_buffer();
    int len = tags.length();

    if (len >= MemoryManager::get_network_buffer_size())
    {
        Logger::error("Failed to restore measurement: %s,%s", measurement.c_str(), tags.c_str());
        return;
    }

    tags.copy(buff, len);
    buff[len] = 0;
    std::vector<DataPoint> dps;

    Measurement *mm = get_measurement(buff, owner, measurement.c_str(), dps);
    if (owner.get_tags() == nullptr)
        owner.parse(buff);
    TagCount count = owner.get_tag_count(true) + 1;
    TagId ids[2 * count];
    TagBuilder builder(count, ids);

    set_tag_count(count);
    builder.init(owner.get_tags());

    int i = mm->get_ts_count();
    mm->add_ts_count(fields.size());

    for (auto field: fields)
    {
        builder.update_last(TT_FIELD_TAG_ID, field.first.c_str());
        TimeSeries *ts = new TimeSeries(builder);
        add_ts(ts);
        mm->add_ts(i++, ts);
        tsv.push_back(ts);
    }

    MemoryManager::free_network_buffer(buff);
}

void
Tsdb::restore_rollup_mgr(std::unordered_map<TimeSeriesId,RollupManager>& map)
{
    std::vector<TimeSeries*> tsv;

    get_all_ts(tsv);

    for (auto ts: tsv)
    {
        auto search = map.find(ts->get_id());
        if (search == map.end()) continue;
        ts->restore_rollup_mgr(search->second);
    }
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
    PThread_ReadLock guard(&m_lock);
    //std::lock_guard<std::mutex> guard(m_lock);
    return m_map.size();
}


/* 'dir' is a full path name. E.g. /tt/data/2023/06/1686441600.1686528000/m0000000001
 */
Metric::Metric(const std::string& dir, PageSize page_size, PageCount page_cnt) :
    m_rollup_data_file(dir+"/rollup.data"),
    m_rollup_header_file(dir+"/rollup.header"),
    m_rollup_header_tmp_file(dir+"/rollup.header.tmp")
{
    create_dir(dir);    // create folder, if necessary

    // try to parse out the id
    auto const pos = dir.find_last_of('/');
    ASSERT(dir[pos+1] == 'm');
    m_id = std::stoul(dir.substr(pos + 2));

    // restore Header/Data files...
    std::vector<std::string> files;
    get_all_files(dir + "/header.*", files);
    for (auto file: files) restore_header(file);
    std::sort(m_header_files.begin(), m_header_files.end(), header_less());

    files.clear();
    get_all_files(dir + "/data.*", files);
    for (auto file: files) restore_data(file, page_size, page_cnt);
    std::sort(m_data_files.begin(), m_data_files.end(), data_less());
}

Metric::~Metric()
{
    close();

    for (auto data: m_data_files)
        delete data;
    for (auto header: m_header_files)
        delete header;
}

void
Metric::restore_data(const std::string& file, PageSize page_size, PageCount page_cnt)
{
    FileIndex id = get_file_suffix(file);
    ASSERT(id != TT_INVALID_FILE_INDEX);
    DataFile *data_file = new DataFile(file, id, page_size, page_cnt);
    m_data_files.push_back(data_file);
}

void
Metric::restore_header(const std::string& file)
{
    m_header_files.push_back(HeaderFile::restore(file));
}

void
Metric::close()
{
    for (auto data: m_data_files)
        data->close();
    for (auto header: m_header_files)
        header->close();
    m_rollup_data_file.close();
    m_rollup_header_file.close();
    m_rollup_header_tmp_file.close();
}

void
Metric::flush(bool sync)
{
    for (auto header: m_header_files)
        header->flush(sync);
}

std::string
Metric::get_metric_dir(std::string& tsdb_dir)
{
    return get_metric_dir(tsdb_dir, m_id);
}

std::string
Metric::get_metric_dir(std::string& tsdb_dir, MetricId id)
{
    std::ostringstream oss;
    oss << tsdb_dir << "/m" << std::setfill('0') << std::setw(10) << id;
    return oss.str();
}

std::string
Metric::get_data_file_name(std::string& tsdb_dir, FileIndex fidx)
{
    std::ostringstream oss;
    oss << tsdb_dir << "/m" << std::setfill('0') << std::setw(10) << m_id << "/data." << std::setw(5) << fidx;
    return oss.str();
}

std::string
Metric::get_header_file_name(std::string& tsdb_dir, FileIndex fidx)
{
    std::ostringstream oss;
    oss << tsdb_dir << "/m" << std::setfill('0') << std::setw(10) << m_id << "/header." << std::setw(5) << fidx;
    return oss.str();
}

DataFile *
Metric::get_data_file(FileIndex file_idx)
{
    if (file_idx < m_data_files.size())
    {
        DataFile *file = m_data_files[file_idx];
        if (file->get_id() == file_idx)
            return file;
    }

    for (auto file: m_data_files)
    {
        if (file->get_id() == file_idx)
            return file;
    }

    return nullptr;
}

HeaderFile *
Metric::get_header_file(FileIndex file_idx)
{
    if (file_idx < m_header_files.size())
    {
        HeaderFile *file = m_header_files[file_idx];
        if (file->get_id() == file_idx)
            return file;
    }

    for (auto file: m_header_files)
    {
        if (file->get_id() == file_idx)
            return file;
    }

    return nullptr;
}

HeaderFile *
Metric::get_last_header(std::string& tsdb_dir, PageCount page_cnt, PageSize page_size)
{
    HeaderFile *header_file;

    if (m_header_files.empty())
    {
        ASSERT(m_data_files.empty());
        // make sure the metric directory under tsdb exists
        //std::string metric_dir = get_metric_dir(tsdb_dir);
        //create_dir(metric_dir);

        header_file = new HeaderFile(get_header_file_name(tsdb_dir, 0), 0, page_cnt, page_size),
        m_header_files.push_back(header_file);
        m_data_files.push_back(new DataFile(get_data_file_name(tsdb_dir, 0), 0, page_size, page_cnt));
    }
    else
    {
        header_file = m_header_files.back();
        header_file->ensure_open(false);

        if (header_file->is_full())
        {
            FileIndex id = m_header_files.size();
            header_file = new HeaderFile(get_header_file_name(tsdb_dir, id), id, page_cnt, page_size);
            m_header_files.push_back(header_file);
            m_data_files.push_back(new DataFile(get_data_file_name(tsdb_dir, id), id, page_size, page_cnt));
        }
    }

    ASSERT(! header_file->is_full());
    return header_file;
}

void
Metric::add_rollup_point(TimeSeriesId tid, uint32_t cnt, double min, double max, double sum)
{
    std::lock_guard<std::mutex> guard(m_rollup_lock);
    m_rollup_data_file.ensure_open(false);
    RollupEntry data_idx = m_rollup_data_file.add_data_point(cnt, min, max, sum);
    m_rollup_header_tmp_file.ensure_open(false);
    m_rollup_header_tmp_file.add_index(tid, data_idx);
}

bool
Metric::rotate(Timestamp now_sec, Timestamp thrashing_threshold)
{
    bool all_closed = true;

    for (DataFile *data_file: m_data_files)
    {
        FileIndex id = data_file->get_id();
        HeaderFile *header_file = m_header_files[id];

        if (data_file->is_open(true))
        {
            Timestamp last_read = data_file->get_last_read();
            if (((int64_t)now_sec - (int64_t)last_read) > (int64_t)thrashing_threshold)
            {
                data_file->close(1);    // close read
                header_file->close();
            }
            else
            {
                all_closed = false;
                Logger::debug("data file %u last read at %" PRIu64 "; now is %" PRIu64,
                    data_file->get_id(), last_read, now_sec);
            }
        }

        if (data_file->is_open(false))
        {
            Timestamp last_write = data_file->get_last_write();
            if (((int64_t)now_sec - (int64_t)last_write) > (int64_t)thrashing_threshold)
            {
                data_file->close(2);    // close for write
                header_file->close();
            }
            else
            {
                all_closed = false;
                Logger::debug("data file %u last write at %" PRIu64 "; now is %" PRIu64,
                    data_file->get_id(), last_write, now_sec);
            }
        }
        else
            header_file->close();
    }

    if (((int64_t)now_sec - (int64_t)m_rollup_data_file.get_last_read()) > (int64_t)thrashing_threshold)
    {
        std::lock_guard<std::mutex> guard(m_rollup_lock);
        m_rollup_data_file.close();
        m_rollup_header_file.close();
    }

    return all_closed;
}

// return true if operation was a success; false otherwise
bool
Metric::rollup(IndexFile *idx_file, int no_entries)
{
    std::lock_guard<std::mutex> guard(m_rollup_lock);

    // remove existing, if any
    m_rollup_header_file.close();
    m_rollup_header_file.build(idx_file, &m_rollup_header_tmp_file, no_entries);

    return true;
}

int
Metric::get_page_count(bool ooo)
{
    int total = 0;
    for (auto header_file: m_header_files)
        total += header_file->count_pages(ooo);
    return total;
}

int
Metric::get_data_page_count()
{
    int total = 0;
    for (auto header_file: m_header_files)
    {
        header_file->ensure_open(true);
        total = std::max(total, (int)header_file->get_page_index());
    }
    return total + 1;
}

int
Metric::get_open_data_file_count(bool for_read)
{
    int total = 0;
    for (auto df: m_data_files)
    {
        if (df->is_open(for_read))
            total++;
    }
    return total;
}

int
Metric::get_open_header_file_count(bool for_read)
{
    int total = 0;
    for (auto hf: m_header_files)
    {
        if (hf->is_open(for_read))
            total++;
    }
    return total;
}


Tsdb::Tsdb(TimeRange& range, bool existing, const char *suffix) :
    m_ref_count(0),
    m_time_range(range),
    m_index_file(Tsdb::get_index_file_name(range, suffix)),
    m_page_size(g_page_size),
    m_page_count(g_page_count)
{
    ASSERT(g_tstamp_resolution_ms ? is_ms(range.get_from()) : is_sec(range.get_from()));

    m_mbucket_count = Config::inst()->get_int(CFG_TSDB_METRIC_BUCKETS,CFG_TSDB_METRIC_BUCKETS_DEF);
    m_metrics.reserve(m_mbucket_count);
    m_compressor_version =
        Config::inst()->get_int(CFG_TSDB_COMPRESSOR_VERSION,CFG_TSDB_COMPRESSOR_VERSION_DEF);
    m_rollup_interval =
        Config::inst()->get_time(CFG_TSDB_ROLLUP_INTERVAL,TimeUnit::SEC,CFG_TSDB_ROLLUP_INTERVAL_DEF);
    if (range.get_duration_sec() < m_rollup_interval)
        m_rollup_interval = range.get_duration_sec();
    m_mode = mode_of();
    Logger::debug("tsdb %T created (mode=%d)", &range, m_mode);
}

Tsdb::~Tsdb()
{
    unload_no_lock();

    for (auto metric: m_metrics)
    {
        if (metric != nullptr)
            delete metric;
    }

    std::lock_guard<std::mutex> guard(m_metrics_lock);
    m_metrics.clear();
    //for (DataFile *file: m_data_files) delete file;
    //for (HeaderFile *file: m_header_files) delete file;
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
        tsdb->restore_config(dir);
        tsdb->reload_header_data_files(dir);
    }
    else    // new one
    {
        // create the folder
        create_dir(dir);

        // write config
        tsdb->write_config(dir);

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
Tsdb::restore_config(const std::string& dir)
{
    Config cfg(dir + "/config");
    cfg.load();

    m_page_size = cfg.get_bytes(CFG_TSDB_PAGE_SIZE, CFG_TSDB_PAGE_SIZE_DEF);
    m_page_count = cfg.get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    m_compressor_version = cfg.get_int(CFG_TSDB_COMPRESSOR_VERSION, CFG_TSDB_COMPRESSOR_VERSION_DEF);
    m_rollup_interval = cfg.get_time(CFG_TSDB_ROLLUP_INTERVAL, TimeUnit::SEC, CFG_TSDB_ROLLUP_INTERVAL_DEF);

    if (cfg.exists(CFG_TSDB_METRIC_BUCKETS))
        m_mbucket_count = cfg.get_int(CFG_TSDB_METRIC_BUCKETS);

    //if (cfg.exists("compacted") && cfg.get_bool("compacted", false))
        //m_mode |= TSDB_MODE_COMPACTED;

    if (cfg.exists("rolled_up") && cfg.get_bool("rolled_up", false))
        m_mode |= TSDB_MODE_ROLLED_UP;

    if (cfg.exists("crashed") && cfg.get_bool("crashed", false))
        m_mode |= TSDB_MODE_CRASHED;
}

void
Tsdb::write_config(const std::string& dir)
{
    Config cfg(dir + "/config");

    cfg.set_value(CFG_TSDB_PAGE_SIZE, std::to_string(m_page_size)+"b");
    cfg.set_value(CFG_TSDB_PAGE_COUNT, std::to_string(m_page_count));
    cfg.set_value(CFG_TSDB_COMPRESSOR_VERSION, std::to_string(m_compressor_version));
    cfg.set_value(CFG_TSDB_ROLLUP_INTERVAL, std::to_string(m_rollup_interval)+"sec");

    if (m_mbucket_count != UINT32_MAX)
        cfg.set_value(CFG_TSDB_METRIC_BUCKETS, std::to_string(m_mbucket_count));

    //if (m_mode & TSDB_MODE_COMPACTED)
        //cfg.set_value("compacted", "true");

    if (m_mode & TSDB_MODE_ROLLED_UP)
        cfg.set_value("rolled_up", "true");

    if (m_mode & TSDB_MODE_CRASHED)
        cfg.set_value("crashed", "true");

    cfg.persist();
}

// Not thread safe. Caller should acquire m_lock before calling!
void
Tsdb::add_config(const std::string& name, const std::string& value)
{
    std::string dir = get_tsdb_dir_name(m_time_range);
    Config cfg(dir + "/config");
    cfg.append(name, value);
}

void
Tsdb::reload_header_data_files(const std::string& dir)
{
    m_metrics.clear();

    // restore metrics
    std::vector<std::string> metric_dirs;
    get_all_files(dir + "/m*", metric_dirs);
    m_metrics.reserve(metric_dirs.size());

    for (auto m: metric_dirs)
    {
        Metric *metric = new Metric(m, m_page_size, m_page_count);
        MetricId id = metric->get_id();

        if (id == m_metrics.size())
            m_metrics.push_back(metric);
        else
        {
            ASSERT(id > m_metrics.size());
            while (id > m_metrics.size())
                m_metrics.push_back(nullptr);
            m_metrics.push_back(metric);
        }
    }
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
        Config::inst()->get_time(CFG_TSDB_ARCHIVE_THRESHOLD, TimeUnit::SEC, CFG_TSDB_ARCHIVE_THRESHOLD_DEF);

    if (now < threshold_sec)
    {
        threshold_sec = now;
    }

    if (! m_time_range.older_than_sec(now - threshold_sec))
    {
        threshold_sec =
            Config::inst()->get_time(CFG_TSDB_READ_ONLY_THRESHOLD, TimeUnit::SEC, CFG_TSDB_READ_ONLY_THRESHOLD_DEF);

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
Tsdb::get_or_add_mapping(const char *metric)
{
    //const char *metric = dp.get_metric();
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
    Mapping *mapping = get_or_add_mapping(dp.get_metric());
    if (mapping == nullptr) return false;
    return mapping->add(dp);
}

Metric *
Tsdb::get_metric(MetricId mid)
{
    std::lock_guard<std::mutex> guard(m_metrics_lock);
    uint32_t bucket = mid % m_mbucket_count;

    if (bucket < m_metrics.size())
        return m_metrics[bucket];
    else
        return nullptr;
}

Metric *
Tsdb::get_or_create_metric(MetricId mid)
{
    std::lock_guard<std::mutex> guard(m_metrics_lock);
    uint32_t bucket = mid % m_mbucket_count;

    // create Metric if necessary
    if (bucket >= m_metrics.size())
    {
        if (m_metrics.empty())
            m_metrics.reserve(Mapping::get_metric_count());

        std::string tsdb_dir = get_tsdb_dir_name(m_time_range); // TODO: tsdb may have a suffix?
        std::string metric_dir = Metric::get_metric_dir(tsdb_dir, bucket);
        Metric *metric = new Metric(metric_dir, m_page_size, m_page_count);

        if (bucket == m_metrics.size())
            m_metrics.push_back(metric);
        else
        {
            while (bucket > m_metrics.size())
                m_metrics.push_back(nullptr);
            m_metrics.push_back(metric);
        }
    }
    else if (m_metrics[bucket] == nullptr)
    {
        std::string tsdb_dir = get_tsdb_dir_name(m_time_range); // TODO: tsdb may have a suffix?
        std::string metric_dir = Metric::get_metric_dir(tsdb_dir, bucket);
        m_metrics[bucket] = new Metric(metric_dir, m_page_size, m_page_count);
    }

    ASSERT(bucket < m_metrics.size());
    ASSERT(m_metrics[bucket] != nullptr);
    return m_metrics[bucket];
}

void
Tsdb::add_rollup_point(MetricId mid, TimeSeriesId tid, uint32_t cnt, double min, double max, double sum)
{
    Metric *metric = get_or_create_metric(mid);
    ASSERT(metric != nullptr);
    metric->add_rollup_point(tid, cnt, min, max, sum);
}

bool
Tsdb::add_data_point(DataPoint& dp, bool forward)
{
    Mapping *mapping = get_or_add_mapping(dp.get_metric());
    if (mapping == nullptr) return false;
    return mapping->add_data_point(dp, forward);
}

bool
Tsdb::add_data_points(const char *measurement, char *tags, Timestamp ts, std::vector<DataPoint>& dps)
{
    ASSERT(measurement != nullptr);
    Mapping *mapping = get_or_add_mapping(measurement);
    if (mapping == nullptr) return false;
    return mapping->add_data_points(measurement, tags, ts, dps);
}

void
Tsdb::restore_metrics(MetricId id, std::string& metric)
{
    Mapping *mapping;
    auto search = g_metric_map.find(metric.c_str());

    if (search == g_metric_map.end())
    {
        mapping = new Mapping(id, metric.c_str());
        g_metric_map[mapping->m_metric] = mapping;
    }
}

TimeSeries *
Tsdb::restore_ts(std::string& metric, std::string& key, TimeSeriesId id)
{
    auto search = g_metric_map.find(metric.c_str());
    ASSERT(search != g_metric_map.end());
    Mapping *mapping = search->second;
    ASSERT(search->first == mapping->m_metric);
    return mapping->restore_ts(metric, key, id);
}

void
Tsdb::restore_measurement(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv)
{
    Mapping *mapping = get_or_add_mapping(measurement.c_str());
    if (mapping != nullptr)
        mapping->restore_measurement(measurement, tags, fields, tsv);
    else
        Logger::warn("restore failed for: %s,%s", measurement.c_str(), tags.c_str());
}

/* The 'key' should not include the special '_field' tag.
 */
MetricId
Tsdb::query_for_ts(const char *metric, Tag *tags, std::unordered_set<TimeSeries*>& ts, const char *key, bool explicit_tags)
{
    Mapping *mapping = nullptr;
    MetricId id = TT_INVALID_METRIC_ID;

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
    {
        id = mapping->get_id();
        mapping->query_for_ts(tags, ts, key, explicit_tags);
    }

    return id;
}

#if 0
bool
Tsdb::query_for_data(TimeSeriesId id, TimeRange& query_range, std::vector<DataPointContainer*>& data)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return query_for_data_no_lock(id, query_range, data);
}

// Query for a single TimeSeries
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
            PThread_Lock lock(data_file->get_lock());
            lock.lock_for_read();
            data_file->ensure_open(true);
            void *page = data_file->get_page(page_header->m_page_index, page_header->m_offset, page_header->m_cursor);

            if (page == nullptr)
            {
                lock.unlock();
                lock.lock_for_write();
                data_file->remap();
                lock.unlock();
                lock.lock_for_read();
                page = data_file->get_page(page_header->m_page_index, page_header->m_offset, page_header->m_cursor);
            }

            if (page == nullptr)
            {
                lock.unlock();
                throw std::runtime_error("Failed to open data file for read.");
            }

            DataPointContainer *container = (DataPointContainer*)
                MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
            container->set_out_of_order(page_header->is_out_of_order());
            container->set_page_index(page_header->get_global_page_index(file_idx, m_page_count));
            container->collect_data(from, tsdb_header, page_header, page);
            ASSERT(container->size() > 0);
            lock.unlock();

            if (page_header->is_out_of_order()) has_ooo = true;

#ifdef _DEBUG
            if (! has_ooo && ! data.empty())    // safety check
            {
                DataPointPair& dp0 = container->get_data_point(0);
                DataPointContainer *prev = data.back();
                size_t size = prev->size();
                ASSERT(size > 0);
                DataPointPair& dp1 = prev->get_data_point(size-1);
                //if (dp0.first < dp1.first) has_ooo = true;
                ASSERT(dp0.first >= dp1.first);
            }
#endif

            data.push_back(container);
        }

        file_idx = page_header->get_next_file();
        header_idx = page_header->get_next_header();
    }

    return has_ooo;
}
#endif

// return false if out-of-order data was found, which means rollup data
// can't be used; return true otherwise;
bool
Tsdb::read_rollup_headers(Metric *metric, std::vector<QueryTask*>& tasks)
{
    ASSERT(metric != nullptr);

    auto query_task_cmp = [](QueryTask* &lhs, QueryTask* &rhs)
    {
        uint32_t lhs_file_idx, rhs_file_idx;
        HeaderIndex header_idx; // value not used

        lhs->get_indices(lhs_file_idx, header_idx);
        rhs->get_indices(rhs_file_idx, header_idx);

        return lhs_file_idx > rhs_file_idx;
    };
    std::priority_queue<QueryTask*, std::vector<QueryTask*>, decltype(query_task_cmp)> pq(query_task_cmp);
    bool ooo = false;

    m_index_file.ensure_open(true);

    for (auto task : tasks)
    {
        TimeSeriesId tid = task->get_ts_id();

        if (m_index_file.get_out_of_order(tid))
        {
            ooo = true;
            break;
        }

        RollupIndex idx = m_index_file.get_rollup_index(tid);
        if (idx != TT_INVALID_ROLLUP_INDEX)
        {
            task->set_indices(idx, 0);
            pq.push(task);
        }
        else
        {
            std::vector<RollupIndex> *entries = task->get_rollup_entries();
            ASSERT(entries != nullptr);
        }
    }

    if (ooo) return false;

    int entries = get_rollup_entries();
    std::lock_guard<std::mutex> guard(metric->m_rollup_lock);
    RollupHeaderFile *header_file = metric->get_rollup_header_file();
    header_file->ensure_open(true);

    while (! pq.empty())
    {
        QueryTask *task = pq.top();
        pq.pop();

        RollupIndex idx;
        HeaderIndex header_idx; // not used
        task->get_indices(idx, header_idx);
        header_file->get_entries(idx, entries, task->get_rollup_entries());
    }

    return true;
}

bool
Tsdb::query_rollup_no_lock(RollupDataFile *data_file, QueryTask *task, RollupType rollup)
{
    ASSERT(task != nullptr);
    ASSERT(data_file != nullptr);

    auto containers = task->get_containers();
    ASSERT(! containers.empty());
    auto container = containers.back();

    uint32_t cnt;
    double min, max, sum, value;
    RollupIndex data_idx;
    HeaderIndex header_idx;

    task->get_indices(data_idx, header_idx);
    if (data_idx == TT_INVALID_ROLLUP_ENTRY) return false;
    bool ok = data_file->query(data_idx, cnt, min, max, sum);

    if (! ok) return false;

    if (cnt > 0)
    {
        Timestamp rollup_interval = get_rollup_interval();
        if (g_tstamp_resolution_ms) rollup_interval *= 1000;
        Timestamp tstamp = m_time_range.get_from() + header_idx * rollup_interval;
        ASSERT(m_time_range.in_range(tstamp) == 0);

        switch (rollup)
        {
            case RollupType::RU_AVG:
                value = sum / (double)cnt;
                break;

            case RollupType::RU_CNT:
                value = (double)cnt;
                break;

            case RollupType::RU_MAX:
                value = max;
                break;

            case RollupType::RU_MIN:
                value = min;
                break;

            case RollupType::RU_SUM:
                value = sum;
                break;

            default:
                return false;
        }

        container->add_data_point(tstamp, value);
    }

    // prepare for next entry
    std::vector<RollupIndex> *entries = task->get_rollup_entries();
    header_idx++;
    if (header_idx >= entries->size())
        task->set_indices(TT_INVALID_FILE_INDEX, TT_INVALID_HEADER_INDEX);
    else
        task->set_indices((*entries)[header_idx], header_idx);

    return true;
}

// Query ONE page, for a single TimeSeries
void
Tsdb::query_for_data_no_lock(MetricId mid, QueryTask *task)
{
    ASSERT(task != nullptr);

    uint32_t file_idx;
    HeaderIndex header_idx;
    Timestamp from = m_time_range.get_from();
    TimeRange& query_range = task->get_query_range();
    Metric *metric = get_metric(mid);

    if (metric == nullptr)
    {
        task->set_indices(TT_INVALID_FILE_INDEX, TT_INVALID_HEADER_INDEX);
        return;
    }

    task->get_indices(file_idx, header_idx);
    ASSERT(file_idx != TT_INVALID_FILE_INDEX);
    ASSERT(header_idx != TT_INVALID_HEADER_INDEX);

    HeaderFile *header_file = metric->get_header_file(file_idx);
    header_file->ensure_open(true);
    struct tsdb_header *tsdb_header = header_file->get_tsdb_header();
    struct page_info_on_disk *page_header = header_file->get_page_header(header_idx);
    Timestamp from1 = from + page_header->is_out_of_order() ? 0 : task->get_tstamp_from();
    TimeRange range(from1, from + page_header->m_tstamp_to);
    bool ooo = get_out_of_order(task->get_ts_id());

    if (ooo) task->set_ooo(true);
    task->set_tstamp_from(page_header->m_tstamp_to);

    if (query_range.has_intersection(range))
    {
        DataFile *data_file = metric->get_data_file(file_idx);
        PThread_Lock lock(data_file->get_lock());
        lock.lock_for_read();
        data_file->ensure_open(true);
        void *page = data_file->get_page(page_header->m_page_index);

        if (page == nullptr)
        {
            lock.unlock();
            lock.lock_for_write();
            data_file->remap();
            lock.unlock();
            lock.lock_for_read();
            page = data_file->get_page(page_header->m_page_index);
        }

        if (page == nullptr)
        {
            lock.unlock();
            throw std::runtime_error("Failed to open data file for read.");
        }

        DataPointContainer *container = (DataPointContainer*)
            MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
        container->set_out_of_order(page_header->is_out_of_order());
        container->set_page_index(page_header->get_global_page_index(file_idx, m_page_count));
        container->collect_data(from, tsdb_header, page_header, page);
        ASSERT(container->size() > 0);
        lock.unlock();

        task->add_container(container);
    }

    if (ooo || (range.get_to() <= query_range.get_to()))
    {
        // prepare for the next page
        task->set_indices(page_header->get_next_file(), page_header->get_next_header());
    }
    else
    {
        // no more data to read for this query
        task->set_indices(TT_INVALID_FILE_INDEX, TT_INVALID_HEADER_INDEX);
    }
}

void
Tsdb::query_for_data(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, bool compact, RollupType rollup)
{
    std::lock_guard<std::mutex> guard(m_lock);
    query_for_data_no_lock(mid, range, tasks, compact, rollup);
}

// Query for multiple TimeSeries
void
Tsdb::query_for_data_no_lock(MetricId mid, TimeRange& range, std::vector<QueryTask*>& tasks, bool compact, RollupType rollup)
{
    uint32_t file_or_rollup_idx;
    FileIndex file_idx;
    HeaderIndex header_idx;
    auto query_task_cmp = [](QueryTask* &lhs, QueryTask* &rhs)
    {
        uint32_t lhs_file_idx, rhs_file_idx;
        HeaderIndex lhs_header_idx, rhs_header_idx;

        lhs->get_indices(lhs_file_idx, lhs_header_idx);
        rhs->get_indices(rhs_file_idx, rhs_header_idx);

        if (lhs_file_idx > rhs_file_idx)
            return true;
        else if (lhs_file_idx == rhs_file_idx)
            return lhs_header_idx > rhs_header_idx;
        else
            return false;
    };
    std::priority_queue<QueryTask*, std::vector<QueryTask*>, decltype(query_task_cmp)> pq(query_task_cmp);

    m_mode |= TSDB_MODE_READ;
    m_index_file.ensure_open(true);
    Timestamp middle = m_time_range.get_middle();
    Metric *metric = get_metric(mid);

    if (rollup != RollupType::RU_NONE)
    {
        // query rollup data
        if (read_rollup_headers(metric, tasks))
        {
            for (auto task : tasks)
            {
                std::vector<RollupIndex> *entries = task->get_rollup_entries();
                if (! entries->empty())
                {
                    task->set_indices((*entries)[0], 0);
                    pq.push(task);

                    DataPointContainer *container = (DataPointContainer*)
                        MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
                    //container->set_page_index(page_header->get_global_page_index(file_idx, m_page_count));
                    task->add_container(container);
                }
            }
        }
        else
        {
            // out-of-order data found, can't use rollup data
            rollup = RollupType::RU_NONE;
        }
    }

    if (rollup == RollupType::RU_NONE)
    {
        // query raw data
        for (auto task : tasks)
        {
            // figure out first page location before pushing to pq
            if (middle < range.get_from())
                m_index_file.get_indices2(task->get_ts_id(), file_idx, header_idx);
            else
                m_index_file.get_indices(task->get_ts_id(), file_idx, header_idx);
            if (file_idx == TT_INVALID_FILE_INDEX || header_idx == TT_INVALID_HEADER_INDEX)
                continue;
            task->set_indices(file_idx, header_idx);
            pq.push(task);
        }
    }

    uint32_t last_file_idx = TT_INVALID_ROLLUP_INDEX;
    RollupDataFile *data_file = metric->get_rollup_data_file();
    PThread_Lock lock(data_file->get_lock());

    if (rollup != RollupType::RU_NONE)
    {
        lock.lock_for_read();
        data_file->ensure_open(true);
    }

    while (! pq.empty())
    {
        QueryTask *task = pq.top();
        pq.pop();

        // try to close previous files
        if (compact)
        {
            task->get_indices(file_or_rollup_idx, header_idx);
            if ((file_or_rollup_idx != last_file_idx) && (last_file_idx != TT_INVALID_ROLLUP_INDEX))
            {
                if (metric != nullptr)
                {
                    metric->get_header_file(last_file_idx)->close();
                    metric->get_data_file(last_file_idx)->close();
                }
            }
            last_file_idx = file_idx;
        }

        if (rollup != RollupType::RU_NONE)
        {
            bool ok = query_rollup_no_lock(data_file, task, rollup);

            if (! ok)
            {
                // remap
                lock.unlock();
                lock.lock_for_write();
                data_file->remap();
                lock.unlock();
                lock.lock_for_read();

                ok = query_rollup_no_lock(data_file, task, rollup);
                if (! ok) continue;
            }
        }
        else
            query_for_data_no_lock(mid, task);
        task->get_indices(file_or_rollup_idx, header_idx);

        if (! get_out_of_order(task->get_ts_id()) && (rollup == RollupType::RU_NONE))
            task->merge_data();

        if (file_or_rollup_idx != TT_INVALID_ROLLUP_INDEX && header_idx != TT_INVALID_HEADER_INDEX)
            pq.push(task);
    }

    if (rollup != RollupType::RU_NONE)
        lock.unlock();

    if (compact)
        m_index_file.close();
}

// This will make tsdb read-only!
void
Tsdb::flush(bool sync)
{
    //for (DataFile *file: m_data_files) file->flush(sync);
    //for (HeaderFile *file: m_header_files) file->flush(sync);
    for (auto metric: m_metrics)
    {
        if (metric != nullptr)
            metric->flush(sync);
    }
    m_index_file.flush(sync);
}

// for testing only
void
Tsdb::flush_for_test()
{
    //std::vector<TimeSeries*> tsv;
    //get_all_ts(tsv);
    //for (auto ts: tsv) ts->flush(false);

    //for (DataFile *file: m_data_files) file->flush(sync);
    //for (HeaderFile *file: m_header_files) file->flush(sync);
    std::vector<Mapping*> mappings;
    get_all_mappings(mappings);
    for (auto mapping: mappings)
        mapping->flush();

    for (auto metric: m_metrics)
    {
        if (metric != nullptr)
            metric->flush(sync);
    }
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
            mapping->close();
#ifdef _DEBUG
            //TimeSeries *next;
            //for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = next)
            //{
                //next = ts->m_next;
                //delete ts;
            //}
            delete mapping;
#endif
        }

        g_metric_map.clear();
    }

    PThread_WriteLock guard(&m_tsdb_lock);

    for (Tsdb *tsdb: m_tsdbs)
    {
        //WriteLock guard(tsdb->m_lock);
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        tsdb->flush(true);
        //for (DataFile *file: tsdb->m_data_files) file->close();
        //for (HeaderFile *file: tsdb->m_header_files) file->close();
        for (auto metric: tsdb->m_metrics)
        {
            if (metric != nullptr)
                metric->close();
        }
        tsdb->m_index_file.close();
#ifdef _DEBUG
        delete tsdb;
#endif
    }

    m_tsdbs.clear();
    CheckPointManager::close();
    MetaFile::instance()->close();
    TimeSeries::cleanup();
    Logger::info("Tsdb::shutdown complete");
}

void
Tsdb::dec_ref_count()
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_ref_count--;
    ASSERT(m_ref_count >= 0);
}

void
Tsdb::dec_ref_count_no_lock()
{
    m_ref_count--;
    ASSERT(m_ref_count >= 0);
}

void
Tsdb::inc_ref_count()
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_ref_count++;
    ASSERT(m_ref_count > 0);
}

// get timestamp of last data-point in this Tsdb, for the given TimeSeries
Timestamp
Tsdb::get_last_tstamp(MetricId mid, TimeSeriesId tid)
{
    // returning TT_INVALID_TIMESTAMP will treat future dps as out of order,
    // which in turn will invalidate rollup data.
    if (is_rolled_up())
        return TT_INVALID_TIMESTAMP;

    Timestamp tstamp = 0;
    FileIndex fidx, file_idx;
    HeaderIndex hidx, header_idx;
    struct page_info_on_disk *page_header = nullptr;
    struct tsdb_header *tsdb_header = nullptr;

    file_idx = TT_INVALID_FILE_INDEX;
    header_idx = TT_INVALID_HEADER_INDEX;

    //ReadLock guard(m_lock);
    std::lock_guard<std::mutex> guard(m_lock);

    m_index_file.ensure_open(false);
    m_index_file.get_indices2(tid, fidx, hidx);
    if (fidx == TT_INVALID_FILE_INDEX)
        m_index_file.get_indices(tid, fidx, hidx);

    while (fidx != TT_INVALID_FILE_INDEX)
    {
        ASSERT(hidx != TT_INVALID_HEADER_INDEX);

        HeaderFile *header_file = get_header_file(mid, fidx);
        ASSERT(header_file != nullptr);
        ASSERT(header_file->get_id() == fidx);

        header_file->ensure_open(true);
        struct page_info_on_disk *header = header_file->get_page_header(hidx);

        if (! header->is_out_of_order())
        {
            file_idx = fidx;
            header_idx = hidx;
            page_header = header;
            tsdb_header = header_file->get_tsdb_header();
        }

        fidx = header->m_next_file;
        hidx = header->m_next_header;
    }

    if (page_header != nullptr)
    {
        DataFile *data_file = get_data_file(mid, file_idx);
        PThread_Lock lock(data_file->get_lock());
        lock.lock_for_read();
        data_file->ensure_open(true);
        void *page = data_file->get_page(page_header->m_page_index);

        if (page == nullptr)
        {
            lock.unlock();
            lock.lock_for_write();
            data_file->remap();
            lock.unlock();
            lock.lock_for_read();
            page = data_file->get_page(page_header->m_page_index);
        }

        if (page == nullptr)
        {
            lock.unlock();
            throw std::runtime_error("Failed to open data file for read.");
        }

        DataPointContainer *container = (DataPointContainer*)
            MemoryManager::alloc_recyclable(RecyclableType::RT_DATA_POINT_CONTAINER);
        container->set_page_index(page_header->get_global_page_index(file_idx, m_page_count));
        container->collect_data(m_time_range.get_from(), tsdb_header, page_header, page);
        lock.unlock();
        if (! container->is_empty())
            tstamp = container->get_last_data_point().first;
        MemoryManager::free_recyclable(container);
    }

    return tstamp;
}

bool
Tsdb::get_out_of_order(TimeSeriesId tid)
{
    return m_index_file.get_out_of_order(tid);
}

void
Tsdb::set_out_of_order(TimeSeriesId tid, bool ooo)
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_index_file.ensure_open(false);
    m_index_file.set_out_of_order(tid, ooo);
}

// this is only called when writing data points
void
Tsdb::get_last_header_indices(MetricId mid, TimeSeriesId tid, FileIndex& file_idx, HeaderIndex& header_idx)
{
    FileIndex fidx;
    HeaderIndex hidx;

    file_idx = TT_INVALID_FILE_INDEX;
    header_idx = TT_INVALID_HEADER_INDEX;

    //ReadLock guard(m_lock);
    std::lock_guard<std::mutex> guard(m_lock);

    m_mode |= TSDB_MODE_READ;
    if (is_rolled_up()) add_config("rolled_up", "false");
    m_mode &= ~(TSDB_MODE_COMPACTED|TSDB_MODE_ROLLED_UP);
    m_index_file.ensure_open(false);
    //m_index_file.get_indices(tid, fidx, hidx);
    m_index_file.get_indices2(tid, fidx, hidx);
    if (fidx == TT_INVALID_FILE_INDEX)
        m_index_file.get_indices(tid, fidx, hidx);

    while (fidx != TT_INVALID_FILE_INDEX)
    {
        ASSERT(hidx != TT_INVALID_HEADER_INDEX);

        file_idx = fidx;
        header_idx = hidx;

        HeaderFile *header_file = get_header_file(mid, fidx);
        ASSERT(header_file != nullptr);
        ASSERT(header_file->get_id() == fidx);

        header_file->ensure_open(true);
        struct page_info_on_disk *header = header_file->get_page_header(hidx);
        fidx = header->m_next_file;
        hidx = header->m_next_header;
    }
}

/* @params  crossed 
 */
void
Tsdb::set_indices(MetricId mid, TimeSeriesId tid, FileIndex prev_file_idx, HeaderIndex prev_header_idx, FileIndex this_file_idx, HeaderIndex this_header_idx, bool crossed)
{
    ASSERT(TT_INVALID_FILE_INDEX != this_file_idx);
    ASSERT(TT_INVALID_HEADER_INDEX != this_header_idx);

    if ((prev_file_idx == TT_INVALID_FILE_INDEX) || (this_header_idx == TT_INVALID_HEADER_INDEX))
        m_index_file.set_indices(tid, this_file_idx, this_header_idx);
    else
    {
        // update header
        HeaderFile *header_file = get_header_file(mid, prev_file_idx);
        ASSERT(header_file != nullptr);
        ASSERT(header_file->get_id() == prev_file_idx);
        header_file->ensure_open(false);
        header_file->update_next(prev_header_idx, this_file_idx, this_header_idx);
    }

    if (crossed)
    {
        FileIndex file_idx = TT_INVALID_FILE_INDEX;
        HeaderIndex header_idx TT_INVALID_HEADER_INDEX;
        m_index_file.get_indices2(tid, file_idx, header_idx);
        if (file_idx == TT_INVALID_FILE_INDEX)
            m_index_file.set_indices2(tid, this_file_idx, this_header_idx);
    }
}

DataFile *
Tsdb::get_data_file(MetricId mid, FileIndex file_idx)
{
    uint32_t bucket = mid % m_mbucket_count;
    if (bucket < m_metrics.size())
    {
        Metric *metric = m_metrics[bucket];
        if (metric != nullptr)
            return metric->get_data_file(file_idx);
    }
    return nullptr;
}

HeaderFile *
Tsdb::get_header_file(MetricId mid, FileIndex file_idx)
{
    uint32_t bucket = mid % m_mbucket_count;
    if (bucket < m_metrics.size())
    {
        Metric *metric = m_metrics[bucket];
        if (metric != nullptr)
            return metric->get_header_file(file_idx);
    }
    return nullptr;

}

PageSize
Tsdb::append_page(MetricId mid, TimeSeriesId tid, FileIndex prev_file_idx, HeaderIndex prev_header_idx, struct page_info_on_disk *header, uint32_t tstamp_from, void *page, bool compact)
{
    ASSERT(page != nullptr);
    ASSERT(header != nullptr);
    //ASSERT(mid < m_metrics.size());
    //ASSERT(m_metrics[mid] != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);
    //WriteLock guard(m_lock);
    Metric *metric = get_or_create_metric(mid);

    ASSERT(metric != nullptr);

#if 0
    uint32_t bucket = mid % m_mbucket_count;

    // create Metric if necessary
    if (bucket >= m_metrics.size())
    {
        if (m_metrics.empty())
            m_metrics.reserve(Mapping::get_metric_count());

        std::string tsdb_dir = get_tsdb_dir_name(m_time_range); // TODO: tsdb may have a suffix?
        std::string metric_dir = Metric::get_metric_dir(tsdb_dir, bucket);
        Metric *metric = new Metric(metric_dir, m_page_size, m_page_count);

        if (bucket == m_metrics.size())
            m_metrics.push_back(metric);
        else
        {
            while (bucket > m_metrics.size())
                m_metrics.push_back(nullptr);
            m_metrics.push_back(metric);
        }
    }
    else if (m_metrics[bucket] == nullptr)
    {
        std::string tsdb_dir = get_tsdb_dir_name(m_time_range); // TODO: tsdb may have a suffix?
        std::string metric_dir = Metric::get_metric_dir(tsdb_dir, bucket);
        m_metrics[bucket] = new Metric(metric_dir, m_page_size, m_page_count);
    }

    ASSERT(bucket < m_metrics.size());
#endif

    const char *suffix = compact ? TEMP_SUFFIX : nullptr;
    std::string tsdb_dir = get_tsdb_dir_name(m_time_range, suffix);
    HeaderFile *header_file = metric->get_last_header(tsdb_dir, get_page_count(), get_page_size());

    //m_load_time = ts_now_sec();
    m_mode |= TSDB_MODE_READ_WRITE;

    //ASSERT(m_metrics[bucket] != nullptr);
    DataFile *data_file = metric->get_last_data();

    data_file->ensure_open(false);
    //header_file->ensure_open(false);
    m_index_file.ensure_open(false);

    ASSERT(! header_file->is_full());
    ASSERT(header_file->get_id() == data_file->get_id());

    //header->m_offset = data_file->get_offset();
    header->m_page_index = data_file->append(page, get_page_size());

    struct tsdb_header *tsdb_header = header_file->get_tsdb_header();
    ASSERT(tsdb_header != nullptr);

    if (tsdb_header->m_page_index < header->m_page_index)
        tsdb_header->m_page_index = header->m_page_index;
    tsdb_header->set_compacted(compact);

    // adjust range in tsdb_header
    Timestamp from = m_time_range.get_from() + tstamp_from;
    Timestamp to = m_time_range.get_from() + header->m_tstamp_to;

    ASSERT(to <= m_time_range.get_to());

    Timestamp middle = m_time_range.get_middle();
    bool crossed = (middle < to);

    if (tsdb_header->m_start_tstamp > from)
        tsdb_header->m_start_tstamp = from;
    if (tsdb_header->m_end_tstamp < to)
        tsdb_header->m_end_tstamp = to;

    HeaderIndex header_idx = header_file->new_header_index(this);
    ASSERT(header_idx != TT_INVALID_HEADER_INDEX);
    struct page_info_on_disk *new_header = header_file->get_page_header(header_idx);
    //ASSERT(header->m_page_index == new_header->m_page_index);
    new_header->init(header);

    set_indices(mid, tid, prev_file_idx, prev_header_idx, header_file->get_id(), header_idx, crossed);

    // passing our own indices back to caller
    header->m_next_file = header_file->get_id();
    header->m_next_header = header_idx;
    ASSERT(get_header_file(mid, header->m_next_file) == header_file);

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
        PThread_ReadLock guard(&m_tsdb_lock);
        tsdb = Tsdb::search(tstamp);
    }

    if ((tsdb == nullptr) && create)
    {
        PThread_WriteLock guard(&m_tsdb_lock);
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

    if (tsdb != nullptr)
        tsdb->inc_ref_count();

    return tsdb;
}

void
Tsdb::insts(const TimeRange& range, std::vector<Tsdb*>& tsdbs)
{
    PThread_ReadLock guard(&m_tsdb_lock);
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
        {
            tsdb->inc_ref_count();
            tsdbs.push_back(tsdb);
        }
    }
}

bool
Tsdb::http_api_put_handler(HttpRequest& request, HttpResponse& response)
{
    ASSERT(request.content != nullptr);
    char *curr = request.content;

    while (std::isspace(*curr))
        curr++;

    if ((*curr == '[') || (*curr == '{'))
        return http_api_put_handler_json(request, response);
    else
        return http_api_put_handler_plain(request, response);
}

// Should be able to handle both signle data point or multiple data points.
bool
Tsdb::http_api_put_handler_json(HttpRequest& request, HttpResponse& response)
{
    ASSERT(request.content != nullptr);
    char *curr = request.content;

    while (std::isspace(*curr))
        curr++;

    int success = 0;
    int failed = 0;

    if (*curr == '{')
    {
        // single data point
        DataPoint dp;
        curr = dp.from_json(curr);

        if ((curr != nullptr) && add_data_point(dp, false))
            success++;
        else
            failed++;
    }
    else
    {
        // multiple data point
        while ((*curr != ']') && (*curr != 0))
        {
            DataPoint dp;
            curr = dp.from_json(curr+1);
            if (curr == nullptr) break;

            if (add_data_point(dp, false))
                success++;
            else
                failed++;
            while (isspace(*curr)) curr++;
        }
    }

    char buff[64];
    snprintf(buff, sizeof(buff), "{\"success\":%d,\"failed\":%d}", success, failed);
    response.init(200, HttpContentType::JSON, std::strlen(buff), buff);
    return true;
}

bool
Tsdb::http_api_put_handler_plain(HttpRequest& request, HttpResponse& response)
{
    ASSERT(request.content != nullptr);
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
    ASSERT((request.length+2) < MemoryManager::get_network_buffer_size());
    curr[request.length] = '\n';
    curr[request.length+1] = ' ';
    curr[request.length+2] = 0;

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
    response.init((success ? 200 : 400), HttpContentType::PLAIN);

    return success;
}

/* Data format:
 *  <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
 *
 * See: docs.influxdata.com/influxdb/v2.5/reference/syntax/line-protocol/
 */
bool
Tsdb::http_api_write_handler(HttpRequest& request, HttpResponse& response)
{
    ASSERT(request.content != nullptr);
    Logger::trace("Entered http_api_put_handler_influx()...");

    char *curr = request.content;
    bool success = true;
    std::vector<DataPoint> dps;
    Timestamp now = ts_now();

    // safety measure
    curr[request.length] = 0;
    curr[request.length+1] = ' ';
    curr[request.length+2] = '=';
    curr[request.length+3] = '\n';
    curr[request.length+4] = '0';

    while ((curr != nullptr) && (*curr != 0))
    {
        if (UNLIKELY(*curr == '#'))
        {
            curr = strchr(curr, '\n');
            curr++;
            continue;
        }

        const char *measurement;
        char *tags = nullptr;
        Timestamp ts = 0;

        bool ok = parse_line(curr, measurement, tags, ts, dps);
        if (! ok) { success = false; break; }

        if (ts == 0) ts = now;
        success = add_data_points(measurement, tags, ts, dps) && success;

        dps.clear();
    }

    response.init((success ? 200 : 400), HttpContentType::PLAIN);
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
            std::set<std::string> keys;

            //for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
            for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
                ts->get_keys(keys);

            for (std::string key: keys)
            {
                if (starts_with(key.c_str(), prefix))
                {
                    suggestions.insert(key);
                    if (suggestions.size() >= max) break;
                }
            }
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
            std::set<std::string> values;

            //for (auto it2 = mapping->m_map.begin(); it2 != mapping->m_map.end(); it2++)
            for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
                ts->get_values(values);

            for (std::string value: values)
            {
                if (starts_with(value.c_str(), prefix))
                {
                    suggestions.insert(value);
                    if (suggestions.size() >= max) break;
                }
            }
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

bool
Tsdb::parse_line(char* &line, const char* &measurement, char* &tags, Timestamp& ts, std::vector<DataPoint>& dps)
{
    measurement = line;

    // look for first comma or space
    for ( ; ; )
    {
        if (*line == '\\')
        {
            *line++ = '_';
            switch (*line)
            {
                case ',':   *line = 'C'; break;
                case '=':   *line = 'E'; break;
                case ' ':   *line = 'S'; break;
                default:    *line = '_'; break;
            }
            line++;
        }
        else if ((*line == ',') || (*line == ' '))
            break;
        else
            line++;
    }

    //do { line++; }
    //while ((*line != ',' || *(line-1) == '\\') && (*line != ' ' || *(line-1) == '\\'));

    if (*line == ',')
    {
        *line = 0;  // end of measurement
        tags = ++line;

        for ( ; ; )
        {
            if (*line == '\\')
            {
                *line++ = '_';
                switch (*line)
                {
                    case ',':   *line = 'C'; break;
                    case '=':   *line = 'E'; break;
                    case ' ':   *line = 'S'; break;
                    default:    *line = '_'; break;
                }
                line++;
            }
            else if (*line == ' ')
                break;
            else
                line++;
        }

        //do { line++; }
        //while (*line != ' ' || *(line-1) == '\\');
    }

    *line = 0;

    // ' <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]'
    do
    {
        dps.emplace_back();

        DataPoint& dp = dps.back();
        char *field = ++line;

        // look for first equal sign
        for ( ; ; )
        {
            if (*line == '\\')
            {
                *line++ = '_';
                switch (*line)
                {
                    case ',':   *line = 'C'; break;
                    case '=':   *line = 'E'; break;
                    case ' ':   *line = 'S'; break;
                    default:    *line = '_'; break;
                }
                line++;
            }
            else if (*line == '=')
                break;
            else
                line++;
        }
        //while (*line != '=' || *(line-1) == '\\')
            //line++;
        *line++ = 0;  // end of field name
        dp.set_raw_tags(field);    // use raw_tags to remember field name
        dp.set_value(std::atof(line));

        while ((*line != ',' || *line == '\\') && (*line != ' ' || *(line-1) == '\\') && (*line != '\n') && (*line != 0))
            line++;
    } while (*line == ',');

    if (*line == ' ') line++;
    if (*line == '\n')
        line++;
    else if (*line != 0)
    {
        ts = std::atoll(line);
        while (*line != '\n' && *line != 0) line++;
        if (*line == '\n') line++;

        // convert ts to desired unit
        ts = validate_resolution(ts);
    }

    return true;
}

void
Tsdb::init()
{
    std::string data_dir = Config::get_data_dir();
    Logger::info("Loading data from %s", data_dir.c_str());

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_tsdb_lock, &attr);

    CheckPointManager::init();
    PartitionManager::init();
    TimeSeries::init();

    tsdb_rotation_freq =
        Config::inst()->get_time(CFG_TSDB_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_TSDB_ROTATION_FREQUENCY_DEF);
    if (g_tstamp_resolution_ms)
        tsdb_rotation_freq *= 1000L;
    if (tsdb_rotation_freq < 1) tsdb_rotation_freq = 1;

    // check if we have enough disk space
    unsigned long page_count =
        Config::inst()->get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    uint64_t avail = get_disk_available_blocks(data_dir);

    // make sure page-count is not greater than UINT16_MAX
    if (page_count > UINT16_MAX)
    {
        Logger::warn("tsdb.page.count too large: %u, using %u", page_count, UINT16_MAX);
        page_count = UINT16_MAX;
    }

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

    MetaFile::init(Tsdb::restore_metrics, Tsdb::restore_ts, Tsdb::restore_measurement);

    //compact2();

    // Setup maintenance tasks
    Task task;
    task.doit = &Tsdb::rotate;
    task.data.integer = 0;
    int freq_sec = Config::inst()->get_time(CFG_TSDB_FLUSH_FREQUENCY, TimeUnit::SEC, CFG_TSDB_FLUSH_FREQUENCY_DEF);
    if (freq_sec < 1) freq_sec = 1;
    Timer::inst()->add_task(task, freq_sec, "tsdb_flush");
    Logger::info("Will try to rotate tsdb every %d secs.", freq_sec);

    task.doit = &Tsdb::archive_ts;
    task.data.integer = 0;
    freq_sec = Config::inst()->get_time(CFG_TS_ARCHIVE_THRESHOLD, TimeUnit::SEC, CFG_TS_ARCHIVE_THRESHOLD_DEF);
    if (freq_sec > 0)
    {
        freq_sec /= 2;
        if (freq_sec <= 0) freq_sec = 1;
        Timer::inst()->add_task(task, freq_sec, "ts_archive");
        Logger::info("Will try to archive ts every %d secs.", freq_sec);
    }

/*
    task.doit = &Tsdb::compact;
    task.data.integer = 0;  // indicates this is from scheduled task (vs. interactive cmd)
    freq_sec = Config::inst()->get_time(CFG_TSDB_COMPACT_FREQUENCY, TimeUnit::SEC, CFG_TSDB_COMPACT_FREQUENCY_DEF);
    if (freq_sec > 0)
    {
        Timer::inst()->add_task(task, freq_sec, "tsdb_compact");
        Logger::info("Will try to compact tsdb every %d secs.", freq_sec);
    }
*/

    task.doit = &Tsdb::rollup;
    task.data.integer = 0;  // indicates this is from scheduled task (vs. interactive cmd)
    freq_sec = Config::inst()->get_time(CFG_TSDB_ROLLUP_FREQUENCY, TimeUnit::SEC, CFG_TSDB_ROLLUP_FREQUENCY_DEF);
    if (freq_sec > 0)
    {
        Timer::inst()->add_task(task, freq_sec, "tsdb_rollup");
        Logger::info("Will try to rollup tsdb every %d secs.", freq_sec);
    }
}

std::string
Tsdb::get_tsdb_dir_name(const TimeRange& range, const char *suffix)
{
    time_t sec = range.get_from_sec();
    struct tm timeinfo;
    localtime_r(&sec, &timeinfo);
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/%d/%02d/%" PRIu64 ".%" PRIu64 "%s",
        Config::get_data_dir().c_str(),
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
    return TimeSeries::get_next_id();
}

// for testing only
int
Tsdb::get_page_count(bool ooo)
{
    int total = 0;
    for (auto tsdb: m_tsdbs)
    {
        for (auto metric: tsdb->m_metrics)
        {
            if (metric != nullptr)
                total += metric->get_page_count(ooo);
        }
    }
    return total;
}

int
Tsdb::get_data_page_count()
{
    int total = 0;
    for (auto tsdb: m_tsdbs)
    {
        for (auto metric: tsdb->m_metrics)
        {
            if (metric != nullptr)
                total += metric->get_data_page_count();
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
    PThread_ReadLock guard(&m_tsdb_lock);
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
    PThread_ReadLock guard(&m_tsdb_lock);
    return (int)m_tsdbs.size();
}

int
Tsdb::get_open_data_file_count(bool for_read)
{
    int total = 0;
    PThread_ReadLock guard(&m_tsdb_lock);
    for (Tsdb *tsdb: m_tsdbs)
    {
        for (auto metric: tsdb->m_metrics)
        {
            if (metric != nullptr)
                total += metric->get_open_data_file_count(for_read);
        }
    }
    return total;
}

int
Tsdb::get_open_header_file_count(bool for_read)
{
    int total = 0;
    PThread_ReadLock guard(&m_tsdb_lock);
    for (Tsdb *tsdb: m_tsdbs)
    {
        for (auto metric: tsdb->m_metrics)
        {
            if (metric != nullptr)
                total += metric->get_open_header_file_count(for_read);
        }
    }
    return total;
}

int
Tsdb::get_open_index_file_count(bool for_read)
{
    int total = 0;
    PThread_ReadLock guard(&m_tsdb_lock);
    for (Tsdb *tsdb: m_tsdbs)
    {
        if (tsdb->m_index_file.is_open(for_read))
            total++;
    }
    return total;
}

void
Tsdb::unload()
{
    //WriteLock unload_guard(m_load_lock);
    std::lock_guard<std::mutex> guard(m_lock);
    unload_no_lock();
}

// This will archive the tsdb. No write nor read will be possible afterwards.
void
Tsdb::unload_no_lock()
{
    //if (! count_is_zero()) return;
    //for (DataFile *file: m_data_files) file->close();
    //for (HeaderFile *file: m_header_files) file->close();
    for (auto metric: m_metrics)
    {
        if (metric != nullptr)
            metric->close();
    }
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
            Config::inst()->get_time(CFG_TSDB_ARCHIVE_THRESHOLD, TimeUnit::DAY, CFG_TSDB_ARCHIVE_THRESHOLD_DEF);
        Timestamp rotation_freq =
            Config::inst()->get_time(CFG_TSDB_ROTATION_FREQUENCY, TimeUnit::DAY, CFG_TSDB_ROTATION_FREQUENCY_DEF);

        if (rotation_freq < 1) rotation_freq = 1;
        uint64_t days = archive_threshold / rotation_freq;

        if (days > 1)
        {
            // reduce CFG_TSDB_ARCHIVE_THRESHOLD by 1 day
            Config::inst()->set_value(CFG_TSDB_ARCHIVE_THRESHOLD, std::to_string(days-1)+"d");
            Logger::info("Reducing %s by 1 day", CFG_TSDB_ARCHIVE_THRESHOLD);
        }
    }

    CheckPointManager::take_snapshot();

    uint64_t now_sec = to_sec(now);
    uint64_t thrashing_threshold =
        Config::inst()->get_time(CFG_TSDB_THRASHING_THRESHOLD, TimeUnit::SEC, CFG_TSDB_THRASHING_THRESHOLD_DEF);

    for (Tsdb *tsdb: tsdbs)
    {
        if (g_shutdown_requested) break;

        //WriteLock unload_guard(tsdb->m_load_lock);
        //std::lock_guard<std::mutex> unload_guard(tsdb->m_load_lock);
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        if (! (tsdb->m_mode & TSDB_MODE_READ))
        {
            tsdb->dec_ref_count_no_lock();
            continue;    // already archived
        }

//        uint32_t mode = tsdb->mode_of();
//        uint64_t load_time = tsdb->m_load_time;

//        if (disk_avail < 100000000L)    // TODO: get it from config
//            mode &= ~TSDB_MODE_WRITE;

        bool all_closed = true;

        for (auto metric: tsdb->m_metrics)
        {
            if (metric == nullptr) continue;
            all_closed = all_closed && metric->rotate(now_sec, thrashing_threshold);
        }

        tsdb->flush(true);

        if (all_closed)
        {
            Logger::info("[rotate] Archiving %T", tsdb);
            tsdb->unload_no_lock();
        }

        tsdb->dec_ref_count_no_lock();
    }

    CheckPointManager::persist();
    MetaFile::instance()->flush();

    if (Config::inst()->exists(CFG_TSDB_RETENTION_THRESHOLD))
        purge_oldest(Config::inst()->get_int(CFG_TSDB_RETENTION_THRESHOLD));

    return false;
}

/* Try to delete memory pages for those TimeSeries that haven't seen
 * any new data for a long time.
 */
bool
Tsdb::archive_ts(TaskData& data)
{
    Timestamp threshold_sec =
        Config::inst()->get_time(CFG_TS_ARCHIVE_THRESHOLD, TimeUnit::SEC, CFG_TS_ARCHIVE_THRESHOLD_DEF);
    Timestamp now_sec = ts_now_sec();
    std::lock_guard<std::mutex> guard(g_metric_lock);

    for (auto it = g_metric_map.begin(); it != g_metric_map.end(); it++)
    {
        Mapping *mapping = it->second;
        ASSERT(it->first == mapping->m_metric);

        //ReadLock mapping_guard(mapping->m_lock);

        for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
            ts->archive(mapping->get_id(), now_sec, threshold_sec);
    }

    return false;
}

bool
Tsdb::validate(Tsdb *tsdb)
{
    PThread_ReadLock guard(&m_tsdb_lock);

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
        PThread_WriteLock guard(&m_tsdb_lock);

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
    // called from scheduled task? if so, enforce off-hour rule;
    if ((data.integer == 0) && ! is_off_hour()) return false;

    Meter meter(METRIC_TICKTOCK_TSDB_COMPACT_MS);
    Tsdb *tsdb = nullptr;
    Timestamp compact_threshold =
        Config::inst()->get_time(CFG_TSDB_COMPACT_THRESHOLD, TimeUnit::SEC, CFG_TSDB_COMPACT_THRESHOLD_DEF);

    Logger::info("[compact] Finding tsdbs to compact...");
    compact_threshold = ts_now_sec() - compact_threshold;

    // Go through all the Tsdbs, from the oldest to the newest,
    // to find the first uncompacted Tsdb to compact.
    {
        PThread_ReadLock guard(&m_tsdb_lock);

        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); it++)
        {
            if (! (*it)->get_time_range().older_than_sec(compact_threshold))
                break;

            std::lock_guard<std::mutex> guard((*it)->m_lock);
            //WriteLock guard((*it)->m_lock);

            // also make sure it's not readable nor writable while we are compacting
            //if ((*it)->m_mode & (TSDB_MODE_COMPACTED | TSDB_MODE_READ_WRITE))
            if ((*it)->m_mode & TSDB_MODE_COMPACTED)
            {
                Logger::debug("[compact] %T is already compacted, ref-count = %d", (*it), (*it)->m_ref_count);
                continue;
            }
            else if (((*it)->m_mode & TSDB_MODE_READ_WRITE) && (data.integer == 0))
            {
                Logger::debug("[compact] %T is still being accessed, ref-count = %d", (*it), (*it)->m_ref_count);
                continue;
            }
            else if ((*it)->m_ref_count > 0)
            {
                Logger::debug("[compact] ref-count of %T is not zero: %d", (*it), (*it)->m_ref_count);
                continue;
            }

            tsdb = *it;
            break;
        }
    }

    if (tsdb != nullptr)
    {
        std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        if (tsdb->m_ref_count <= 0)
        {
            TimeRange range = tsdb->get_time_range();

            Logger::info("[compact] Found this tsdb to compact: %T", tsdb);

            // perform compaction
            try
            {
                // cleanup existing temporary tsdb, if any
                std::string dir = get_tsdb_dir_name(range, TEMP_SUFFIX);
                if (ends_with(dir, TEMP_SUFFIX)) // safty check
                    rm_dir(dir);

                // create a temporary tsdb to compact data into
                Tsdb *compacted = Tsdb::create(range, false, TEMP_SUFFIX);
#ifdef __x86_64__
                compacted->m_page_size = g_sys_page_size;
#endif

                std::vector<Mapping*> mappings;
                int batch_size =
                    Config::inst()->get_int(CFG_TSDB_COMPACT_BATCH_SIZE, CFG_TSDB_COMPACT_BATCH_SIZE_DEF);

                Tsdb::get_all_mappings(mappings);
                tsdb->m_index_file.ensure_open(true);

                QuerySuperTask super_task(tsdb);
                PageSize next_size = compacted->get_page_size();

                for (Mapping *mapping : mappings)
                {
                    super_task.set_metric_id(mapping->get_id());

                    for (TimeSeries *ts = mapping->get_ts_head(); ts != nullptr; ts = ts->m_next)
                    {
                        super_task.add_task(ts);

                        if (super_task.get_task_count() >= batch_size)
                        {
                            super_task.perform(false);
                            write_to_compacted(mapping->get_id(), super_task, compacted, next_size);
                            super_task.empty_tasks();
                        }
                    }

                    if (super_task.get_task_count() > 0)
                    {
                        super_task.perform(false);
                        write_to_compacted(mapping->get_id(), super_task, compacted, next_size);
                        super_task.empty_tasks();
                    }
                }

                compacted->unload_no_lock();
                delete compacted;
                tsdb->unload_no_lock();

                // rename to indicate compaction was successful
                std::string temp_name = get_tsdb_dir_name(range, TEMP_SUFFIX);
                std::string done_name = get_tsdb_dir_name(range, DONE_SUFFIX);
                rm_dir(done_name);  // in case it already exists
                int rc = std::rename(temp_name.c_str(), done_name.c_str());
                compact2();

                // mark it as compacted
                tsdb->m_mode |= TSDB_MODE_COMPACTED;
                dir = get_tsdb_dir_name(range);
                tsdb->reload_header_data_files(dir);
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
            Logger::debug("[compact] Tsdb busy, not compacting. ref = %d", tsdb->m_ref_count);
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
    std::string pattern = Config::get_data_dir();

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
        Logger::info("[compact] Found %s done compactions", done_dir.c_str());

        // format: <data-directory>/<year>/<month>/1614009600.1614038400.done
        ASSERT(ends_with(done_dir, DONE_SUFFIX));
        std::string base = done_dir.substr(0, done_dir.size()-std::strlen(DONE_SUFFIX));
        std::string back = base + BACK_SUFFIX;

        rm_dir(back);   // in case it exists

        // mv 1614009600.1614038400 to 1614009600.1614038400.back
        if (file_exists(base))
            std::rename(base.c_str(), back.c_str());

        // mv 1614009600.1614038400.done to 1614009600.1614038400
        std::rename(done_dir.c_str(), base.c_str());

        // rm 1614009600.1614038400.back
        if (file_exists(base) && file_exists(back))
        {
            // TODO: enable this
            //rm_dir(back);
        }
    }
}

bool
Tsdb::rollup(TaskData& data)
{
    // called from scheduled task? if so, enforce off-hour rule;
    if ((data.integer == 0) && ! is_off_hour()) return false;

    Meter meter(METRIC_TICKTOCK_TSDB_ROLLUP_MS);
    Tsdb *tsdb = nullptr;
    Timestamp rollup_threshold =
        Config::inst()->get_time(CFG_TSDB_ROLLUP_THRESHOLD, TimeUnit::SEC, CFG_TSDB_ROLLUP_THRESHOLD_DEF);

    Logger::info("[rollup] Finding tsdbs to rollup...");
    rollup_threshold = ts_now_sec() - rollup_threshold;

    // Go through all the Tsdbs, from the oldest to the newest,
    // to find the first not-rolled-up Tsdb to rollup.
    {
        PThread_ReadLock guard(&m_tsdb_lock);

        for (auto it = m_tsdbs.begin(); it != m_tsdbs.end(); it++)
        {
            if (! (*it)->get_time_range().older_than_sec(rollup_threshold))
                break;

            std::lock_guard<std::mutex> guard((*it)->m_lock);
            //WriteLock guard((*it)->m_lock);

            // also make sure it's not readable nor writable while we are rolling up
            //if ((*it)->m_mode & (TSDB_MODE_COMPACTED | TSDB_MODE_READ_WRITE))
            if ((*it)->is_rolled_up())
            {
                Logger::debug("[rollup] %T is already rolled up, ref-count = %d", (*it), (*it)->m_ref_count);
                continue;
            }
            else if (((*it)->m_mode & TSDB_MODE_READ_WRITE) && (data.integer == 0))
            {
                Logger::debug("[rollup] %T is still being accessed, ref-count = %d", (*it), (*it)->m_ref_count);
                continue;
            }
            else if ((*it)->m_ref_count > 0)
            {
                Logger::debug("[rollup] ref-count of %T is not zero: %d", (*it), (*it)->m_ref_count);
                continue;
            }

            tsdb = *it;
            break;
        }
    }

    data.integer = 0;

    if (tsdb != nullptr)
    {
        //std::lock_guard<std::mutex> guard(tsdb->m_lock);
        //WriteLock guard(tsdb->m_lock);

        tsdb->inc_ref_count();

        if (tsdb->m_ref_count <= 1)
        {
            TimeRange range = tsdb->get_time_range();

            tsdb->m_index_file.ensure_open(false);
            Logger::info("[rollup] Found this tsdb to rollup: %T", tsdb);

            // perform rollup
            try
            {
                bool success = true;
                int i = 0;
                size_t size = tsdb->m_metrics.size();
                Timestamp delay_ms =
                    Config::inst()->get_time(CFG_TSDB_ROLLUP_DELAY, TimeUnit::MS, CFG_TSDB_ROLLUP_DELAY_DEF);

                //for (auto metric: tsdb->m_metrics)
                while (size > 0)
                {
                    Metric *metric = tsdb->m_metrics[i];
                    ASSERT(metric != nullptr);

                    if (! metric->rollup(&tsdb->m_index_file, tsdb->get_rollup_entries()))
                    {
                        success = false;
                        Logger::debug("[rollup] Metric::rollup(%u) failed", metric->get_id());
                        break;
                    }

                    if (size <= ++i)
                        break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms/size));
                }

                if (success)
                {
                    // mark it as rolled-up
                    std::lock_guard<std::mutex> guard(tsdb->m_lock);
                    tsdb->m_mode |= TSDB_MODE_ROLLED_UP;
                    tsdb->add_config("rolled_up", "true");
                    data.integer = 1;
                    Logger::info("[rollup] 1 Tsdb rolled-up");
                }
            }
            catch (const std::exception& ex)
            {
                Logger::error("[rollup] rollup failed: %s", ex.what());
            }
            catch (...)
            {
                Logger::error("[rollup] rollup failed for unknown reasons");
            }
        }
        else
        {
            Logger::debug("[rollup] Tsdb busy, not rolling up. ref = %d", tsdb->m_ref_count);
        }

        tsdb->m_mode |= TSDB_MODE_READ; // allowing rotate() to archive it
        tsdb->dec_ref_count();
    }
    else
    {
        Logger::info("[rollup] Did not find any appropriate Tsdb to rollup.");
    }

    return false;
}

void
Tsdb::write_to_compacted(MetricId mid, QuerySuperTask& super_task, Tsdb *compacted, PageSize& next_size)
{
    ASSERT(compacted != nullptr);

    for (QueryTask *task : super_task.get_tasks())
    {
        TimeSeriesId id = task->get_ts_id();
        compacted->inc_ref_count();
        PageInMemory page(mid, id, compacted, false, next_size);
        DataPointVector& dps = task->get_dps();

        for (auto dp: dps)
        {
            const Timestamp tstamp = dp.first;
            bool ok = page.add_data_point(tstamp, dp.second);

            if (! ok)
            {
                next_size = page.flush(id, true);
                ASSERT(next_size > 0);
                page.init(mid, id, nullptr, false, next_size);
                ok = page.add_data_point(tstamp, dp.second);
                ASSERT(ok);
            }
        }

        if (! page.is_empty())
        {
            next_size = page.flush(id, true);
            ASSERT(next_size > 0);
        }

        task->recycle();    // release memory
    }
}

void
Tsdb::set_crashes(Tsdb *oldest_tsdb)
{
    PThread_ReadLock guard(&m_tsdb_lock);

    for (auto it = m_tsdbs.rbegin(); it != m_tsdbs.rend(); it++)
    {
        Tsdb *tsdb = *it;
        tsdb->m_mode |= TSDB_MODE_CRASHED;
        if (tsdb == oldest_tsdb) break;
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
