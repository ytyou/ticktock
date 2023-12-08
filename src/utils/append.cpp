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

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "global.h"
#include "append.h"
#include "config.h"
#include "fd.h"
#include "logger.h"
#include "memmgr.h"
#include "page.h"
#include "timer.h"
#include "tsdb.h"


namespace tt
{


std::mutex AppendLog::m_lock;
bool AppendLog::m_enabled = false;


void
AppendLog::init()
{
    m_enabled = Config::inst()->get_bool(CFG_APPEND_LOG_ENABLED, CFG_APPEND_LOG_ENABLED_DEF);
    if (! m_enabled) return;

    // Schedule tasks to flush append logs.
    Task task;
    task.doit = &AppendLog::flush_all;
    int freq_sec = Config::inst()->get_time(CFG_APPEND_LOG_FLUSH_FREQUENCY, TimeUnit::SEC, CFG_APPEND_LOG_FLUSH_FREQUENCY_DEF);
    ASSERT(freq_sec > 0);
    Timer::inst()->add_task(task, freq_sec, "append_log_flush");
    Logger::info("using %s of %ds", CFG_APPEND_LOG_FLUSH_FREQUENCY, freq_sec);
}

bool
AppendLog::flush_all(TaskData& data)
{
    if (g_shutdown_requested)
        return false;

    std::string append_dir = Config::get_wal_dir();
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";
    std::lock_guard<std::mutex> guard(m_lock);
    FILE *file = open(tmp_name);

    if (file == nullptr)
    {
        Logger::error("failed to open %s for writing", tmp_name.c_str());
        return false;
    }

    std::vector<Mapping*> mappings;
    Tsdb::get_all_mappings(mappings);

    for (auto mapping: mappings)
    {
        std::vector<TimeSeries*> tsv;
        mapping->get_all_ts(tsv);
        for (auto ts: tsv) ts->append(mapping->get_id(), file);
    }

    fflush(file);
    fclose(file);

    // append.tmp => append.log
    rm_file(log_name);
    if (std::rename(tmp_name.c_str(), log_name.c_str()) != 0)
        Logger::error("Failed to rename %s to %s", tmp_name.c_str(), log_name.c_str());

    MetaFile::instance()->flush();
    return false;
}

FILE *
AppendLog::open(std::string& name)
{
    FILE *file = nullptr;
    int fd = ::open(name.c_str(), O_APPEND|O_CREAT|O_RDWR|O_TRUNC|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);
    if (fd >= 0) file = fdopen(fd, "wb");
    return file;
}

void
AppendLog::shutdown()
{
    std::string append_dir = Config::get_wal_dir();
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";
    std::lock_guard<std::mutex> guard(m_lock);

    rm_file(tmp_name);
    rm_file(log_name);
}

bool
AppendLog::restore_needed()
{
    std::string append_dir = Config::get_wal_dir();
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";

    return file_exists(tmp_name) || file_exists(log_name);
}

void
AppendLog::restore(std::vector<TimeSeries*>& tsv)
{
    std::string append_dir = Config::get_wal_dir();
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";
    std::string name = file_exists(log_name) ? log_name : tmp_name;

    size_t header_size = sizeof(struct append_log_entry);
    char *buff = MemoryManager::alloc_network_buffer();
    FILE *file = fopen(name.c_str(), "rb");
    Tsdb *oldest_tsdb = nullptr;

    for ( ; ; )
    {
        size_t size = ::fread(buff, header_size, 1, file);
        if (size < 1) break;

        MetricId mid = ((struct append_log_entry*)buff)->mid;
        TimeSeriesId tid = ((struct append_log_entry*)buff)->tid;
        Timestamp tstamp = ((struct append_log_entry*)buff)->tstamp;
        PageSize offset = ((struct append_log_entry*)buff)->offset;
        uint8_t start = ((struct append_log_entry*)buff)->start;
        uint8_t flags = ((struct append_log_entry*)buff)->flags;
        FileIndex file_idx = ((struct append_log_entry*)buff)->file_idx;
        HeaderIndex header_idx = ((struct append_log_entry*)buff)->header_idx;

        int bytes = offset;

        if ((flags & 0x03) == 0)    // version 0 compressor
            bytes *= sizeof(DataPointPair);
        else if (start != 0)
            bytes++;

        if (tsv.size() <= tid)
        {
            Logger::error("Time series %u in append log, but not present in meta file", tid);
            continue;
        }

        TimeSeries *ts = tsv[tid];
        ASSERT(ts != nullptr);

        size = ::fread(buff, bytes, 1, file);

        if (size < 1)
        {
            Logger::error("Truncated append log, ts %u not restored", tid);
            break;
        }

        Tsdb *tsdb = Tsdb::inst(tstamp, false);

        if ((oldest_tsdb == nullptr) || (tsdb->get_time_range().older_than_sec(oldest_tsdb->get_time_range().get_from_sec())))
            oldest_tsdb = tsdb;

        if (tsdb == nullptr)
        {
            Logger::error("Can't recover time series %u, tstamp %" PRIu64 " not exist", tid, tstamp);
            continue;
        }

        ts->restore(tsdb, mid, tstamp, offset, start, (uint8_t*)buff, ((flags&0x80)==0x80), file_idx, header_idx);
    }

    if (file != nullptr)
        fclose(file);

    rm_file(tmp_name);
    MemoryManager::free_network_buffer(buff);

    if (oldest_tsdb != nullptr)
        Tsdb::set_crashes(oldest_tsdb);
}


}
