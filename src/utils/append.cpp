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
    m_enabled = Config::get_bool(CFG_APPEND_LOG_ENABLED, CFG_APPEND_LOG_ENABLED_DEF);
    if (! m_enabled) return;

    // Schedule tasks to flush append logs.
    Task task;
    task.doit = &AppendLog::flush_all;
    int freq_sec = Config::get_time(CFG_APPEND_LOG_FLUSH_FREQUENCY, TimeUnit::SEC, CFG_APPEND_LOG_FLUSH_FREQUENCY_DEF);
    ASSERT(freq_sec > 0);
    Timer::inst()->add_task(task, freq_sec, "append_log_flush");
    Logger::info("using %s of %ds", CFG_APPEND_LOG_FLUSH_FREQUENCY, freq_sec);
}

bool
AppendLog::flush_all(TaskData& data)
{
    if (g_shutdown_requested)
        return false;

    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
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
        for (auto ts: tsv) ts->append(file);
    }

    fflush(file);
    fclose(file);

    // append.tmp => append.log
    rm_file(log_name);
    if (std::rename(tmp_name.c_str(), log_name.c_str()) != 0)
        Logger::error("Failed to rename %s to %s", tmp_name.c_str(), log_name.c_str());

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
    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";
    std::lock_guard<std::mutex> guard(m_lock);

    rm_file(tmp_name);
    rm_file(log_name);
}

bool
AppendLog::restore_needed()
{
    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";

    return file_exists(tmp_name) || file_exists(log_name);
}

void
AppendLog::restore(std::vector<TimeSeries*>& tsv)
{
    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string tmp_name = append_dir + "/append.tmp";
    std::string log_name = append_dir + "/append.log";
    std::string name = file_exists(log_name) ? log_name : tmp_name;

    size_t header_size = sizeof(struct append_log_entry);
    char *buff = MemoryManager::alloc_network_buffer();
    FILE *file = fopen(name.c_str(), "rb");

    for ( ; ; )
    {
        size_t size = ::fread(buff, header_size, 1, file);
        if (size < header_size) break;

        TimeSeriesId id = ((struct append_log_entry*)buff)->id;
        Timestamp tstamp = ((struct append_log_entry*)buff)->tstamp;
        PageSize offset = ((struct append_log_entry*)buff)->offset;
        uint8_t start = ((struct append_log_entry*)buff)->start;
        uint8_t is_ooo = ((struct append_log_entry*)buff)->is_ooo;

        if (tsv.size() <= id)
        {
            Logger::error("Time series %u in append log, but not present in meta file", id);
            continue;
        }

        TimeSeries *ts = tsv[id];
        ASSERT(ts != nullptr);

        size = ::fread(buff, offset, 1, file);

        if (size < offset)
        {
            Logger::error("Truncated append log, ts %u not restored", id);
            break;
        }

        Tsdb *tsdb = Tsdb::inst(tstamp, false);

        if (tsdb == nullptr)
        {
            Logger::error("Can't recover time series %u, tstamp %lu not exist", id, tstamp);
            continue;
        }

        ts->restore(tsdb, offset, start, buff, (is_ooo==(uint8_t)1));
    };

    if (file != nullptr)
        fclose(file);

    rm_file(tmp_name);
    MemoryManager::free_network_buffer(buff);
}


}
