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

#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <sys/statvfs.h>
#include "config.h"
#include "global.h"
#include "logger.h"
#include "query.h"
#include "stats.h"
#include "timer.h"
#include "tsdb.h"
#include "utils.h"
#include "memmgr.h"
#include "leak.h"


namespace tt
{


static struct proc_stats g_proc_stats;

std::mutex Stats::m_lock;
DataPoint *Stats::m_dps_head = nullptr;
DataPoint *Stats::m_dps_tail = nullptr;


void
Stats::init()
{
    memset(&g_proc_stats, 0, sizeof(g_proc_stats));

    if (Config::get_bool(CFG_TSDB_SELF_METER_ENABLED, CFG_TSDB_SELF_METER_ENABLED_DEF))
    {
        Task task;
        task.doit = &Stats::inject_metrics;
        int freq_sec = Config::get_time(CFG_STATS_FREQUENCY, TimeUnit::SEC, CFG_STATS_FREQUENCY_DEF);
        Timer::inst()->add_task(task, freq_sec, "stats_inject");
        Logger::info("using stats.frequency.sec of %d", freq_sec);
    }
    else
    {
        Logger::info("Not collecting self stats");
    }
}

bool
Stats::inject_metrics(TaskData& data)
{
    Logger::trace("Enter Stats::inject_metrics");
/*
    // ticktock.metrics.count
    {
        int metrics_count = Tsdb::get_metrics_count();
        DataPoint dp(now, (double)metrics_count);
        dp.add_tag(METRIC_TAG_NAME, "ticktock.metrics.count");
        tsdb->add(dp);
    }

    // ticktock.time_series.count
    {
        int ts_count = Tsdb::get_ts_count();
        DataPoint dp(now, (double)ts_count);
        dp.add_tag(METRIC_TAG_NAME, "ticktock.time_series.count");
        tsdb->add(dp);
    }
*/
    Timestamp now = ts_now();

    // We need g_xmx_mb for memory collection. So it must run periodically.
    collect_proc_stat(now);

    if (Config::get_bool(CFG_TSDB_SELF_METER_ENABLED, CFG_TSDB_SELF_METER_ENABLED_DEF))
    {
        Tsdb *tsdb = Tsdb::inst(now);

        // inject collected metrics in m_dps_head
        inject_internal_metrics(now, tsdb);

        // ticktock.connection.count
        {
            int conn_count = TcpListener::get_active_conn_count();
            DataPoint dp(now, (double)conn_count);
            dp.set_metric("ticktock.connection.count");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.connection.count");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }

        // ticktock.data_point.count
        /*
        {
            int dp_count = Tsdb::get_dp_count();
            DataPoint dp(now, (double)dp_count);
            dp.set_metric("ticktock.data_point.count");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.data_point.count");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }
        */

        // ticktock.mmap_file.count
        {
            int32_t count = PageManager::get_mmap_file_count();
            DataPoint dp(now, (double)count);
            dp.set_metric("ticktock.mmap_file.count");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }

        // ticktock.time_series.count
        {
            int ts_count = Tsdb::get_ts_count();
            DataPoint dp(now, (double)ts_count);
            dp.set_metric("ticktock.time_series.count");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.time_series.count");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }

        // ticktock.page.percent_used
        {
            double page_pctused = tsdb->get_page_percent_used();
            DataPoint dp(now, page_pctused);
            dp.set_metric("ticktock.page.used.percent");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.page.used.percent");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }

        // ticktock.tcp.pending_task.count
        if (tcp_server_ptr != nullptr)
        {
            std::vector<std::vector<size_t>> counts;
            tcp_server_ptr->get_pending_task_count(counts);

            for (int i = 0; i < counts.size(); i++)
            {
                for (int j = 0; j < counts[i].size(); j++)
                {
                    DataPoint dp(now, counts[i][j]);
                    dp.set_metric("ticktock.tcp.pending_task.count");
                    std::string listener = std::to_string(i);
                    std::string responder = std::to_string(j);
                    dp.add_tag("listener", listener.c_str());
                    dp.add_tag("responder", responder.c_str());
                    tsdb->add(dp);
                }
            }
        }

        // ticktock.http.pending_task.count
        if (http_server_ptr != nullptr)
        {
            std::vector<std::vector<size_t>> counts;
            http_server_ptr->get_pending_task_count(counts);

            for (int i = 0; i < counts.size(); i++)
            {
                for (int j = 0; j < counts[i].size(); j++)
                {
                    DataPoint dp(now, counts[i][j]);
                    dp.set_metric("ticktock.http.pending_task.count");
                    std::string listener = std::to_string(i);
                    std::string responder = std::to_string(j);
                    dp.add_tag("listener", listener.c_str());
                    dp.add_tag("responder", responder.c_str());
                    tsdb->add(dp);
                }
            }
        }

        // ticktock.query.pending_task.count
        {
            std::vector<size_t> counts;
            QueryExecutor::get_pending_task_count(counts);

            for (int i = 0; i < counts.size(); i++)
            {
                DataPoint dp(now, counts[i]);
                dp.set_metric("ticktock.query.pending_task.count");
                std::string executor = std::to_string(i);
                dp.add_tag("executor", executor.c_str());
                tsdb->add(dp);
            }
        }

        collect_proc_io(now, tsdb);

        // proc stat metrics (rss, vsize, proc.num_thread) have been collected above.
        write_proc_stat(now, tsdb);

        // memory manager stats
        {
            std::vector<DataPoint> dps;
            MemoryManager::collect_stats(now, dps);
            for (DataPoint& dp: dps) tsdb->add(dp);
        }

#ifdef _LEAK_DETECTION
        // memory leak detection stats
        write_leak_stat(now, tsdb);
#endif
    }

    MemoryManager::log_stats();
    return false;
}

void
Stats::add_data_point(DataPoint *dp)
{
    ASSERT(dp != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_dps_tail == nullptr)
    {
        m_dps_head = m_dps_tail = dp;
    }
    else
    {
        m_dps_tail->next() = (Recyclable*)dp;
        m_dps_tail = dp;
    }
    m_dps_tail->next() = nullptr;
}

// We assume internal metrics do NOT have any tags!
void
Stats::inject_internal_metrics(Timestamp ts, Tsdb *tsdb)
{
    DataPoint *dps = nullptr;

    // remove from list
    {
        std::lock_guard<std::mutex> guard(m_lock);
        dps = m_dps_head;
        m_dps_head = m_dps_tail = nullptr;
    }

    std::unordered_map<const char*,DataPoint*,hash_func,eq_func> map;

    for (DataPoint *dp = dps; dp != nullptr; dp = (DataPoint*)dp->next())
    {
        const char *metric = dp->get_metric();

        auto search = map.find(metric);
        if (search == map.end())
        {
            map.insert({metric, dp});
            continue;
        }

        // aggregate
        if (ends_with(metric, ".cnt") || ends_with(metric, ".count") ||
            (std::strstr(metric, ".total.") != nullptr))
        {
            // sum
            search->second->set_value(search->second->get_value() + dp->get_value());
        }
        else
        {
            // max
            if (dp->get_value() > search->second->get_value())
                search->second->set_value(dp->get_value());
        }
    }

    for (const auto& kv : map)
    {
        DataPoint *dp = kv.second;
        ASSERT(dp->get_metric() != nullptr);
        dp->set_timestamp(ts);
        dp->add_tag(HOST_TAG_NAME, g_host_name.c_str());
        tsdb->add(*(dp));
    }

    if (dps != nullptr) MemoryManager::free_recyclables(dps);
}

/* format of /proc/self/io:
rchar: 1948
wchar: 0
syscr: 7
syscw: 0
read_bytes: 0
write_bytes: 0
cancelled_write_bytes: 0
*/
void
Stats::collect_proc_io(Timestamp tstamp, Tsdb *tsdb)
{
    ASSERT(tsdb != nullptr);
    std::ifstream stat_file("/proc/self/io");

    if (stat_file.is_open())
    {
        uint64_t read_bytes = -1;
        uint64_t write_bytes = -1;

        for (std::string line; std::getline(stat_file, line); )
        {
            if (starts_with(line.c_str(), "read"))
            {
                read_bytes = std::stoull(line.c_str() + 12);
            }
            else if (starts_with(line.c_str(), "write"))
            {
                write_bytes = std::stoull(line.c_str() + 13);
            }
        }

        stat_file.close();

        if (read_bytes >= 0)
        {
            DataPoint dp(tstamp, (double)read_bytes);
            dp.set_metric("ticktock.io.read_bytes");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.io.read_bytes");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }

        if (write_bytes >= 0)
        {
            DataPoint dp(tstamp, (double)write_bytes);
            dp.set_metric("ticktock.io.write_bytes");
            //dp.add_tag(METRIC_TAG_NAME, "ticktock.io.write_bytes");
            dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
            tsdb->add(dp);
        }
    }
}

void
Stats::collect_proc_stat(Timestamp tstamp)
{
    std::ifstream stat_file("/proc/self/stat");

    if (stat_file.is_open())
    {
        std::string line;
        std::getline(stat_file, line);
        stat_file.close();

        // 18105 (tt) S 18103 18103 5237 34816 18103 1077944320 2408 0 3 0 24 29 0 0 20 0 10 0 2920753 815054848 3148 18446744073709551615 4194304 4891950 140732293981024 140732293979872 139924451311149 0 0 0 1026 18446744073709551615 0 0 17 1 0 0 0 0 0 6991232 6993236 27054080 140732293985885 140732293985908 140732293985908 140732293988337 0

        // parse the line
        int rc = std::sscanf(line.c_str(),
            "%d %s %c %d %d %d %d %d %u %lu %lu %lu %lu %lu %lu %ld %ld %ld %ld %ld %ld %llu %lu %ld %lu",
            &g_proc_stats.pid, g_proc_stats.comm, &g_proc_stats.state,
            &g_proc_stats.ppid, &g_proc_stats.pgrp, &g_proc_stats.session,
            &g_proc_stats.tty_nr, &g_proc_stats.tpgid, &g_proc_stats.flags,
            &g_proc_stats.minflt, &g_proc_stats.cminflt, &g_proc_stats.majflt,
            &g_proc_stats.cmajflt, &g_proc_stats.utime, &g_proc_stats.stime,
            &g_proc_stats.cutime, &g_proc_stats.cstime, &g_proc_stats.priority,
            &g_proc_stats.nice, &g_proc_stats.num_threads, &g_proc_stats.itrealvalue,
            &g_proc_stats.starttime, &g_proc_stats.vsize, &g_proc_stats.rss,
            &g_proc_stats.rsslim);
        ASSERT(rc == 25);
    }
}

void
Stats::write_proc_stat(Timestamp tstamp, Tsdb *tsdb)
{
    {
        DataPoint dp(tstamp, (double)(g_proc_stats.rss*g_page_size));
        dp.set_metric("ticktock.memory.rss");
        //dp.add_tag(METRIC_TAG_NAME, "ticktock.memory.rss");
        dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
        tsdb->add(dp);
        Logger::debug("rss = %ld", g_proc_stats.rss);
    }

    {
        DataPoint dp(tstamp, (double)g_proc_stats.vsize);
        dp.set_metric("ticktock.memory.vsize");
        //dp.add_tag(METRIC_TAG_NAME, "ticktock.memory.vsize");
        dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
        tsdb->add(dp);
    }

    {
        DataPoint dp(tstamp, (double)g_proc_stats.num_threads);
        dp.set_metric("ticktock.proc.num_threads");
        //dp.add_tag(METRIC_TAG_NAME, "ticktock.proc.num_threads");
        dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
        tsdb->add(dp);
    }
}

int
Stats::collect_stats(char *buff, int size)
{
    Timestamp now = ts_now();
    std::vector<std::vector<size_t>> counts;
    int len = 0;

    if (tcp_server_ptr != nullptr)
    {
        tcp_server_ptr->get_pending_task_count(counts);

        for (int i = 0; i < counts.size(); i++)
        {
            for (int j = 0; j < counts[i].size(); j++)
            {
                size_t cnt = counts[i][j];
                int l = snprintf(buff+len, size-len,
                    "ticktock.tcp.pending_task.count %" PRIu64 " %d listener=%d responder=%d %s=%s\n",
                    now, (int)cnt, i, j, HOST_TAG_NAME, g_host_name.c_str());

                if (size <= (len + l)) break;
                len += l;
            }
        }
    }

    // HTTP server pending tasks
    if (http_server_ptr != nullptr)
    {
        counts.clear();
        http_server_ptr->get_pending_task_count(counts);

        for (int i = 0; i < counts.size(); i++)
        {
            for (int j = 0; j < counts[i].size(); j++)
            {
                size_t cnt = counts[i][j];
                int l = snprintf(buff+len, size-len,
                    "ticktock.http.pending_task.count %" PRIu64 " %d listener=%d responder=%d %s=%s\n",
                    now, (int)cnt, i, j, HOST_TAG_NAME, g_host_name.c_str());

                if (size <= (len + l)) break;
                len += l;
            }
        }
    }

    Tsdb *tsdb = Tsdb::inst(now, false);

    if ((tsdb != nullptr) && (size > len))
    {
        std::vector<size_t> counts;

        len += snprintf(buff+len, size-len,
            "ticktock.connection.count %" PRIu64 " %d %s=%s\nticktock.time_series.count %" PRIu64 " %d %s=%s\nticktock.page.used.percent %" PRIu64 " %f %s=%s\nticktock.ooo_page.count %" PRIu64 " %d %s=%s\nticktock.timer.pending_task.count %" PRIu64 " %zu %s=%s\n",
            now, TcpListener::get_active_conn_count(), HOST_TAG_NAME, g_host_name.c_str(),
            now, Tsdb::get_ts_count(), HOST_TAG_NAME, g_host_name.c_str(),
            now, tsdb->get_page_percent_used(), HOST_TAG_NAME, g_host_name.c_str(),
            now, Tsdb::get_page_count(true), HOST_TAG_NAME, g_host_name.c_str(),
            now, Timer::inst()->m_scheduler.get_pending_task_count(counts), HOST_TAG_NAME, g_host_name.c_str());
    }

    if ((0 < len) && (len < size))
        buff[len] = 0;
    return len;
}

// Return our current memory usage (RSS) in MB
uint64_t
Stats::get_rss_mb()
{
    return ((uint64_t)g_proc_stats.rss * (uint64_t)g_page_size) / (uint64_t)ONE_MEGABYTES;
}

uint64_t
Stats::get_disk_avail()
{
    struct statvfs buff;
    std::string data_dir = Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF);
    int rc = statvfs(data_dir.c_str(), &buff);
    if (rc != 0) return -1;
    return ((uint64_t)buff.f_bsize * (uint64_t)buff.f_bavail);
}

#ifdef _LEAK_DETECTION

void
Stats::write_leak_stat(Timestamp tstamp, Tsdb *tsdb)
{
    ASSERT(tsdb != nullptr);

    {
        DataPoint dp(tstamp, (double)ld_stats(nullptr));
        dp.set_metric("ticktock.leak.total");
        //dp.add_tag(METRIC_TAG_NAME, "ticktock.leak.total");
        dp.add_tag(HOST_TAG_NAME, g_host_name.c_str());
        tsdb->add(dp);
    }
}

#endif


}
