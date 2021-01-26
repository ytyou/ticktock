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

#pragma once

#include "dp.h"
#include "task.h"
#include "http.h"
#include "tsdb.h"
#include "type.h"


namespace tt
{


struct proc_stats
{
    int pid;
    char comm[32];
    char state;
    int ppid;
    int pgrp;
    int session;
    int tty_nr;
    int tpgid;
    unsigned int flags;
    unsigned long minflt;
    unsigned long cminflt;
    unsigned long majflt;
    unsigned long cmajflt;
    unsigned long utime;
    unsigned long stime;
    signed long cutime;
    signed long cstime;
    signed long priority;
    signed long nice;
    signed long num_threads;
    signed long itrealvalue;
    unsigned long long starttime;
    unsigned long vsize;
    unsigned long rsslim;
    long rss;
};


class Stats
{
public:
    static void init();
    static long get_rss_mb();

    static bool http_get_api_stats_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_version_handler(HttpRequest& request, HttpResponse& response);

    // This is used to collect metrics until inject_metrics() is called,
    // at which point they will be sent to Tsdb.
    static void add_data_point(DataPoint *dp);

private:
    static bool inject_metrics(TaskData& data);
    static void inject_internal_metrics(Timestamp ts, Tsdb *tsdb);
    static void collect_proc_io(Timestamp tstamp, Tsdb *tsdb);
    static void collect_proc_stat(Timestamp tstamp);
    static void write_proc_stat(Timestamp tstamp, Tsdb *tsdb);
    static void write_leak_stat(Timestamp tstamp, Tsdb *tsdb);

    // internal metrics to be injected into Tsdb by inject_metrics()
    static DataPoint *m_dps_head;
    static DataPoint *m_dps_tail;
    static std::mutex m_lock;   // to protect m_dps_head & m_dps_tail
};


}
