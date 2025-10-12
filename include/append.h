/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include <memory>
#include <cstdio>
#include <mutex>
#include <vector>
#include <stdarg.h>
#include "task.h"


namespace tt
{


class TimeSeries;


/* This is used to read/write WAL, in order to recover from an abnormal termination.
 */
class AppendLog
{
public:
    static void init();
    static FILE *open(std::string& name);

    // This will be called from Timer periodically to generate append log (WAL).
    static bool flush_all(TaskData& data);  // generate a new WAL to replace the existing one
    static void shutdown();     // called during normal shutdown

    static bool restore_needed();   // need to restore from WAL?
    static void restore(std::vector<TimeSeries*>& tsv); // restore from WAL

    // make it non-copyable
    AppendLog(AppendLog const&) = delete;
    AppendLog& operator=(AppendLog const&) = delete;

private:
    static bool m_enabled;
    static std::mutex m_lock;
};


}
