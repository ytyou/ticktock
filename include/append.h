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

#pragma once

#include <memory>
#include <cstdio>
#include <mutex>
#include <vector>
#include <stdarg.h>
#include "zlib/zlib.h"
#include "task.h"


namespace tt
{


/* This is used to record all writes from clients. In case of power failure,
 * this log can be used to recover all data that's not flushed to disk before
 * the crash.
 *
 * Zlib, by Jean-loup Gailly and Mark Adler, is used to compress the data
 * stored in these append logs. See zlib.net.
 *
 * This is a per-thread singleton.
 */
class AppendLog
{
public:
    static AppendLog *inst();
    static void init();

    static bool flush(TaskData& data);
    static bool close(TaskData& data);

    // This will be called from Timer periodically to flush append logs
    // of all threads.
    static bool flush_all(TaskData& data);
    static bool rotate(TaskData& data);

    void append(char *data, size_t size);

    // make it non-copyable
    AppendLog(AppendLog const&) = delete;
    AppendLog& operator=(AppendLog const&) = delete;

    virtual ~AppendLog();

private:
    AppendLog();

    void close();
    void reopen();

    void append_internal(char *data, size_t size);

    static bool m_enabled;
    static std::atomic<uint64_t> m_order;

    FILE *m_file;
    Timestamp m_rotate_time;
    z_stream m_stream;
};


}
