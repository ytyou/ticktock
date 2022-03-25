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

#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "global.h"
#include "append.h"
#include "utils.h"
#include "config.h"
#include "logger.h"
#include "fd.h"
#include "meter.h"
#include "http.h"
#include "timer.h"


namespace tt
{


bool AppendLog::m_enabled = false;
static thread_local AppendLog *instance = nullptr;


AppendLog::AppendLog() :
    m_file(nullptr),
    m_time(0L)
{
    reopen();
}

AppendLog::~AppendLog()
{
    close();
}

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

    // Schedule tasks to rotate append logs.
    task.doit = &AppendLog::rotate;
    freq_sec = Config::get_time(CFG_APPEND_LOG_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_APPEND_LOG_ROTATION_FREQUENCY_DEF);
    ASSERT(freq_sec > 0);
    Timer::inst()->add_task(task, freq_sec, "append_log_rotate");
    Logger::info("using %s of %ds", CFG_APPEND_LOG_ROTATION_FREQUENCY, freq_sec);
}

// Return the thread_local singleton instance.
AppendLog *
AppendLog::inst()
{
    return (instance == nullptr) ? (instance = new AppendLog) : instance;
}

bool
AppendLog::close(TaskData& data)
{
    AppendLog *log = AppendLog::inst();
    Logger::trace("Closing append log %p", log->m_file);
    log->close();
    return false;
}

bool
AppendLog::flush(TaskData& data)
{
    AppendLog *log = AppendLog::inst();
    Logger::trace("Flushing append log %p", log->m_file);

    if (log->m_file != nullptr)
    {
        fflush(log->m_file);
    }

    log->reopen();  // try to rotate, if necessary

    return false;
}

bool
AppendLog::flush_all(TaskData& data)
{
    if (m_enabled && (http_server_ptr != nullptr))
    {
        // Ask HttpServer to broadcast instruction to all its listener threads
        // to flush their (thread local) append logs.
        http_server_ptr->instruct0(PIPE_CMD_FLUSH_APPEND_LOG, strlen(PIPE_CMD_FLUSH_APPEND_LOG));
    }

    return false;
}

bool
AppendLog::rotate(TaskData& data)
{
    int retention_count = Config::get_int(CFG_APPEND_LOG_RETENTION_COUNT, CFG_APPEND_LOG_RETENTION_COUNT_DEF);
    int listener_count = Config::get_int(CFG_HTTP_LISTENER_COUNT, CFG_HTTP_LISTENER_COUNT_DEF);
    int responder_count = Config::get_int(CFG_HTTP_RESPONDERS_PER_LISTENER, CFG_HTTP_RESPONDERS_PER_LISTENER_DEF);

    Logger::debug("retention_count = %d, listener_count = %d, responder_count = %d",
        retention_count, listener_count, responder_count);

    ASSERT(retention_count > 0);
    ASSERT(listener_count > 0);
    ASSERT(responder_count > 0);

    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string log_pattern = append_dir + "/append.*.log.zip";

    int cnt = rotate_files(log_pattern, retention_count * listener_count * responder_count);

    if (cnt > 0)
    {
        Logger::info("Purged %d append logs, retained %d",
            cnt, retention_count * listener_count * responder_count);
    }

    return false;
}

void
AppendLog::close()
{
    if (m_file != nullptr)
    {
        // flush zlib
        unsigned char in[1], out[128];
        in[0] = '\n';
        m_stream.avail_in = 1;
        m_stream.next_in = in;
        do
        {
            m_stream.avail_out = sizeof(out);
            m_stream.next_out = out;
            int ret = deflate(&m_stream, Z_FINISH);
            ASSERT(ret != Z_STREAM_ERROR);
            unsigned int bytes = sizeof(out) - m_stream.avail_out;
            ret = fwrite(out, 1, bytes, m_file);
            ASSERT(ret == bytes);
        } while (m_stream.avail_out == 0);
        (void)deflateEnd(&m_stream);

        fflush(m_file);
        fclose(m_file);
        m_file = nullptr;
    }
}

void
AppendLog::reopen()
{
    if (! m_enabled)
    {
        Logger::debug("append log disabled in the config");
        return;
    }

    int rotation_sec =
        Config::get_time(CFG_APPEND_LOG_ROTATION_FREQUENCY, TimeUnit::SEC, CFG_APPEND_LOG_ROTATION_FREQUENCY_DEF);
    long time = (ts_now_sec() / rotation_sec) * rotation_sec;

    if (time == m_time) return;
    m_time = time;

    close();    // close before open again

    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);

    if (! append_dir.empty())
    {
        // zlib can't append to existing compressed files, so let's make sure
        // that we create a new file.
        std::string log_file = append_dir + "/append." + std::to_string(time) + "." + g_thread_id + ".log.zip";

        for (int i = 0; i < 1024; i++)  // should be able to find one within 1024 tries
        {
            if (! file_exists(log_file)) break;
            log_file = append_dir + "/append." + std::to_string(time) + "." + g_thread_id + "." + std::to_string(i) + ".log.zip";
        }

        //m_file = fopen(log_file.c_str(), "a+");

        int fd = open(log_file.c_str(), O_APPEND|O_CREAT|O_RDWR|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

        if (fd >= 0)
            m_file = fdopen(fd, "a+");
        else
            m_file = nullptr;

        if (m_file == nullptr)
        {
            Logger::error("Failed to open append log file %s for writing: %d",
                log_file.c_str(), errno);
        }
        else
        {
            Logger::info("Writing to append log: %s", log_file.c_str());
        }
    }

    // initialize zlib
    m_stream.zalloc = Z_NULL;
    m_stream.zfree = Z_NULL;
    m_stream.opaque = Z_NULL;

    int ret = deflateInit(&m_stream, Z_DEFAULT_COMPRESSION);

    if (ret != Z_OK)
    {
        Logger::error("Failed to initialize zlib: ret = %d", ret);
    }
}

void
AppendLog::append(char *data, size_t size)
{
    if (! m_enabled) return;

    if ((m_file != nullptr) && (data != nullptr) && (size >= 0))
    {
        unsigned char buff[size];

        // compress and write
        m_stream.avail_in = size;
        m_stream.next_in = (unsigned char*)data;

        do
        {
            m_stream.avail_out = size;
            m_stream.next_out = buff;

            int ret = deflate(&m_stream, Z_SYNC_FLUSH);
            ASSERT(ret != Z_STREAM_ERROR);

            unsigned int bytes = size - m_stream.avail_out;
            ret = fwrite(buff, 1, bytes, m_file);

            if (ret != bytes)
            {
                Logger::error("write() to append log failed, %d, %d", ret, bytes);
                break;
            }
        } while (m_stream.avail_out == 0);

        ASSERT(m_stream.avail_in == 0);
    }
}


}
