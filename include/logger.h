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

#include <map>
#include <memory>
#include <cstdio>
#include <mutex>
#include <stdarg.h>
#include "task.h"


namespace tt
{


enum class LogLevel : unsigned char
{
    TRACE = 1,
    DEBUG = 2,
    TCP = 3,
    HTTP = 4,
    INFO = 5,
    WARN = 6,
    ERROR = 7,
    FATAL = 8,
    UNKNOWN = 100
};


// This is a singleton.
class Logger
{
public:
    static void init();     // must be called before any other methods
    inline static Logger* inst() { return m_instance; }

    // std::endl will be appended, if it's not already there
    static void trace(const char *format, ...);
    static void debug(const char *format, ...);
    static void tcp(const char *format, int fd, ...);
    static void http(const char *format, int fd, ...);
    static void info(const char *format, ...);
    static void warn(const char *format, ...);
    static void error(const char *format, ...);
    static void fatal(const char *format, ...);

    static void set_level(const char *level);

    inline static LogLevel get_level()
    {
        return m_level;
    }

    inline static void set_level(LogLevel level)
    {
        m_level = level;
    }

    // make it not copyable
    Logger(Logger const&) = delete;
    Logger& operator=(Logger const&) = delete;

    void close();
    void reopen(int fd = -1);

    // This will be called from Timer periodically to rotate log files.
    static bool rotate(TaskData& data);

    ~Logger();

private:
    Logger();
    Logger(int fd); // per connection logger
    void rename();

    void print(const LogLevel level, int fd, const char *format, va_list args);

    static Logger *get_instance(int fd);
    static std::string get_log_file(int fd);

    static const char* level_name(const LogLevel level);
    static void prepare_header(char *buff, int size, const LogLevel level, int fd, const char *format);

    static LogLevel m_level;
    static const int m_max_level_len = 74;  // this includes timestamp

    std::mutex m_lock;
    std::FILE *m_stream;
    int m_fd;
    std::atomic<bool> m_dirty;  // true if flush is needed

    static Logger *m_instance;
    static std::map<int, Logger*> m_instances;  // per connection instances
};


}
