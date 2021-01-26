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

#include <memory>
#include <cstdio>
#include <mutex>
#include <stdarg.h>
#include "task.h"


namespace tt
{


enum class LogLevel
{
    TRACE = 1,
    DEBUG = 2,
    INFO = 3,
    WARN = 4,
    ERROR = 5,
    FATAL = 6,
    UNKNOWN = 100
};


// This is a singleton.
class Logger
{
public:
    static std::shared_ptr<Logger> inst()
    {
        static std::shared_ptr<Logger> instance {new Logger};
        return instance;
    }

    // std::endl will be appended, if it's not already there
    static void trace(const char *format, ...);
    static void debug(const char *format, ...);
    static void info(const char *format, ...);
    static void warn(const char *format, ...);
    static void error(const char *format, ...);
    static void fatal(const char *format, ...);

    static const char *get_level_as_string(LogLevel level);
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
    void reopen();

    // This will be called from Timer periodically to rotate log files.
    static bool rotate(TaskData& data);

    ~Logger();

private:
    Logger();
    void rename();

    void print(const LogLevel level, const char *format, va_list args);

    static const char* level_name(const LogLevel level);
    static void prepare_header(char *buff, int size, const LogLevel level, const char *format);

    static LogLevel m_level;
    static const int m_max_level_len = 64;    // this includes timestamp

    std::mutex m_lock;
    std::FILE *m_stream;
    int m_fd;
};


}
