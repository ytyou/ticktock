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

#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include "global.h"
#include "utils.h"
#include "config.h"
#include "logger.h"
#include "timer.h"


namespace tt
{


Logger *Logger::m_instance = nullptr;
LogLevel Logger::m_level = LogLevel::UNKNOWN;


Logger::Logger() :
    m_stream(nullptr),
    m_fd(-1)
{
    reopen();

    // if log level was not specified on the command line,
    // we will get it from the config file...
    if (get_level() == LogLevel::UNKNOWN)
    {
        try
        {
            std::string level = Config::get_str(CFG_LOG_LEVEL,CFG_LOG_LEVEL_DEF);
            set_level(level.c_str());
        }
        catch (std::exception& ex)
        {
            fprintf(stderr, "failed to set log level %s\n", ex.what());
        }
    }

    // Schedule tasks to rotate logs.
    Task task;
    task.doit = &Logger::rotate;
    task.data.pointer = (void*)this;
    Timer::inst()->add_task(task, 5, "logger_rotate");  // try every 5 seconds
}

Logger::~Logger()
{
    close();
}

void
Logger::init()
{
    m_instance = new Logger;
}

void
Logger::close()
{
    if (m_stream != nullptr)
    {
        std::fflush(m_stream);
        std::fclose(m_stream);
        m_stream = nullptr;
    }
}

void
Logger::reopen()
{
    close();

    std::string log_file = Config::get_str(CFG_LOG_FILE,CFG_LOG_FILE_DEF);

    if (! log_file.empty() && (log_file != "-"))
    {
        m_fd = open(log_file.c_str(), O_CREAT|O_WRONLY|O_TRUNC|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

        if (m_fd == -1)
        {
            fprintf(stderr, "Failed to open file %s for writing: %d\n",
                log_file.c_str(), errno);
        }
        else
        {
            m_stream = fdopen(m_fd, "w");

            if (m_stream == nullptr)
            {
                fprintf(stderr, "Failed to convert fd %d to FILE: %d\n", m_fd, errno);
            }
            else
            {
                fprintf(stderr, "Writing to log file: %s\n", log_file.c_str());
            }
        }
    }
    else
    {
        fprintf(stderr, "Will log to console\n");
    }
}

bool
Logger::rotate(TaskData& data)
{
    Logger *logger = (Logger*)data.pointer;
    ASSERT(logger != nullptr);

    if (logger->m_fd != -1)
    {
        int limit = Config::get_bytes(CFG_LOG_ROTATION_SIZE, CFG_LOG_ROTATION_SIZE_DEF);

        struct stat buf;
        fstat(logger->m_fd, &buf);
        off_t size = buf.st_size;   // byte count

        if (size >= limit)
        {
            // WARNING: do not log anything after we acquire lock below;
            //          or we will run into deadlock since logging anything
            //          also requires locking;
            std::lock_guard<std::mutex> guard(logger->m_lock);

            // close the file; rename it; and then open a new one;
            logger->close();
            logger->rename();
            logger->reopen();

            // cleanup if necessary
            int retention_count = Config::get_int(CFG_LOG_RETENTION_COUNT, CFG_LOG_RETENTION_COUNT_DEF);
            std::string log_file = Config::get_str(CFG_LOG_FILE, CFG_LOG_FILE_DEF);
            std::string log_pattern = log_file + ".*";
            rotate_files(log_pattern, retention_count);
        }
        else if (logger->m_stream != nullptr)
        {
            std::fflush(logger->m_stream);
        }
    }

    return false;
}

void
Logger::rename()
{
    long now = ts_now_sec();
    std::string log_file = Config::get_str(CFG_LOG_FILE,CFG_LOG_FILE_DEF);
    std::string new_file = log_file + "." + std::to_string(now);

    if (std::rename(log_file.c_str(), new_file.c_str()))
    {
        fprintf(stderr, "Failed to rename %s to %s\n", log_file.c_str(), new_file.c_str());
    }
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::trace(const char *format, ...)
{
    if ((get_level() > LogLevel::TRACE) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::TRACE, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::debug(const char *format, ...)
{
    if ((get_level() > LogLevel::DEBUG) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::DEBUG, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::tcp(const char *format, ...)
{
    if ((get_level() > LogLevel::TCP) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::TCP, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::http(const char *format, ...)
{
    if ((get_level() > LogLevel::HTTP) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::HTTP, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::info(const char *format, ...)
{
    if ((get_level() > LogLevel::INFO) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::INFO, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::warn(const char *format, ...)
{
    if ((get_level() > LogLevel::WARN) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::WARN, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::error(const char *format, ...)
{
    if ((get_level() > LogLevel::ERROR) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::ERROR, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::fatal(const char *format, ...)
{
    if ((get_level() > LogLevel::FATAL) || (format == nullptr))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::FATAL, format, args);

    va_end(args);
}

const char *
Logger::level_name(const LogLevel level)
{
    switch (level)
    {
        case LogLevel::TRACE:   return "TRACE";
        case LogLevel::DEBUG:   return "DEBUG";
        case LogLevel::ERROR:   return "ERROR";
        case LogLevel::FATAL:   return "FATAL";
        case LogLevel::HTTP:    return "HTTP";
        case LogLevel::INFO:    return "INFO";
        case LogLevel::TCP:     return "TCP";
        case LogLevel::WARN:    return "WARN";
        default:                return "UNKNOWN";
    }
}

void
Logger::set_level(const char *level)
{
    LogLevel logLevel = LogLevel::INFO; // default

    if (level != nullptr)
    {
        switch (*level)
        {
            case 'd':
            case 'D':
                logLevel = LogLevel::DEBUG;
                break;
            case 'e':
            case 'E':
                logLevel = LogLevel::ERROR;
                break;
            case 'f':
            case 'F':
                logLevel = LogLevel::FATAL;
                break;
            case 'h':
            case 'H':
                logLevel = LogLevel::HTTP;
                break;
            case 'i':
            case 'I':
                logLevel = LogLevel::INFO;
                break;
            case 't':
            case 'T':
                logLevel = (level[1]=='c'||level[1]=='C') ? LogLevel::TCP : LogLevel::TRACE;
                break;
            case 'w':
            case 'W':
                logLevel = LogLevel::WARN;
                break;
            default:
                logLevel = LogLevel::UNKNOWN;
                break;
        }
    }

    set_level(logLevel);
}

void
Logger::prepare_header(char *buff, int size, const LogLevel level, const char *format)
{
    time_t sec;
    unsigned int msec;

    ts_now(sec, msec);

    struct tm timeinfo;
    localtime_r(&sec, &timeinfo);
    std::strftime(buff, size, "%Y-%m-%d %H:%M:%S", &timeinfo);
    sprintf(buff+19, ".%03d [%s] [%s] %s",
        msec, level_name(level), g_thread_id.c_str(), format);
}

// format parameter is guaranteed to be non-NULL
void
Logger::print(const LogLevel level, const char *format, va_list args)
{
    size_t len = std::strlen(format);
    char fmt[len + m_max_level_len];

    prepare_header(fmt, sizeof(fmt), level, format);

    // append std::endl, if not already there
    if (format[len-1] != '\n')
    {
        std::strcat(fmt, "\n");
    }

    // TODO: Place the content in a queue and have a dedicated
    //       thread to do logging, using aio_write().
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_stream == nullptr)
    {
        // log to console
        std::vprintf(fmt, args);
    }
    else
    {
        // log to file
        std::vfprintf(m_stream, fmt, args);
    }
}


}
