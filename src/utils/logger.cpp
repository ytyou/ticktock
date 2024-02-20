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
#include <glob.h>
#include <sys/stat.h>
#include <printf.h>
#include "global.h"
#include "utils.h"
#include "config.h"
#include "logger.h"
#include "fd.h"
#include "serial.h"
#include "timer.h"


namespace tt
{


Logger *Logger::m_instance = nullptr;
std::map<int, Logger*> Logger::m_instances;
LogLevel Logger::m_level = LogLevel::UNKNOWN;


static int
handler_func(FILE *stream, const struct printf_info *info, const void * const *args)
{
    Serializable *s = *((Serializable **) (args[0]));
    ASSERT(s->c_size() > 1);
    char buff[s->c_size()];
    int len = fprintf(stream, "%*s", (info->left ? -info->width : info->width), s->c_str(buff));
    return len;
}

static int
info_func(const struct printf_info *info, size_t n, int *argtypes, int *size)
{
    argtypes[0] = PA_POINTER;
    size[0] = sizeof(void*);
    return 1;
}


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
            std::string level = Config::inst()->get_str(CFG_LOG_LEVEL,CFG_LOG_LEVEL_DEF);
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
    Timer::inst()->add_task(task, 10, "logger_rotate"); // try every 10 seconds
}

Logger::Logger(int fd) :
    m_stream(nullptr),
    m_fd(-1)
{
    reopen(fd);
}

Logger::~Logger()
{
    close();
}

void
Logger::init()
{
    m_instance = new Logger;
    register_printf_specifier('T', handler_func, info_func);
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

Logger *
Logger::get_instance(int fd)
{
    static std::mutex m;

    std::unique_lock<std::mutex> lock(m);
    auto search = m_instances.find(fd);

    if (search != m_instances.end())
        return search->second;
    else    // create one
    {
        Logger *logger = new Logger(fd);
        m_instances[fd] = logger;
        return logger;
    }
}

std::string
Logger::get_log_file(int fd)
{
    std::string log_file;

    if (fd > 0)
    {
        //auto const pos = log_file.find_last_of('/');
        //ASSERT(pos != std::string::npos);
        log_file = Config::get_log_dir();
        log_file += "/conn-";
        log_file += std::to_string(fd);
        log_file += ".log";
    }
    else
        log_file = Config::get_log_file();

    return log_file;
}

void
Logger::reopen(int fd)
{
    close();

    std::string log_file = get_log_file(fd);

    if (! log_file.empty() && (log_file != "-"))
    {
        m_fd = open(log_file.c_str(), O_APPEND|O_CREAT|O_WRONLY|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        m_fd = FileDescriptorManager::dup_fd(m_fd, FileDescriptorType::FD_FILE);

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

    if ((logger->m_fd != -1) && logger->m_dirty.load())
    {
        int limit = Config::inst()->get_bytes(CFG_LOG_ROTATION_SIZE, CFG_LOG_ROTATION_SIZE_DEF);

        struct stat buf;
        fstat(logger->m_fd, &buf);
        off_t size = buf.st_size;   // byte count
        logger->m_dirty = false;

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

            // cleanup main log file, if necessary
            int retention_count = Config::inst()->get_int(CFG_LOG_RETENTION_COUNT, CFG_LOG_RETENTION_COUNT_DEF);
            std::string log_file = Config::get_log_file();
            std::string log_pattern = log_file + ".*";
            rotate_files(log_pattern, retention_count);

            // cleanup per connection log files, if necessary
            /*
            auto pos = log_file.find_last_of('/');
            ASSERT(pos != std::string::npos);
            log_file = log_file.substr(0, pos+1) + "conn-*.log";

            glob_t glob_result;
            glob(log_file.c_str(), GLOB_TILDE, nullptr, &glob_result);
            for (int i = 0; i < glob_result.gl_pathc; i++)
            {
                std::string log(glob_result.gl_pathv[i]);
                log += ".*";
                rotate_files(log, retention_count);
            }
            */
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
    std::string log_file = Config::get_log_file();
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
    if (LIKELY((get_level() > LogLevel::TRACE) || (format == nullptr)))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::TRACE, -1, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::debug(const char *format, ...)
{
    if (LIKELY((get_level() > LogLevel::DEBUG) || (format == nullptr)))
        return;

    va_list args;
    va_start(args, format);

    m_instance->print(LogLevel::DEBUG, -1, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::tcp(const char *format, int fd, ...)
{
    if (LIKELY((get_level() > LogLevel::TCP) || (format == nullptr)))
        return;

    va_list args;
    va_start(args, fd);

    m_instance->print(LogLevel::TCP, fd, format, args);

    va_end(args);
}

// Add a timestamp and a log level to the passed in log entry;
void
Logger::http(const char *format, int fd, ...)
{
    if (LIKELY((get_level() > LogLevel::HTTP) || (format == nullptr)))
        return;

    va_list args;
    va_start(args, fd);

    m_instance->print(LogLevel::HTTP, fd, format, args);

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

    m_instance->print(LogLevel::INFO, -1, format, args);

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

    m_instance->print(LogLevel::WARN, -1, format, args);

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

    m_instance->print(LogLevel::ERROR, -1, format, args);

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

    m_instance->print(LogLevel::FATAL, -1, format, args);

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
Logger::prepare_header(char *buff, int size, const LogLevel level, int fd, const char *format)
{
    time_t sec;
    unsigned int msec;

    ts_now(sec, msec);

    struct tm timeinfo;
    localtime_r(&sec, &timeinfo);
    std::strftime(buff, size, "%Y-%m-%d %H:%M:%S", &timeinfo);

    if (fd < 0)
    {
        sprintf(buff+19, ".%03d [%s] [%s] %s",
            msec, level_name(level), g_thread_id.c_str(), format);
    }
    else
    {
        sprintf(buff+19, ".%03d [%s] [%s] [%d] %s",
            msec, level_name(level), g_thread_id.c_str(), fd, format);
    }
}

// format parameter is guaranteed to be non-NULL
void
Logger::print(const LogLevel level, int fd, const char *format, va_list args)
{
    size_t len = std::strlen(format);
    char fmt[len + m_max_level_len];

    prepare_header(fmt, sizeof(fmt), level, fd, format);

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

    m_dirty = true;
}


}
