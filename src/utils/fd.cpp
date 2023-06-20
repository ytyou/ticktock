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

#include <fcntl.h>
#include <unistd.h>
#include <sys/resource.h>
#include "fd.h"
#include "config.h"
#include "logger.h"
#include "tcp.h"
#include "type.h"


namespace tt
{


int FileDescriptorManager::m_min_step;
int FileDescriptorManager::m_min_file;
std::atomic<int> FileDescriptorManager::m_min_http;
std::atomic<int> FileDescriptorManager::m_max_tcp;
std::mutex FileDescriptorManager::m_lock;


void
FileDescriptorManager::init()
{
    // Logger not initialized yet, do NOT log.
    m_min_step = Config::inst()->get_int(CFG_TCP_MIN_HTTP_STEP, CFG_TCP_MIN_HTTP_STEP_DEF);
    if (m_min_step < 1) m_min_step = 1;
    m_min_file = 0;
    for (int i = 0; i < LISTENER0_COUNT; i++)
        m_min_file += Config::inst()->get_tcp_listener_count(i) + Config::inst()->get_http_listener_count(i);
    m_min_file = 10 * m_min_file +
        Config::inst()->get_int(CFG_TCP_MIN_FILE_DESCRIPTOR, CFG_TCP_MIN_FILE_DESCRIPTOR_DEF);
    if (m_min_file < 100) m_min_file = 100;
    m_max_tcp = m_min_file;

    struct rlimit limit;
    int rc = getrlimit(RLIMIT_NOFILE, &limit);
    int max_http = 1024;    // default
    if (rc == 0)
    {
        if (limit.rlim_cur == RLIM_INFINITY)
            max_http = 1073741824;  // 1B
        else
            max_http = (int)limit.rlim_cur;
    }
    m_min_http = max_http;
    reduce_min_http(max_http);
}

int
FileDescriptorManager::dup_fd(int fd, FileDescriptorType type)
{
    if (UNLIKELY(fd < 0)) return fd;
    if (UNLIKELY(fd >= m_min_file))
    {
        if (type == FileDescriptorType::FD_FILE)
            return fd;
        Logger::error("fd (%d) >= m_min_file (%d)", fd, m_min_file);
        close(fd);
        return -1;
    }

    int new_fd = -1;

    if (type == FileDescriptorType::FD_TCP)
    {
        if (fd < m_min_file)
            new_fd = fcntl(fd, F_DUPFD_CLOEXEC, m_min_file);
        else
            new_fd = fd;

        if (new_fd >= 0)
        {
            // update m_max_tcp, if necessary
            increase_max_tcp(new_fd);
        }
    }
    else if (type == FileDescriptorType::FD_HTTP)
    {
        for (int min_fd = m_min_http.load(std::memory_order_relaxed); min_fd >= 0; )
        {
            new_fd = fcntl(fd, F_DUPFD_CLOEXEC, min_fd);
            if (new_fd >= 0) break;
            min_fd = reduce_min_http(min_fd);
        }
    }
    else
    {
        new_fd = fd;
        //new_fd = fcntl(fd, F_DUPFD_CLOEXEC, m_min_file);
    }

    if ((new_fd >= 0) && (new_fd != fd))
        close(fd);
    else if (new_fd < 0)
    {
        int max_tcp = m_max_tcp.load(std::memory_order_relaxed);
        int min_http = m_min_http.load(std::memory_order_relaxed);
        Logger::error("Run out of file descriptors, max_tcp=%d, min_http=%d", max_tcp, min_http);
    }

    return new_fd;
}

int
FileDescriptorManager::increase_max_tcp(int fd)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int max_tcp = m_max_tcp.load(std::memory_order_relaxed);
    if (max_tcp < fd) m_max_tcp.store(fd, std::memory_order_relaxed);
    return m_max_tcp.load(std::memory_order_relaxed);
}

int
FileDescriptorManager::reduce_min_http(int fd)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int min_http = m_min_http.load(std::memory_order_relaxed);
    if (min_http < fd) return min_http;
    int max_tcp = m_max_tcp.load(std::memory_order_relaxed);
    if (min_http <= (max_tcp+1))
    {
        m_min_http.store(-1, std::memory_order_relaxed);
        return -1; // out-of-fds
    }
    min_http -= m_min_step;
    if (min_http <= max_tcp) min_http = max_tcp + 1;
    m_min_http.store(min_http, std::memory_order_relaxed);
    return min_http;
}


}
