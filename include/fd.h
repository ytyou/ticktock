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

#include <atomic>
#include <mutex>


namespace tt
{


enum FileDescriptorType : unsigned char
{
    FD_FILE,
    FD_HTTP,
    FD_TCP
};


/* Because file descriptors can be reused, problems could arise when
 * one type of file descriptors are then being used as a different type.
 * This is because there could be a task sitting in a queue waiting for
 * CPU to process the file descriptor of previous type; when the task is
 * finally being processed, it will cause an error because the file
 * descriptor is now representing something of different type.
 *
 * To prevent this kind of problem, we will always 'assign' the same type
 * of resources to the same range of file descriptors. For example, HTTP
 * connections will always be assigned file descriptors from the last
 * section of all usable file descriptors; TCP connections, and files
 * will all have their own range of file descriptors.
 */
class FileDescriptorManager
{
public:
    static void init();

    // Returns new fd; or -1 when we run out of FDs
    static int dup_fd(int fd, FileDescriptorType type);

private:
    static int increase_max_tcp(int fd);
    static int reduce_min_http(int fd);

    // All files and TCP sockets use [m_min_file, m_min_http];
    // All HTTP sockets use [m_min_http, ...];
    // m_min_http can be dynamically adjusted;
    static int m_min_file;
    static std::atomic<int> m_min_http;
    static int m_min_step;

    static std::atomic<int> m_max_tcp;  // max used so far
    static std::mutex m_lock;
};


}
