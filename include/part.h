/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2019-2021  Yongtao You (yongtao.you@gmail.com),
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

#include <deque>
#include <mutex>
#include "dp.h"
#include "config.h"
#include "utils.h"


namespace tt
{


class Tsdb;
class PartitionManager;


// Not to be used by multiple threads!
class PartitionBuffer
{
public:
    PartitionBuffer();
    ~PartitionBuffer();

    bool append(DataPoint& dp);

    inline char *data()
    {
        return m_buff;
    }

    inline int size() const
    {
        return m_size;
    }

    inline int get_buffer_size() const
    {
        return m_buff_size;
    }

    inline void set_size(size_t size)
    {
        ASSERT(size < m_buff_size);
        m_size = size;
        m_buff[m_size] = 0;
    }

    inline bool is_empty() const
    {
        return (m_size == 0);
    }

    inline bool is_full() const
    {
        return ((m_size+m_max_line) > m_buff_size);
    }

    inline void clear()
    {
        m_size = 0;
    }

private:
    char *m_buff;
    int m_size;

    static int m_max_line;
    static int m_buff_size;
};


class BackLog
{
public:
    BackLog(int server_id);
    ~BackLog();

    static void init();
    static bool exists(int server_id);

    inline bool is_open_for_read() const { return m_open_for_read; }
    inline bool is_open_for_append() const { return m_open_for_append; }

    bool read(PartitionBuffer *buffer);     // return false if end-of-file
    bool append(PartitionBuffer *buffer);

    bool open_for_read();
    bool open_for_append();
    void close();
    void remove();

private:
    bool open(std::string& name, const char *mode);
    static void get_buff_files(int server_id, std::vector<std::string>& files);

    int m_server_id;
    FILE *m_file;
    int m_size;     // in bytes
    std::string m_file_name;

    bool m_open_for_read;
    bool m_open_for_append;

    static int m_rotation_size;
};


/* When a new data point arrives, we will serialize it into a circular buffer.
 * The data point will sit in the buffer waiting to be forwarded. There will
 * be a separate thread doing the actual forwarding.
 *
 * If we failed to forward a data point to the remote server, we will append it
 * to a file on disk (similar to an append log). Once the remote server recovers,
 * we backfilled all the data points in those files to the remote server.
 */
class PartitionServer
{
public:
    PartitionServer(int id, std::string address, int tcp_port, int http_port);
    ~PartitionServer();

    // if dp == nullptr, we perform flush()
    bool forward(DataPoint& dp);

    PartitionBuffer *get_thread_local_buffer();
    void set_thread_local_buffer(PartitionBuffer *buffer);
    void submit_buffer(PartitionBuffer *buffer);

    inline bool is_self() const { return m_self; }

    char *c_str(char *buff, size_t len)
    {
        snprintf(buff, len, "%d:%s:%d:%d", m_id, m_address.c_str(), m_tcp_port, m_http_port);
        return buff;
    }

private:
    void do_work();
    void connect();
    bool send(const char *buff, int len);
    void close();
    PartitionBuffer *get_buffer(bool empty);

    int m_id;
    int m_fd;
    int m_tcp_port;
    int m_http_port;
    std::string m_address;

    bool m_self;
    bool m_stop_requested;

    std::mutex m_lock;  // to protect m_buffers
    std::deque<PartitionBuffer*> m_buffers; // empty buffers are at the front
    int m_buff_count;

    BackLog *m_backlog;
    std::thread m_worker;
};


class Partition
{
public:
    Partition(Tsdb *tsdb, PartitionManager *mgr, const char *from, const char *to, std::set<int>& servers);

    bool add_data_point(DataPoint& dp);
    inline bool is_local() const { return m_local; }
    inline bool is_catch_all() const { return m_from.empty(); }
    bool match(const char *metric) const;

private:
    int m_id;
    bool m_local;
    std::string m_from;
    std::string m_to;
    std::vector<PartitionServer*> m_servers;
    Tsdb *m_tsdb;
    PartitionManager *m_mgr;
};


/* There will be exactly one PartitionManager per Tsdb. When a new data point
 * arrives at a Tsdb, it sends the data point to its PartitionManager, which
 * will decide which partition this data point belongs to, and forward it to
 * the Partition. If the local host is part of the Partition, then the Partition
 * will call Tsdb::add() to insert the data in the local host; if there are
 * other hosts in the Partition, it will send the data point to the Forwarder
 * for that host.
 */
class PartitionManager
{
public:
    PartitionManager(Tsdb *tsdb, bool existing);
    ~PartitionManager();

    static void init();

    bool add_data_point(DataPoint& dp);
    bool submit_data_points();

    Partition *get_partition(const char *metric) const;

    inline PartitionServer *get_server(unsigned int id)
    {
        if (id >= m_servers.size()) return nullptr;
        return m_servers[id];
    }

private:
    static std::vector<PartitionServer*> m_servers;
    std::vector<Partition*> m_partitions;
    Tsdb *m_tsdb;
};


}
