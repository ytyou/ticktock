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

#include <mutex>
#include "dp.h"
#include "config.h"
#include "utils.h"


namespace tt
{


class Tsdb;
class PartitionManager;


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

    bool forward(DataPoint& dp);

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

    bool dump(char *buff, int len);    // FOR DEBUGGING ONLY

    int m_id;
    int m_tcp_port;
    int m_http_port;
    std::string m_address;

    bool m_self;
    bool m_stop_requested;
    bool m_running_late;

    int m_fd;
    char *m_buff;
    int m_size, m_size1;
    std::atomic<int> m_head, m_tail;

    std::mutex m_lock;
    std::thread m_worker;
};


class Partition
{
public:
    Partition(Tsdb *tsdb, PartitionManager *mgr);

    bool add_data_point(DataPoint& dp);

private:
    int m_id;
    bool m_local;
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
    PartitionManager(Tsdb *tsdb);
    ~PartitionManager();

    bool add_data_point(DataPoint& dp);

    inline PartitionServer *get_server(unsigned int id)
    {
        if (id >= m_servers.size()) return nullptr;
        return m_servers[id];
    }

private:
    std::vector<PartitionServer*> m_servers;
    std::vector<Partition*> m_partitions;
    Tsdb *m_tsdb;
};


}
