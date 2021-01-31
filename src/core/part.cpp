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

#include <errno.h>
#include <thread>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "logger.h"
#include "memmgr.h"
#include "part.h"
#include "tcp.h"
#include "tsdb.h"
#include "json.h"
#include "utils.h"


namespace tt
{


PartitionServer::PartitionServer(int id, std::string address, int tcp_port, int http_port) :
    m_id(id),
    m_address(address),
    m_tcp_port(tcp_port),
    m_http_port(http_port),
    m_fd(-1),
    m_head(0),
    m_tail(0),
    m_stop_requested(false)
{
    m_self = is_my_ip(address);
    m_buff = MemoryManager::alloc_network_buffer();
    m_size = MemoryManager::get_network_buffer_size();
    m_size1 = m_size - Config::get_int(CFG_TSDB_MAX_DP_LINE, CFG_TSDB_MAX_DP_LINE_DEF);

    // TODO: allow more than one thread?
    m_worker = std::thread(&PartitionServer::do_work, this);
}

PartitionServer::~PartitionServer()
{
    m_stop_requested = true;

    if (m_worker.joinable()) m_worker.join();

    if (m_buff != nullptr)
    {
        MemoryManager::free_network_buffer(m_buff);
        m_buff = nullptr;
    }

    close();
}

bool
PartitionServer::forward(DataPoint& dp)
{
    std::lock_guard<std::mutex> guard(m_lock);

    int head = m_head;
    int tail = m_tail;

    if (head <= tail)
    {
        int n = snprintf(&m_buff[tail], m_size-tail, "put %s %lu %.10f %s\n",
            dp.get_metric(), dp.get_timestamp(), dp.get_value(), dp.get_raw_tags());
        ASSERT(n < (m_size-tail));
        tail += n;

        if (tail >= m_size1)
        {
            if (tail == m_size1)
                tail = 0;
            else
            {
                int len = tail - m_size1;
                if ((len+1) >= head) return false;  // full
                memcpy(m_buff, &m_buff[m_size1], len);
                tail = len;
            }
        }
    }
    else
    {
        int n = snprintf(&m_buff[tail], head-tail-1, "put %s %lu %.10f %s\n",
            dp.get_metric(), dp.get_timestamp(), dp.get_value(), dp.get_raw_tags());
        if (n >= (head-tail-1)) return false; // full
        tail += n;
    }

    m_tail = tail;
    return true;
}

void
PartitionServer::connect()
{
    struct sockaddr_in addr;

    m_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (m_fd == -1)
    {
        Logger::warn("socket() failed, errno = %d", errno);
        return;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(m_tcp_port);
    inet_pton(AF_INET, m_address.c_str(), &addr.sin_addr.s_addr);

    int retval = ::connect(m_fd, (struct sockaddr *)&addr, sizeof(addr));

    if (retval == -1)
    {
        Logger::warn("connect() failed, errno = %d\n", errno);
        ::close(m_fd);
        m_fd = -1;
    }
}

bool
PartitionServer::send(const char *buff, int len)
{
    if (len <= 0) return true;

    int sent_total = 0;

    while (sent_total < len)
    {
        int sent = ::send(m_fd, buff+sent_total, len-sent_total, 0);

        if (sent == -1)
        {
            Logger::warn("send() failed, errno = %d", errno);
            return false;
        }

        sent_total += sent;
    }

    return true;
}

bool
PartitionServer::dump(char *buff, int len)
{
    if (len <= 0) return true;
    char last = buff[len];
    buff[len] = 0;
    Logger::info("%s", buff);
    buff[len] = last;
    return true;
}

void
PartitionServer::do_work()
{
    unsigned int k = 0;
    g_thread_id = "part_forwarder";

    while (! m_stop_requested)
    {
        int head = m_head;
        int tail = m_tail;

        if (head == tail)   // empty?
            spin_yield(k++);
        else
        {
            try
            {
                if (m_fd == -1)
                {
                    // try to (re-)connect
                    connect();

                    if (m_fd == -1)
                    {
                        Logger::warn("Can't connect to remote server!");
                        spin_yield(k++);
                        continue;
                    }
                    else
                    {
                        // send DONT_FORWARD message to remote
                        if (! send(DONT_FORWARD, std::strlen(DONT_FORWARD)))
                        {
                            close();
                            Logger::error("failed to send don't forward\n");
                        }
                    }
                }

                if (head < tail)
                {
                    // send m_buff[head..tail)
                    if (! send(&m_buff[head], tail-head)) close();
                    //dump(&m_buff[head], tail-head);
                }
                else
                {
                    // send m_buff[head..size) and m_buff[0..tail)
                    ASSERT(head < m_size1);
                    if (! send(&m_buff[head], m_size1-head)) close();
                    if (! send(m_buff, tail)) close();
                    //dump(&m_buff[head], m_size1-head);
                    //dump(m_buff, tail);
                }

                k = 0;
                m_head = tail;
            }
            catch (...)
            {
                close();
            }
        }
    }
}

void
PartitionServer::close()
{
    if (m_fd != -1)
    {
        ::close(m_fd);
        m_fd = -1;
    }
}


Partition::Partition(Tsdb *tsdb, PartitionManager *mgr) :
    m_id(0),        // TODO: from config
    m_tsdb(tsdb),
    m_mgr(mgr),
    m_local(false)
{
    PartitionServer *svr;

    // TODO: get list of servers for this partition from config
    for (int i = 0; (svr = m_mgr->get_server(i)) != nullptr; i++)
    {
        if (svr->is_self())
            m_local = true;
        else
            m_servers.push_back(svr);
    }
}

bool
Partition::add_data_point(DataPoint& dp)
{
    if (m_local)
    {
        if (! m_tsdb->add(dp))
            return false;
    }

    bool success = true;

    for (auto server: m_servers)
    {
        if (! server->forward(dp))
            success = false;
    }

    if (! success)
    {
        // TODO: save it to a file to be backfilled later
    }

    return true;
}


PartitionManager::PartitionManager(Tsdb *tsdb) :
    m_tsdb(tsdb)
{
    ASSERT(tsdb != nullptr);

    if (Config::exists(CFG_CLUSTER_SERVERS))
    {
        std::string servers = Config::get_str(CFG_CLUSTER_SERVERS);
        char buff[servers.size()+2];
        JsonArray arr;

        servers.copy(buff, sizeof(buff));
        JsonParser::parse_array(buff, arr);

        Logger::info("servers: %s", buff);

        for (auto val: arr)
        {
            int id;
            char *address;
            JsonMap& map = val->to_map();

            // id
            auto search = map.find("id");
            if (search == map.end())
            {
                Logger::error("cluster.servers config missing server id");
                continue;
            }
            id = (int)search->second->to_double();

            // address
            search = map.find("address");
            if (search == map.end())
            {
                Logger::error("cluster.servers config missing server address");
                continue;
            }
            address = search->second->to_string();

            // tcp_port
            int tcp_port = CFG_TCP_SERVER_PORT_DEF;
            search = map.find("tcp_port");
            if (search != map.end())
            {
                tcp_port = (int)search->second->to_double();
            }

            // http_port
            int http_port = CFG_HTTP_SERVER_PORT_DEF;
            search = map.find("http_port");
            if (search != map.end())
            {
                http_port = (int)search->second->to_double();
            }

            m_servers.push_back(new PartitionServer(id, address, tcp_port, http_port));
        }

        JsonParser::free_array(arr);

        // For now, create exactly one partition
        m_partitions.push_back(new Partition(tsdb, this));
    }
    else
        Logger::debug("Cluster is not defined.");
}

PartitionManager::~PartitionManager()
{
    for (auto s: m_servers) delete s;
    m_servers.clear();
    for (auto p: m_partitions) delete p;
    m_partitions.clear();
}

bool
PartitionManager::add_data_point(DataPoint& dp)
{
    ASSERT(! m_partitions.empty());

    // special case: only 1 partition
    if (m_partitions.size() == 1)
        return m_partitions.back()->add_data_point(dp);

    return false;   // not implemented
}


}
