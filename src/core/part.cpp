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
#include <signal.h>
#include <thread>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <glob.h>
#include "logger.h"
#include "memmgr.h"
#include "part.h"
#include "tcp.h"
#include "tsdb.h"
#include "json.h"
#include "utils.h"


namespace tt
{


int BackLog::m_rotation_size = 0;
int PartitionBuffer::m_max_line = 0;
int PartitionBuffer::m_buff_size = 0;
std::map<int,PartitionServer*> PartitionManager::m_servers;

// indexed by PartitionServer::m_id
static thread_local std::vector<PartitionBuffer*> partition_server_forward_buffers;


PartitionBuffer::PartitionBuffer() :
    m_size(0)
{
    m_buff = MemoryManager::alloc_network_buffer();

    if (m_max_line == 0)
        m_max_line = Config::get_int(CFG_TSDB_MAX_DP_LINE, CFG_TSDB_MAX_DP_LINE_DEF);
    if (m_buff_size == 0)
        m_buff_size = MemoryManager::get_network_buffer_size();

}

PartitionBuffer::~PartitionBuffer()
{
    if (m_buff != nullptr)
    {
        MemoryManager::free_network_buffer(m_buff);
        m_buff = nullptr;
    }
}

bool
PartitionBuffer::append(DataPoint& dp)
{
    int n = snprintf(&m_buff[m_size], m_buff_size-m_size, "put %s %lu %.10f %s\n",
        dp.get_metric(), dp.get_timestamp(), dp.get_value(), dp.get_raw_tags());

    if (n >= (m_buff_size-m_size))
    {
        m_buff[m_size] = 0;
        return false;
    }

    m_size += n;
    ASSERT(m_buff[m_size] == 0);
    return true;
}


BackLog::BackLog(int server_id) :
    m_server_id(server_id),
    m_file(nullptr),
    m_size(0),
    m_open_for_read(false),
    m_open_for_append(false)
{
}

BackLog::~BackLog()
{
    close();
}

void
BackLog::init()
{
    m_rotation_size =
        Config::get_bytes(CFG_CLUSTER_BACKLOG_ROTATION_SIZE, CFG_CLUSTER_BACKLOG_ROTATION_SIZE_DEF);
}

bool
BackLog::exists(int server_id)
{
    std::vector<std::string> files;
    get_buff_files(server_id, files);
    return ! files.empty();
}

void
BackLog::get_buff_files(int server_id, std::vector<std::string>& files)
{
    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string log_pattern = append_dir + "/backlog." + std::to_string(server_id)+ ".*.log";
    glob_t glob_result;
    glob(log_pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);
    for (unsigned int i = 0; i < glob_result.gl_pathc; i++)
        files.push_back(std::string(glob_result.gl_pathv[i]));
    globfree(&glob_result);
    if (files.size() > 1)
        std::sort(files.begin(), files.end());
}

bool
BackLog::open(std::string& name, const char *mode)
{
    ASSERT(m_file == nullptr);
    ASSERT(mode != nullptr);

    Logger::debug("BackLog: %s is open for %s", name.c_str(), mode);

    m_file_name = name;
    m_file = fopen(name.c_str(), mode);
    return (m_file != nullptr);
}

bool
BackLog::open_for_read()
{
    std::vector<std::string> files;
    get_buff_files(m_server_id, files);
    if (files.empty()) return false;
    m_open_for_read = open(files.front(), "r");
    return m_open_for_read;
}

bool
BackLog::open_for_append()
{
    // Do we need partition id in the file name?
    Timestamp now = ts_now_sec();
    std::string append_dir = Config::get_str(CFG_APPEND_LOG_DIR);
    std::string name = append_dir + "/backlog." + std::to_string(m_server_id)+ "." + std::to_string(now) + ".log";
    m_open_for_append = open(name, "a");
    m_size = 0;
    return m_open_for_append;
}

void
BackLog::close()
{
    if (m_file != nullptr)
    {
        fclose(m_file);
        m_file = nullptr;
        m_open_for_read = false;
        m_open_for_append = false;
    }
}

void
BackLog::remove()
{
    ASSERT(! m_open_for_read);
    ASSERT(! m_open_for_append);

    if (! m_file_name.empty())
    {
        std::remove(m_file_name.c_str());
        Logger::debug("removing %s", m_file_name.c_str());
    }
}

bool
BackLog::read(PartitionBuffer *buffer)
{
    ASSERT(buffer != nullptr);
    ASSERT(buffer->is_empty());
    ASSERT(m_open_for_read);

    int n = fread(buffer->data(), 1, buffer->get_buffer_size()-1, m_file);
    buffer->set_size(n);
    Logger::trace("backlog read %d bytes", n);
    return true;
}

bool
BackLog::append(PartitionBuffer *buffer)
{
    ASSERT(buffer != nullptr);
    ASSERT(! buffer->is_empty());
    ASSERT(buffer->data()[buffer->size()] == 0);
    ASSERT(m_open_for_append);

    if (m_size >= m_rotation_size)
    {
        close();
        if (! open_for_append())
            return false;
    }

    int n = fprintf(m_file, "%s", buffer->data());
    m_size += n;
    return (n == buffer->size());
}


PartitionServer::PartitionServer(int id, std::string address, int tcp_port, int http_port) :
    m_id(id),
    m_address(address),
    m_tcp_port(tcp_port),
    m_http_port(http_port),
    m_buff_count(0),
    m_backlog(nullptr),
    m_fd(-1),
    m_stop_requested(false)
{
    m_self = is_my_ip(address) && (m_tcp_port == Config::get_int(CFG_TCP_SERVER_PORT, CFG_TCP_SERVER_PORT_DEF));

    // TODO: allow more than one thread?
    m_worker = std::thread(&PartitionServer::do_work, this);
}

PartitionServer::~PartitionServer()
{
    m_stop_requested = true;
    if (m_worker.joinable()) m_worker.join();
    if (m_backlog != nullptr) delete m_backlog;
    close();
}

PartitionBuffer *
PartitionServer::get_thread_local_buffer()
{
    PartitionBuffer *buffer = nullptr;
    if (m_id < partition_server_forward_buffers.size())
        buffer = partition_server_forward_buffers[m_id];
    return buffer;
}

void
PartitionServer::set_thread_local_buffer(PartitionBuffer *buffer)
{
    while (partition_server_forward_buffers.size() <= m_id)
        partition_server_forward_buffers.push_back(nullptr);
    partition_server_forward_buffers[m_id] = buffer;
}

bool
PartitionServer::forward(DataPoint& dp)
{
    PartitionBuffer *buffer = get_thread_local_buffer();

    // try to append
    if (buffer == nullptr)
    {
        {
            std::lock_guard<std::mutex> guard(m_lock);

            for (auto it = m_buffers.begin(); it != m_buffers.end(); it++)
            {
                if (! (*it)->is_full())
                {
                    buffer = (*it);
                    m_buffers.erase(it);
                    break;
                }
            }

            if ((buffer == nullptr) && (m_buff_count < 16))  // TODO: config
            {
                buffer = new PartitionBuffer(); // TODO: make it Recyclable
                m_buff_count++;
            }
            else if (buffer == nullptr)
            {
                return false;
            }
        }

        set_thread_local_buffer(buffer);
    }

    ASSERT(buffer != nullptr);
    return buffer->append(dp);
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
        Logger::warn("connect(%s:%d) failed, errno = %d\n", m_address.c_str(), m_tcp_port, errno);
        ::close(m_fd);
        m_fd = -1;
    }
    else
    {
        // send DONT_FORWARD message to remote
        if (! send(DONT_FORWARD, std::strlen(DONT_FORWARD)))
        {
            Logger::warn("failed to send DONT_FORWARD to (%s:%d)\n", m_address.c_str(), m_tcp_port);
            ::close(m_fd);
            m_fd = -1;
        }
        else
        {
            Logger::info("connected to %s:%d, DONT_FORWARD sent", m_address.c_str(), m_tcp_port);
        }
    }

    if (m_fd != -1)
        Logger::info("connected to %s:%d, fd=%d", m_address.c_str(), m_tcp_port, m_fd);
    else
        Logger::debug("failed to connect to %s:%d", m_address.c_str(), m_tcp_port);
}

bool
PartitionServer::send(const char *buff, int len)
{
    if (len <= 0) return true;

    int sent_total = 0;

    try
    {
        while (sent_total < len)
        {
            int sent = ::send(m_fd, buff+sent_total, len-sent_total, 0);

            if (sent == -1)
            {
                Logger::warn("send() failed, errno = %d", errno);
                close();
                return false;
            }

            sent_total += sent;
        }
    }
    catch (...)
    {
        close();
        Logger::warn("send() failed with an exception");
        return false;
    }

    return (sent_total > 0);
}

PartitionBuffer *
PartitionServer::get_buffer(bool empty)
{
    PartitionBuffer *buffer = nullptr;
    std::lock_guard<std::mutex> guard(m_lock);

    for (auto it = m_buffers.rbegin(); it != m_buffers.rend(); it++)
    {
        if (((*it)->is_empty() && empty) || (!(*it)->is_empty() && !empty))
        {
            buffer = (*it);
            m_buffers.erase((++it).base());
            break;
        }
    }

    if ((buffer == nullptr) && empty && (m_buff_count < 8))
    {
        buffer = new PartitionBuffer(); // TODO: make it Recyclable
        m_buff_count++;
    }

    return buffer;
}

void
PartitionServer::submit_buffer(PartitionBuffer *buffer)
{
    ASSERT(buffer != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);
    m_buffers.push_back(buffer);
}

void
PartitionServer::do_work()
{
    unsigned int k = 0;
    g_thread_id = "part_server_" + std::to_string(m_id);

    // block SIGPIPE, permanently
    sigset_t oldset, newset;
    int retval = sigemptyset(&newset);
    if (retval != 0)
        Logger::warn("sigemptyset() failed, errno = %d", errno);
    else
    {
        retval = sigaddset(&newset, SIGPIPE);
        if (retval != 0)
            Logger::warn("sigaddset() failed, errno = %d", errno);
        else
        {
            retval = pthread_sigmask(SIG_BLOCK, &newset, &oldset);
            if (retval != 0)
                Logger::warn("pthread_sigmask() failed, errno = %d", errno);
        }
    }

    if (BackLog::exists(m_id))
        m_backlog = new BackLog(m_id);

    while (! m_stop_requested)
    {
        PartitionBuffer *buffer = get_buffer(false);    // get non-empty buffer

        if (buffer == nullptr)
        {
            // See if there are any backlogs. If so, try send them here.
            if ((m_backlog != nullptr) && (m_fd != -1))
            {
                if (m_backlog->is_open_for_append())
                    m_backlog->close();

                if (! m_backlog->is_open_for_read())
                    m_backlog->open_for_read();

                if (m_backlog->is_open_for_read())
                {
                    buffer = get_buffer(true);
                    ASSERT(buffer != nullptr);

                    if (m_backlog->read(buffer) && (! buffer->is_empty()))
                    {
                        if (send(buffer->data(), buffer->size()))
                            buffer->clear();
                        else
                        {
                            m_backlog->close();
                            spin_yield(k++);
                        }
                    }
                    else
                    {
                        ASSERT(buffer->is_empty());
                        m_backlog->close();
                        m_backlog->remove();
                    }

                    std::lock_guard<std::mutex> guard(m_lock);
                    m_buffers.push_front(buffer);
                    buffer = nullptr;
                }
                else
                {
                    delete m_backlog;
                    m_backlog = nullptr;
                    spin_yield(k++);
                }
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                if (m_backlog != nullptr) connect();
            }
        }
        else
        {
            if (m_fd == -1)
            {
                // try to (re-)connect
                connect();

                if (m_fd != -1)
                {
                    if ((m_backlog != nullptr) && m_backlog->is_open_for_append())
                        m_backlog->close();
                }
                else
                {
                    Logger::warn("Can't connect to remote server!");
                }
            }

            if (m_fd != -1)
            {
                if (send(buffer->data(), buffer->size()))
                {
                    k = 0;
                    buffer->clear();
                }
            }
            else
            {
                // try to save it to backlog
                if (m_backlog == nullptr)
                {
                    m_backlog = new BackLog(m_id);
                    m_backlog->open_for_append();
                }

                if (m_backlog->append(buffer))
                    buffer->clear();
            }

            std::lock_guard<std::mutex> guard(m_lock);
            m_buffers.push_front(buffer);
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


Partition::Partition(Tsdb *tsdb, PartitionManager *mgr, const char *from, const char *to, std::set<int>& servers) :
    m_id(0),        // TODO: from config
    m_tsdb(tsdb),
    m_mgr(mgr),
    m_from(from),
    m_to(to),
    m_local(false)
{
    PartitionServer *svr;

    for (auto s: servers)
    {
        svr = m_mgr->get_server(s);
        if (svr == nullptr) continue;

        if (svr->is_self())
            m_local = true;
        else
            m_servers.push_back(svr);
    }
}

bool
Partition::match(const char *metric) const
{
    if (m_from.empty() || m_to.empty())
        return true;
    else
        return (m_from <= metric) && (metric < m_to);
}

bool
Partition::add_data_point(DataPoint& dp)
{
    bool success = true;

    for (auto server: m_servers)
    {
        if (server->is_self()) continue;

        if (! server->forward(dp))
        {
            success = false;
            char buff[64];
            Logger::debug("failed to forward to server %s", server->c_str(buff, sizeof(buff)));
        }
    }

    if (! success)
    {
        // TODO: save it to a file to be backfilled later
    }

    return true;
}


PartitionManager::PartitionManager(Tsdb *tsdb, bool existing) :
    m_tsdb(tsdb)
{
    ASSERT(tsdb != nullptr);

    std::string partition_defs;

    if (existing)
        partition_defs = tsdb->get_partition_defs();
    else
        partition_defs = Config::get_str(CFG_CLUSTER_PARTITIONS);

    if (! partition_defs.empty())
    {
        char buff[partition_defs.size()+2];
        JsonArray arr;

        partition_defs.copy(buff, sizeof(buff));
        JsonParser::parse_array(buff, arr);

        for (auto val: arr)
        {
            JsonMap& map = val->to_map();
            char *from = nullptr, *to = nullptr;
            std::set<int> servers;

            // from
            auto search = map.find("from");
            if (search != map.end())
                from = search->second->to_string();

            // to
            search = map.find("to");
            if (search != map.end())
                to = search->second->to_string();

            // servers
            search = map.find("servers");
            if (search != map.end())
            {
                JsonArray& svrs = search->second->to_array();

                for (auto s: svrs)
                    servers.insert((int)s->to_double());
            }

            if (! servers.empty())
            {
                Partition *partition =
                    new Partition(tsdb, this, NONE_NULL_STR(from), NONE_NULL_STR(to), servers);
                m_partitions.push_back(partition);
            }
        }

    }
}

PartitionManager::~PartitionManager()
{
    for (auto p: m_partitions) delete p;
    m_partitions.clear();
}

void
PartitionManager::init()
{
    BackLog::init();

    if (Config::exists(CFG_CLUSTER_SERVERS))
    {
        std::string servers = Config::get_str(CFG_CLUSTER_SERVERS);
        char buff[servers.size()+2];
        JsonArray arr;

        servers.copy(buff, sizeof(buff));
        JsonParser::parse_array(buff, arr);

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

            m_servers.emplace(id, new PartitionServer(id, address, tcp_port, http_port));
        }

        JsonParser::free_array(arr);
    }
    else
        Logger::debug("Cluster is not defined.");
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

bool
PartitionManager::submit_data_points()
{
    for (int i = 0; i < partition_server_forward_buffers.size(); i++)
    {
        PartitionBuffer *buffer = partition_server_forward_buffers[i];
        if (buffer == nullptr) continue;

        ASSERT(m_servers.find(i) != m_servers.end());
        Logger::trace("submitting %d bytes to transmit", buffer->size());
        m_servers[i]->submit_buffer(buffer);
        partition_server_forward_buffers[i] = nullptr;
    }

    return true;
}

Partition *
PartitionManager::get_partition(const char *metric) const
{
    if (m_partitions.empty()) return nullptr;

    Partition *catch_all = nullptr;

    for (auto partition: m_partitions)
    {
        if (partition->is_catch_all())
        {
            ASSERT(catch_all == nullptr);
            catch_all = partition;
            continue;
        }

        if (partition->match(metric))
            return partition;
    }

    return catch_all;
}


}
