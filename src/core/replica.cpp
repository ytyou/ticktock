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

#include <algorithm>
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
#include "replica.h"
#include "tcp.h"
#include "tsdb.h"
#include "json.h"
#include "utils.h"


namespace tt
{


int32_t ReplicationManager::m_id;
int64_t ReplicationManager::m_start;
bool ReplicationManager::m_local;
bool ReplicationManager::m_remote;

std::vector<ReplicationStream*> ReplicationManager::m_streams;
std::vector<ReplicationServer*> ReplicationManager::m_replicas;


ReplicationBuffer::ReplicationBuffer() :
    m_buff(nullptr),
    m_data_size(0),
    m_buff_size(MemoryManager::get_network_buffer_size())
{
    init();
}

ReplicationBuffer::~ReplicationBuffer()
{
    recycle();
}

void
ReplicationBuffer::init()
{
    if (m_buff == nullptr)
        m_buff = MemoryManager::alloc_network_buffer();
    m_data_size = 0;
}

bool
ReplicationBuffer::recycle()
{
    if (m_buff != nullptr)
    {
        MemoryManager::free_network_buffer(m_buff);
        m_buff = nullptr;
    }
    return true;
}

size_t
ReplicationBuffer::append(const char *data, size_t len)
{
    return 0;
}


ReplicationServer::ReplicationServer(int32_t id, const char *address, int32_t tcp_port, int32_t http_port) :
    m_id(id),
    m_address(address),
    m_tcp_port(tcp_port),
    m_http_port(http_port),
    m_fd(-1)
{
    for (auto& rs: ReplicationManager::get_streams())
    {
        std::thread th(&ReplicationServer::do_work, this, rs);
        m_workers.push_back(std::move(th));
    }
}

ReplicationServer::~ReplicationServer()
{
    shutdown();
    for (auto &w: m_workers)
        if (w.joinable()) w.join();
}

void
ReplicationServer::connect(ReplicationStream *stream)
{
    ASSERT(stream != nullptr);

    // connect to the replica
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
        // send 'rep', which will be handled by ReplicationManager::handshake()
        char buff[64];
        int32_t len = stream->get_rep(buff, sizeof(buff));

        if (! send(buff, len))
        {
            Logger::warn("failed to send rep to (%s:%d)\n", m_address.c_str(), m_tcp_port);
            ::close(m_fd);
            m_fd = -1;
        }
        else
        {
            Logger::info("connected to %s:%d, rep sent", m_address.c_str(), m_tcp_port);
        }
    }

    if (m_fd != -1)
        Logger::info("connected to %s:%d, fd=%d", m_address.c_str(), m_tcp_port, m_fd);
    else
        Logger::debug("failed to connect to %s:%d", m_address.c_str(), m_tcp_port);
}

bool
ReplicationServer::send(const char *buff, int32_t len)
{
    if (len <= 0) return true;

    int32_t sent_total = 0;

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

void
ReplicationServer::do_work(ReplicationStream *stream)
{
    ASSERT(stream != nullptr);

    while (! is_shutdown_requested())
    {
    }
}

void
ReplicationServer::close()
{
    if (m_fd != -1)
    {
        ::close(m_fd);
        m_fd = -1;
    }
}


/*
ReplicationCheckPoint::ReplicationCheckPoint(uint64_t first) :
    m_first(first),
    m_second(0)
{
}
*/


ReplicationStream::ReplicationStream(int32_t id) :
    m_id(id)
{
}

int32_t
ReplicationStream::get_rep(char *buff, size_t len)
{
    return std::snprintf(buff, len, "rep %d.%d", ReplicationManager::get_id(), m_id);
}

bool
ReplicationStream::append(const char *data, size_t len)
{
    {
        std::lock_guard<std::mutex> guard(m_lock);

        for ( ; ; )
        {
            ReplicationBuffer& buff = m_buffers.back();
            size_t size = buff.append(data, len);
            ASSERT(size <= len);
            len -= size;

            if (len != 0)
            {
                buff.set_check_point();
                m_buffers.emplace_back();
            }
            else
                break;
        }
    }

    m_signal.notify_all();
    return true;
}


void
ReplicationManager::init()
{
    if (Config::exists(CFG_REPLICATION_MODE) &&
        Config::exists(CFG_REPLICATION_SERVER_ID) &&
        Config::exists(CFG_REPLICATION_REPLICAS))
    {
        m_id = Config::get_int(CFG_REPLICATION_SERVER_ID);

        // TODO: retrieve 'start' from ReplicationState file
        m_start = 0;

        const std::string& mode = Config::get_str(CFG_REPLICATION_MODE);
        if (mode.compare("ON") == 0)
        {
            m_local = true;
            m_remote = true;
        }
        else if (mode.compare("PROXY") == 0)
        {
            m_local = false;
            m_remote = true;
        }
        else    // OFF
        {
            m_local = true;
            m_remote = false;
        }

        Logger::info("Replication mode: %s", mode.c_str());

        std::string replicas = Config::get_str(CFG_REPLICATION_REPLICAS);
        char buff[replicas.size()+2];
        JsonArray arr;

        replicas.copy(buff, sizeof(buff));
        JsonParser::parse_array(buff, arr);

        for (auto val: arr)
        {
            int id;
            const char *address;
            JsonMap& map = val->to_map();

            // id
            auto search = map.find("id");
            if (search == map.end())
            {
                Logger::error("replication.replicas config missing server id");
                continue;
            }
            id = (int)search->second->to_double();

            // address
            search = map.find("address");
            if (search == map.end())
            {
                Logger::error("replication.replicas config missing server address");
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

            if (1) //(std::find())
            {
                // modify existing, if necessary
            }
            else
                m_replicas.push_back(new ReplicationServer(id, address, tcp_port, http_port));
        }

        JsonParser::free_array(arr);
    }
    else
    {
        m_local = true;
        m_remote = false;

        for (auto &r : m_replicas) r->shutdown();
        m_replicas.clear();
        m_replicas.shrink_to_fit();

        Logger::info("Replication mode: OFF");
    }

    // create exactly 1 stream, for now
    if (m_streams.empty())
        m_streams.push_back(new ReplicationStream(0));
}

// return the length of the 'rep' line
int
ReplicationManager::handshake(char *rep, HttpResponse& response)
{
    // extract leader's info

    // return our check-point

    return 0;
}

bool
ReplicationManager::forward(HttpRequest& request, HttpResponse& response)
{
    // TODO: for now we only have 1 stream...
    ASSERT(! m_streams.empty());
    bool success = m_streams.back()->append(request.content, request.length);
    response.init(success?200:500);
    return success;
}

int32_t
ReplicationManager::checkpoint(char *cp, HttpResponse& response)
{
    return 0;
}

void
ReplicationManager::shutdown(bool wait)
{
    for (auto &r : m_replicas)
        r->shutdown();

    if (wait)
    {
    }
}


}
