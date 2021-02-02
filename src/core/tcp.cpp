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

#include <cassert>
#include <thread>
#include <sys/epoll.h>
#include <net/if.h>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "global.h"
#include "tcp.h"
#include "config.h"
#include "append.h"
#include "memmgr.h"
#include "logger.h"
#include "leak.h"
#include "tsdb.h"


namespace tt
{


std::mutex TcpListener::m_lock;
std::map<int,TcpConnection*> TcpListener::m_all_conn_map;


/* TcpServer Implementation
 */
TcpServer::TcpServer() :
    TcpServer(Config::get_int(CFG_TCP_LISTENER_COUNT, CFG_TCP_LISTENER_COUNT_DEF)+1)
{
}

TcpServer::TcpServer(int listener_count) :
    m_socket_fd(-1),
    m_max_conns_per_listener(512),
    m_next_listener(0),
    m_listener_count(listener_count)
{
    size_t size = sizeof(TcpListener*) * m_listener_count;
    m_listeners = static_cast<TcpListener**>(malloc(size));
    ASSERT(m_listeners != nullptr);
    std::memset(m_listeners, 0, size);

    Logger::info("TCP m_listener_count = %d", m_listener_count);
    Logger::info("TCP m_max_conns_per_listener = %d", m_max_conns_per_listener);
}

TcpServer::~TcpServer()
{
    close_conns();

    if (m_listeners != nullptr)
    {
        for (int i = 0; i < m_listener_count; i++)
        {
            if (m_listeners[i] != nullptr) delete m_listeners[i];
        }
        free(m_listeners);
    }
}

void
TcpServer::close_conns()
{
    if (! is_shutdown_requested())
    {
        shutdown();
    }

    if (m_listeners != nullptr)
    {
        for (int i = 0; i < m_listener_count; i++)
        {
            if (m_listeners[i] != nullptr) m_listeners[i]->close_conns();
        }
    }

    if (m_socket_fd > 0)
    {
        close(m_socket_fd);
        m_socket_fd = -1;
    }
}

void
TcpServer::init()
{
}

bool
TcpServer::start(int port)
{
    ASSERT(port > 0);
    Logger::info("Starting TCP Server on port %d...", port);

    // 1. create and bind the socket
    struct addrinfo hints;
    struct addrinfo *result = nullptr, *ap;

    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;        // both IPv4 and IPv6 are ok
    hints.ai_socktype = SOCK_STREAM;    // TCP socket
    hints.ai_flags = AI_PASSIVE;

    int retval = getaddrinfo(nullptr, std::to_string(port).c_str(), &hints, &result);

    if (retval != 0)
    {
        Logger::error("Failed to start TCP server: %s", gai_strerror(retval));
        return false;
    }

    ASSERT(result != nullptr);

    for (ap = result; ap != nullptr; ap = ap->ai_next)
    {
        m_socket_fd = socket(ap->ai_family, ap->ai_socktype, ap->ai_protocol);
        if (m_socket_fd == -1) continue;

        if (g_opt_reuse_port)
        {
            int enable = 1;
            retval = setsockopt(m_socket_fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int));
            if (retval < 0) Logger::error("Failed to setsockopt, errno: %d", errno);
        }

        // adjust TCP window size
        int opt;
        socklen_t optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_RCVBUF, &opt, &optlen);

        if (retval == 0)
            Logger::info("Original SO_RCVBUF = %d", opt);
        else
            Logger::info("getsockopt(SO_RCVBUF) failed, errno = %d", errno);

        if (Config::exists(CFG_TCP_SOCKET_RCVBUF_SIZE))
        {
            opt = Config::get_bytes(CFG_TCP_SOCKET_RCVBUF_SIZE);
            retval = setsockopt(m_socket_fd, SOL_SOCKET, SO_RCVBUF, &opt, optlen);
            if (retval != 0)
                Logger::warn("setsockopt(RCVBUF) failed, errno = %d", errno);
            else
                Logger::info("SO_RCVBUF set to %d", opt);
        }

        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_SNDBUF, &opt, &optlen);

        if (retval == 0)
            Logger::info("Original SO_SNDBUF = %d", opt);
        else
            Logger::info("getsockopt(SO_SNDBUF) failed, errno = %d", errno);

        if (Config::exists(CFG_TCP_SOCKET_SNDBUF_SIZE))
        {
            opt = Config::get_bytes(CFG_TCP_SOCKET_SNDBUF_SIZE);
            retval = setsockopt(m_socket_fd, SOL_SOCKET, SO_SNDBUF, &opt, optlen);
            if (retval != 0)
                Logger::warn("setsockopt(SNDBUF) failed, errno = %d", errno);
            else
                Logger::info("SO_SNDBUF set to %d", opt);
        }

        retval = bind(m_socket_fd, ap->ai_addr, ap->ai_addrlen);
        if (retval == 0) break; // take the first successful bind
        close(m_socket_fd);
    }

    if (ap == nullptr)
    {
        Logger::error("Failed to bind to any network interfaces");
        return false;
    }

    freeaddrinfo(result);

    // collect socket info (options)
    {
        int opt;
        socklen_t optlen;
        char dev[IFNAMSIZ];

        optlen = sizeof(dev);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_BINDTODEVICE, dev, &optlen);
        if (retval == 0)
        {
            if ((0 <= optlen) && (optlen < IFNAMSIZ))
            {
                dev[optlen] = 0;
                Logger::info("SO_BINDTODEVICE = %s", dev);
            }
        }
        else
            Logger::info("SO_BINDTODEVICE: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_DEBUG, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_DEBUG = %d", opt);
        else
            Logger::info("SO_DEBUG: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_DONTROUTE, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_DONTROUTE = %d", opt);
        else
            Logger::info("SO_DONTROUTE: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_KEEPALIVE, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_KEEPALIVE = %d", opt);
        else
            Logger::info("SO_KEEPALIVE: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_PRIORITY, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_PRIORITY = %d", opt);
        else
            Logger::info("SO_PRIORITY: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_RCVBUF, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_RCVBUF = %d", opt);
        else
            Logger::info("SO_RCVBUF: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_RCVBUFFORCE, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_RCVBUFFORCE = %d", opt);
        else
            Logger::info("SO_RCVBUFFORCE: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_RCVLOWAT, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_RCVLOWAT = %d", opt);
        else
            Logger::info("SO_RCVLOWAT: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_SNDBUF, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_SNDBUF = %d", opt);
        else
            Logger::info("SO_SNDBUF: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_SNDBUFFORCE, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_SNDBUFFORCE = %d", opt);
        else
            Logger::info("SO_SNDBUFFORCE: errno = %d", errno);

        optlen = sizeof(opt);
        retval = getsockopt(m_socket_fd, SOL_SOCKET, SO_SNDLOWAT, &opt, &optlen);
        if (retval == 0)
            Logger::info("SO_SNDLOWAT = %d", opt);
        else
            Logger::info("SO_SNDLOWAT: errno = %d", errno);
    }

    // 2. make socket non-blocking
    if (! set_flags(m_socket_fd, O_NONBLOCK)) return false;

    // 3. listen on the socket
    retval = listen(m_socket_fd, m_max_conns_per_listener * m_listener_count);

    if (retval == -1)
    {
        Logger::error("Failed to listen on socket, errno: %d", errno);
        return false;
    }

    // 4. create threads to handle epoll events
    //    Create all the level 1 listeners before creating level 0 listener so
    //    that when level 0 listener is ready to send msgs to level 1 listeners
    //    they are already created and ready.
    for (int i = 1; i < m_listener_count; i++)
    {
        m_listeners[i] = new TcpListener(this, m_socket_fd, m_max_conns_per_listener, i);
    }

    m_listeners[0] = new TcpListener(this, m_socket_fd, m_max_conns_per_listener);

    return true;
}

// we are in edge-triggered mode, must read all data
bool
TcpServer::recv_tcp_data(TaskData& data)
{
    static size_t buff_size = MemoryManager::get_network_buffer_size() - 2;
    TcpConnection *conn = static_cast<TcpConnection*>(data.pointer);

    Logger::trace("recv_tcp_data: conn=%p, fd=%d", conn, conn->fd);

    int fd = conn->fd;

    char* (buffs[2]);
    int len = 0, curr = 0;
    bool conn_error = false;    // try to keep-alive

    if (conn->buff != nullptr)
    {
        len = conn->offset;
        buffs[0] = conn->buff;
        buffs[1] = MemoryManager::alloc_network_buffer();
        conn->buff = nullptr;
    }
    else
    {
        buffs[0] = MemoryManager::alloc_network_buffer();
        buffs[1] = MemoryManager::alloc_network_buffer();
    }

    while (len < buff_size)
    {
        // the MSG_DONTWAIT is not really needed, since
        // the socket itself is non-blocking
        int cnt = recv(fd, &buffs[curr][len], buff_size-len, MSG_DONTWAIT);

        if (cnt > 0)
        {
            len += cnt;
        }
        else if (cnt == 0)
        {
            break;
        }
        else //if (cnt == -1)
        {
            // TODO: what about EINTR???
            //       maybe we should delete conn???
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) break;
            Logger::warn("recv(%d) failed, errno = %d", fd, errno);
            conn_error = true;
            break;
        }

        buffs[curr][len] = 0;

        if (buffs[curr][len-1] == '\n')
        {
            process_data(conn, buffs[curr], len);
            len = 0;
        }
        else if (len >= buff_size)
        {
            //char *last = std::strrchr(buffs[curr], '\n');

            // find the last '\n'
            char *first = buffs[curr];
            char *last = nullptr;

            for (char *p = first+(len-2); p != first; p--)
            {
                if (*p == '\n')
                {
                    last = p;
                    break;
                }
            }

            if (last == nullptr)
            {
                Logger::error("Bad data format received in TcpListener:\n%s", first);
                len = 0;
            }
            else
            {
                int next = (curr + 1) % 2;
                int l = last - first + 1;
                len -= l;
                memcpy((void*)buffs[next], last+1, len);
                process_data(conn, buffs[curr], l);
                curr = next;
            }
        }
    }

    if (len > 0)
    {
        conn_error = false;
        conn->buff = buffs[curr];
        conn->offset = len;

        int next = (curr + 1) % 2;
        MemoryManager::free_network_buffer(buffs[next]);
    }
    else
    {
        conn->buff = nullptr;
        conn->offset = 0;

        MemoryManager::free_network_buffer(buffs[0]);
        MemoryManager::free_network_buffer(buffs[1]);
    }

    // closing the fd will deregister it from epoll
    // since we never dup() or fork(); but let's
    // deregister it anyway, just in case.
    if (conn_error)
    {
        conn->state |= TCS_ERROR;
    }

    return false;
}

bool
TcpServer::process_data(TcpConnection *conn, char *data, int len)
{
    try
    {
        data[len] = 0;

        HttpRequest request;
        HttpResponse response;

        request.init();
        request.content = data;
        request.length = len;
        request.forward = conn->forward;

        Logger::trace("TcpServer::process_data():\n%s", data);

        Tsdb::http_api_put_handler_plain(request, response);

        if (response.content_length > 0)
        {
            char *body = response.get_body();

            if (body != nullptr)
            {
                if (std::strncmp(body, DONT_FORWARD, std::strlen(DONT_FORWARD)) == 0)
                    conn->forward = false;
                else
                    send_response(conn->fd, body, std::strlen(body));
            }
        }
    }
    catch (const std::exception& ex)
    {
        Logger::error("Failed to process tcp request: %s", ex.what());
    }
    catch (...)
    {
        Logger::error("Failed to process tcp request with unknown exception");
    }

    return false;
}

void
TcpServer::send_response(int fd, char *content, int len)
{
    int sent_total = 0;

    ASSERT(fd != -1);
    ASSERT(content != nullptr);

    while (len > 0)
    {
        int sent = send(fd, content+sent_total, len, 0);

        if (sent == -1)
        {
            Logger::warn("tcp send_response() failed, errno = %d", errno);
            return;
        }

        len -= sent;
        sent_total += sent;
    }
}

void
TcpServer::shutdown(ShutdownRequest request)
{
    Stoppable::shutdown(request);

    for (size_t i = 0; i < m_listener_count; i++)
    {
        if (m_listeners[i] != nullptr)
        {
            m_listeners[i]->shutdown(request);
        }
    }
}

void
TcpServer::wait(size_t timeout_secs)
{
    for (size_t i = 0; i < m_listener_count; i++)
    {
        if (m_listeners[i] != nullptr)
        {
            m_listeners[i]->wait(timeout_secs);
        }
    }
}

bool
TcpServer::is_stopped() const
{
    for (size_t i = 0; i < m_listener_count; i++)
    {
        if ((m_listeners[i] != nullptr) && (! m_listeners[i]->is_stopped()))
        {
            return false;
        }
    }

    return true;
}

void
TcpServer::get_level1_listeners(std::vector<TcpListener*>& listeners) const
{
    for (size_t i = 1; i < m_listener_count; i++)
    {
        if ((m_listeners[i] != nullptr) && (! m_listeners[i]->is_stopped()))
        {
            listeners.push_back(m_listeners[i]);
        }
    }
}

TcpListener *
TcpServer::next_listener()
{
    m_next_listener++;
    if (m_next_listener >= m_listener_count) m_next_listener = 1;
    Logger::debug("m_next_listener = %d", m_next_listener);
    return m_listeners[m_next_listener];
}

TcpListener *
TcpServer::get_least_conn_listener() const
{
    TcpListener *listener = nullptr;
    size_t conn_cnt = m_max_conns_per_listener + 1;

    for (size_t i = 1; i < m_listener_count; i++)
    {
        if ((m_listeners[i] != nullptr) && (! m_listeners[i]->is_stopped()))
        {
            size_t cnt = m_listeners[i]->get_active_conn_count();

            if (cnt < conn_cnt)
            {
                conn_cnt = cnt;
                listener = m_listeners[i];
            }
        }
    }

    return listener;
}

TcpListener *
TcpServer::get_most_conn_listener() const
{
    TcpListener *listener = nullptr;
    int conn_cnt = -1;

    for (size_t i = 1; i < m_listener_count; i++)
    {
        if ((m_listeners[i] != nullptr) && (! m_listeners[i]->is_stopped()))
        {
            int cnt = m_listeners[i]->get_active_conn_count();

            if (conn_cnt < cnt)
            {
                conn_cnt = cnt;
                listener = m_listeners[i];
            }
        }
    }

    return listener;
}

size_t
TcpServer::get_pending_task_count() const
{
    size_t count = 0;

    for (size_t i = 0; i < m_listener_count; i++)
    {
        if (m_listeners[i] != nullptr)
        {
            count += m_listeners[i]->get_pending_task_count();
        }
    }

    return count;
}

int
TcpServer::get_total_task_count(size_t counts[], int size) const
{
    int i = 0;

    for (int l = 0; l < m_listener_count; l++)
    {
        int n = m_listeners[l]->get_total_task_count(&counts[i], size);

        i += n;
        size -= n;
    }

    return i;
}

size_t
TcpServer::get_active_conn_count() const
{
    int count = 0;

    for (size_t i = 0; i < m_listener_count; i++)
    {
        if (m_listeners[i] != nullptr)
        {
            count += m_listeners[i]->get_active_conn_count();
            Logger::debug("listener %d: active connection count = %d", i, m_listeners[i]->get_active_conn_count());
        }
    }

    return count;
}

bool
TcpServer::set_flags(int fd, int flags)
{
    int curr_flags = fcntl(fd, F_GETFL, 0);

    if (curr_flags == -1)
    {
        Logger::error("Failed to get flags for fd %d, errno: %d", fd, errno);
        return false;
    }

    curr_flags |= flags; // O_NONBLOCK;

    int retval = fcntl(fd, F_SETFL, curr_flags);

    if (retval == -1)
    {
        Logger::error("Failed to set flags for fd %d, errno: %d", fd, errno);
        return false;
    }

    return true;
}

void
TcpServer::instruct0(const char *instruction, int size)
{
    if (m_listeners[0] != nullptr)
    {
        m_listeners[0]->instruct(instruction, size);
    }
}

void
TcpServer::instruct1(const char *instruction, int size)
{
    for (int i = 1; i < m_listener_count; i++)
    {
        if (m_listeners[i] != nullptr)
        {
            m_listeners[i]->instruct(instruction, size);
        }
    }
}

TcpConnection *
TcpServer::create_conn() const
{
    return (TcpConnection*)MemoryManager::alloc_recyclable(RecyclableType::RT_TCP_CONNECTION);
}

Task
TcpServer::get_recv_data_task(TcpConnection *conn) const
{
    Task task;

    task.doit = &TcpServer::recv_tcp_data;
    task.data.pointer = conn;

    return task;
}


/* TcpListener Implementation
 */
TcpListener::TcpListener(TcpServer *server, int fd, size_t max_conns) :
    m_id(0),
    m_server(server),
    m_max_conns(max_conns),
    m_max_events(Config::get_int(CFG_TCP_MAX_EPOLL_EVENTS, CFG_TCP_MAX_EPOLL_EVENTS_DEF)),
    m_socket_fd(fd),
    m_epoll_fd(-1),
    m_pipe_fds{-1,-1},
    m_conns(nullptr),
    m_live_conns(nullptr),
    m_free_conns(nullptr),
    m_least_conn_listener(nullptr),
    m_conn_in_transit(nullptr)
    //m_stats_active_conn_count(0)
{
    if (! init(fd))
    {
        close_conns();
    }
}

// this one handles the tcp traffic
TcpListener::TcpListener(TcpServer *server, int fd, size_t max_conns, int id) :
    m_id(id),
    m_server(server),
    m_max_conns(max_conns),
    m_max_events(Config::get_int(CFG_TCP_MAX_EPOLL_EVENTS, CFG_TCP_MAX_EPOLL_EVENTS_DEF)),
    m_socket_fd(fd),
    m_epoll_fd(-1),
    m_pipe_fds{-1,-1},
    m_conns(nullptr),
    m_live_conns(nullptr),
    m_free_conns(nullptr),
    m_least_conn_listener(nullptr),
    m_conn_in_transit(nullptr),
    m_responders(std::string("tcp_")+std::to_string(id),
                 Config::get_int(CFG_TCP_RESPONDERS_PER_LISTENER, CFG_TCP_RESPONDERS_PER_LISTENER_DEF),
                 16)
    //m_stats_active_conn_count(0)
{
    if (! init(-1))
    {
        close_conns();
    }
}

// do not create listening thread here
TcpListener::TcpListener() :
    m_server(nullptr),
    m_max_conns(0),
    m_max_events(0),
    m_socket_fd(-1),
    m_epoll_fd(-1),
    m_pipe_fds{-1,-1},
    m_conns(nullptr),
    m_live_conns(nullptr),
    m_free_conns(nullptr),
    m_least_conn_listener(nullptr),
    m_conn_in_transit(nullptr)
    //m_stats_active_conn_count(0)
{
}

TcpListener::~TcpListener()
{
    close_conns();
    if (m_conns != nullptr) free(m_conns);
}

bool
TcpListener::init(int socket_fd)
{
    // create epoll instance
    m_epoll_fd = epoll_create1(0);

    if (m_epoll_fd == -1)
    {
        Logger::error("Failed to create epoll instance, errno: %d", errno);
        return false;
    }

    // register listening socket for epoll events
    if ((socket_fd >= 0) && ! register_with_epoll(socket_fd)) return false;

    // 6. setup a self-pipe to wake up listener
    //m_pipe_fds[2] = {-1, -1};

    int retval = pipe(m_pipe_fds);

    if (retval == -1)
    {
        Logger::error("Failed to create self-pipe, errno: %d", errno);
        return false;
    }

    if (! TcpServer::set_flags(m_pipe_fds[0], O_NONBLOCK)) return false;
    if (! TcpServer::set_flags(m_pipe_fds[1], O_NONBLOCK)) return false;
    if (! register_with_epoll(m_pipe_fds[0])) return false;

    //m_pipe_fd = pipe_fds[1];    // save this in order to send signal later

    // create listener thread
    if (socket_fd >= 0) // level 0
    {
        m_listener = std::thread(&TcpListener::listener0, this);
    }
    else    // level 1
    {
        m_listener = std::thread(&TcpListener::listener1, this);
    }

    return true;
}

void
TcpListener::close_conns()
{
    if (m_pipe_fds[1] != -1)
    {
        close(m_pipe_fds[1]);
        m_pipe_fds[1] = -1;
    }

    if (m_epoll_fd != -1)
    {
        close(m_epoll_fd);
        m_epoll_fd = -1;
    }
}

void
TcpListener::shutdown(ShutdownRequest request)
{
    Stoppable::shutdown(request);
    m_responders.shutdown(request);

    if (is_shutdown_requested() && (m_pipe_fds[1] != -1))
    {
        Logger::trace("Writing to self-pipe...");
        write_pipe(m_pipe_fds[1], PIPE_CMD_SET_STOPPED);
    }
}

void
TcpListener::wait(size_t timeout_secs)
{
    Logger::debug("Waiting for listener to stop...");
    if (m_listener.joinable()) m_listener.join();
    Logger::debug("Waiting for responders to stop...");
    m_responders.wait(timeout_secs);
    Logger::debug("All has stopped.");
}

bool
TcpListener::register_with_epoll(int fd)
{
    struct epoll_event event;

    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLRDHUP;

    if (fd != m_socket_fd)
    {
        event.events |= EPOLLET;    // edge-triggered
    }

    int retval = epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &event);

    if (retval == -1)
    {
        Logger::error("Failed to register socket %d for epoll events, errno: %d", fd, errno);
        return false;
    }

    //m_stats_active_conn_count++;
    Logger::debug("%d registered with epoll", fd);

    return true;
}

bool
TcpListener::deregister_with_epoll(int fd)
{
    struct epoll_event event;

    // the last (event) parameter will be ignored by epoll_ctl()
    int retval = epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, &event);

    if (retval == -1)
    {
        if ((errno != ENOENT) && (errno != EBADF))
        {
            Logger::error("Failed to deregister socket %d for epoll events, errno: %d", fd, errno);
            return false;
        }
    }

    //m_stats_active_conn_count--;
    Logger::debug("%d de-registered with epoll", fd);

    return true;
}

void
TcpListener::listener0()
{
    int fd_cnt;
    struct epoll_event events[m_max_events];
    uint32_t err_flags = EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    PipeReader pipe_reader(m_pipe_fds[0]);

    g_thread_id = "tcp_listener_0";

    Logger::info("entered epoll_wait() loop, fd=%d", m_epoll_fd);

    while (! is_shutdown_requested())
    {
        Logger::debug("enter epoll_wait(%d)", m_epoll_fd);

        fd_cnt = epoll_wait(m_epoll_fd, events, m_max_events, 5000);

        if (fd_cnt == -1)
        {
            Logger::error("epoll_wait() failed, errno: %d", errno);
            continue;
        }

        Logger::debug("received %d events from epoll_wait(%d)", fd_cnt, m_epoll_fd);

        // process fd_cnt of events, by handing them over to next level listeners
        for (int i = 0; i < fd_cnt; i++)
        {
            int fd = events[i].data.fd;

            if ((events[i].events & err_flags) || (!(events[i].events & EPOLLIN)))
            {
                // socket errors
                // TODO: check if there's an existing conn for this fd,
                // and if so, remove it
                close(events[i].data.fd);
                Logger::trace("socket error on listener0, fd=%d, events: 0x%x",
                    fd, events[i].events);
            }
            else if (fd == m_socket_fd)
            {
                // new connection
                new_conn0();
            }
            else if (fd == m_pipe_fds[0])
            {
                char *cmd;
                char buff[64];

                while ((cmd = pipe_reader.read_pipe()) != nullptr)
                {
                    Logger::debug("cmd:%s; pipe_reader:%s;", cmd, pipe_reader.c_str(buff, sizeof(buff)));

                    m_server->instruct1(cmd, strlen(cmd));

                    switch (cmd[0])
                    {
                        case PIPE_CMD_SET_STOPPED[0]:   set_stopped(); break;
                        default: break;
                    }
                }

                Logger::debug("cmd:null; pipe_reader:%s;", pipe_reader.c_str(buff, sizeof(buff)));
            }
        }

        if (fd_cnt == 0)
        {
            rebalance0();
        }
    }

    set_stopped();
    Logger::info("listener %d stopped.", m_id);
}

void
TcpListener::listener1()
{
    int fd_cnt;
    struct epoll_event events[m_max_events];
    uint32_t err_flags = EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    PipeReader pipe_reader(m_pipe_fds[0]);

    g_thread_id = "tcp_listener_" + std::to_string(m_id);

    Logger::info("entered epoll_wait() loop, fd=%d", m_epoll_fd);

    while (! is_shutdown_requested())
    {
        Logger::debug("enter epoll_wait(%d)", m_epoll_fd);

        fd_cnt = epoll_wait(m_epoll_fd, events, m_max_events, 5000);

        if (fd_cnt == -1)
        {
            Logger::error("epoll_wait() failed, errno: %d", errno);
            continue;
        }

        Logger::debug("received %d events from epoll_wait(%d)", fd_cnt, m_epoll_fd);

        // process fd_cnt of events, by handing them over to responders
        for (int i = 0; i < fd_cnt; i++)
        {
            int fd = events[i].data.fd;
            //Logger::info("fd = %d", fd);

            // TODO: in case of EPOLLRDHUP, we might need to read() until
            //       nothing left before we can close the connection.
            if ((events[i].events & err_flags) || (!(events[i].events & EPOLLIN)))
            {
                // socket errors
                Logger::trace("socket error on listener1, fd: %d, events: 0x%x",
                    fd, events[i].events);
                close_conn(fd);
            }
            else if (fd == m_pipe_fds[0])
            {
                char *cmd;
                char buff[64];

                while ((cmd = pipe_reader.read_pipe()) != nullptr)
                {
                    Logger::debug("cmd:%s; pipe_reader:%s;", cmd, pipe_reader.c_str(buff, sizeof(buff)));

                    switch (cmd[0])
                    {
                        case PIPE_CMD_REBALANCE_CONN[0]:    rebalance1(); break;
                        case PIPE_CMD_NEW_CONN[0]:          new_conn2(std::atoi(cmd+2)); break;
                        case PIPE_CMD_DISCONNECT_CONN[0]:   disconnect(); break;
                        case PIPE_CMD_FLUSH_APPEND_LOG[0]:  flush_append_log(); break;
                        case PIPE_CMD_CLOSE_APPEND_LOG[0]:  close_append_log(); break;
                        case PIPE_CMD_SET_STOPPED[0]:       set_stopped(); break;
                        default: break;
                    }
                }

                Logger::debug("cmd:null; pipe_reader:%s;", pipe_reader.c_str(buff, sizeof(buff)));
            }
            else
            {
                // new data on existing connections
                TcpConnection *conn = get_conn(fd);

                ASSERT(conn != nullptr);
                Logger::trace("received data on conn %p, fd %d", conn, conn->fd);

                Task task = m_server->get_recv_data_task(conn);

                // if previous attempt was not able to read the complete
                // request, we want to assign this one to the same worker.
                if (conn->worker_id != INVALID_WORKER_ID)
                {
                    m_responders.submit_task(task, conn->worker_id);
                }
                else
                {
                    conn->worker_id = m_responders.submit_task(task);
                }
            }
        }
    }

    set_stopped();
    Logger::info("TCP listener %d stopped.", m_id);
}

void
TcpListener::new_conn0()
{
    while (! is_shutdown_requested())
    {
        struct sockaddr addr;
        socklen_t len = sizeof(struct sockaddr);

        int fd = accept4(m_socket_fd, &addr, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (fd == -1)
        {
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
            {
                break;  // done handling all incoming connections
            }
            else
            {
                Logger::error("accept4() error: %d", errno);
                break;
            }
        }

        //Logger::info("new connection: %d", fd);

        // send it to level 1 listener
        TcpListener *listener1 = m_server->next_listener();
        char buff[32];
        snprintf(buff, 30, "%c %d\n", PIPE_CMD_NEW_CONN[0], fd);
        write_pipe(listener1->m_pipe_fds[1], buff);
    }

    // TODO: remove closed connections from m_conn_map!
}

// accept just one connection at a time
void
TcpListener::new_conn2(int fd)
{
    Logger::trace("new_conn2(%d)", fd);

    if (fd <= 0) return;

    if (! is_shutdown_requested())
    {
        // create new tcp connection before register with epoll
        TcpConnection *conn = get_or_create_conn(fd);
        if (conn == nullptr) return;

        ASSERT(fd == conn->fd);
        conn->state &= ~(TCS_ERROR | TCS_CLOSED);   // start new, clear these flags

        if (conn->state & TCS_REGISTERED)
        {
            unsigned int state = conn->state;
            Logger::info("already connected: %d, state: 0x%x", fd, state);
        }
        else
        {
            register_with_epoll(fd);
            conn->state |= TCS_REGISTERED;
            Logger::trace("new connection: %d", fd);
        }
    }
}

void
TcpListener::close_conn(int fd)
{
    auto search = m_conn_map.find(fd);

    if (search != m_conn_map.end())
    {
        TcpConnection *conn = search->second;
        m_conn_map.erase(search);
        Logger::trace("close_conn: conn=%p fd=%d", conn, conn->fd);

        {
            std::lock_guard<std::mutex> guard(m_lock);
            search = m_all_conn_map.find(fd);

            if (search != m_all_conn_map.end())
            {
                if (conn == search->second)
                {
                    m_all_conn_map.erase(search);
                }
            }
        }

        MemoryManager::free_recyclable(conn);
    }

    deregister_with_epoll(fd);
    close(fd);
}

// accept just one connection at a time
void
TcpListener::rebalance1()
{
    TcpConnection *conn = m_conn_in_transit.load(std::memory_order_relaxed);
    if (conn == nullptr) return;
    Logger::trace("received conn %p, fd %d, via rebalance1()", conn, conn->fd);
    conn->listener = this;
    ASSERT(! (conn->state & TCS_REGISTERED));
    register_with_epoll(conn->fd);
    conn->state |= TCS_REGISTERED;
    m_conn_map.insert(std::pair<int,TcpConnection*>(conn->fd, conn));
    send_response(conn);
    m_conn_in_transit.store(nullptr, std::memory_order_relaxed);
}

void
TcpListener::send_response(TcpConnection *conn)
{
    // see if we should move this connection to another listener
    TcpListener *least = m_least_conn_listener.load(std::memory_order_relaxed);

    if (least != nullptr)
    {
        Logger::info("moving conn %d to %d", conn->fd, least->m_id);

        // remove from map
        auto search = m_conn_map.find(conn->fd);
        ASSERT(search != m_conn_map.end());
        m_conn_map.erase(search);
        deregister_with_epoll(conn->fd);
        conn->state &= ~TCS_REGISTERED;

        TcpConnection *null = nullptr;

        for (unsigned int k = 0;
            ! least->m_conn_in_transit.compare_exchange_strong(null, conn, std::memory_order_relaxed);
            k++)
        {
            //write(least->m_pipe_fds[1], "b\n", 2);
            write_pipe(least->m_pipe_fds[1], PIPE_CMD_REBALANCE_CONN);
            spin_yield(k);
        }

        //write(least->m_pipe_fds[1], "b\n", 2);
        write_pipe(least->m_pipe_fds[1], PIPE_CMD_REBALANCE_CONN);
        m_least_conn_listener.store(nullptr, std::memory_order_relaxed);
    }
}

void
TcpListener::disconnect()
{
    Logger::debug("enter disconnect()");

    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    std::map<int,TcpConnection*>::iterator it;

    for (it = m_conn_map.begin(); it != m_conn_map.end(); )
    {
        TcpConnection *conn = it->second;
        std::chrono::duration<double> elapsed_seconds = now - conn->last_contact;
        long secs = elapsed_seconds.count();
        int timeout = Config::get_time(CFG_TCP_CONNECTION_TIMEOUT, TimeUnit::SEC, CFG_TCP_CONNECTION_TIMEOUT_DEF);

        if (secs > timeout)
        {
            Logger::trace("closing connection: conn=%p fd=%d", conn, conn->fd);

            //auto search = m_conn_map.find(conn->fd);
            //ASSERT(search != m_conn_map.end());
            it = m_conn_map.erase(it);

            {
                std::lock_guard<std::mutex> guard(m_lock);
                auto search = m_all_conn_map.find(conn->fd);
                if (search != m_all_conn_map.end())
                {
                    m_all_conn_map.erase(search);
                }
            }

            deregister_with_epoll(conn->fd);
            close(conn->fd);
            MemoryManager::free_recyclable(conn);
        }
        else
        {
            it++;
            Logger::debug("connection %d used %ld seconds ago", conn->fd, secs);
        }
    }
}

void
TcpListener::write_pipe(int fd, const char *msg)
{
    ASSERT(msg != nullptr);

    size_t len = std::strlen(msg);

    for (int k = 0; k < 32; k++)
    {
        ssize_t rc = write(fd, msg, len);

        if (rc >= len) return;

        if (rc >= 0)
        {
            msg += rc;
            len -= rc;
        }
        else if (errno != EAGAIN)
        {
            Logger::info("failed to write_pipe(%d), errno=%d", fd, errno);
            break;
        }
        else
        {
            spin_yield(k);
        }
    }

    Logger::debug("write_pipe() failed to write all bytes, %d remaining", len);
}

void
TcpListener::flush_append_log()
{
    Task task;
    task.doit = &AppendLog::flush;
    m_responders.submit_task_to_all(task);
}

void
TcpListener::close_append_log()
{
    Task task;
    task.doit = &AppendLog::close;
    m_responders.submit_task_to_all(task);
}

TcpConnection *
TcpListener::get_or_create_conn(int fd)
{
    TcpConnection *conn = nullptr;
    auto search = m_conn_map.find(fd);

    if (search == m_conn_map.end())
    {
        //conn = (TcpConnection*)MemoryManager::alloc_recyclable(RecyclableType::RT_TCP_CONNECTION);
        conn = m_server->create_conn();

        {
            std::lock_guard<std::mutex> guard(m_lock);
            search = m_all_conn_map.find(fd);

            if (search == m_all_conn_map.end())
            {
                m_all_conn_map.insert(std::pair<int,TcpConnection*>(fd, conn));
            }
            else
            {
                TcpConnection *c = search->second;

                if (c->state & TCS_CLOSED)
                {
                    MemoryManager::free_recyclable(c);
                    m_all_conn_map.erase(search);
                    m_all_conn_map.insert(std::pair<int,TcpConnection*>(fd, conn));
                }
                else
                {
                    unsigned int state = c->state;
                    Logger::warn("duplicate fd %d discarded, state: 0x%x, conn=%p",
                        fd, state, c);
                    MemoryManager::free_recyclable(conn);
                    return nullptr;
                }
            }
        }

        conn->fd = fd;
        conn->server = m_server;
        conn->listener = this;

        m_conn_map.insert(std::pair<int,TcpConnection*>(fd, conn));
        Logger::trace("created conn %d", fd);
    }
    else
    {
        conn = search->second;
    }

    ASSERT(fd == conn->fd);
    conn->last_contact = std::chrono::steady_clock::now();
    Logger::trace("conn: %p, fd: %d", conn, conn->fd);

    return conn;
}

TcpConnection *
TcpListener::get_conn(int fd)
{
    TcpConnection *conn = nullptr;
    auto search = m_conn_map.find(fd);

    if (search != m_conn_map.end())
    {
        conn = search->second;
        ASSERT(fd == conn->fd);
        conn->last_contact = std::chrono::steady_clock::now();
        Logger::trace("conn: %p, fd: %d", conn, conn->fd);
    }

    return conn;
}

int
TcpListener::get_active_conn_count()
{
    std::lock_guard<std::mutex> guard(m_lock);
    return m_all_conn_map.size();
}

// Find the level 1 listener with max/min connections;
// and instruct the one with max connections to move
// one of the connections to the one with min connections;
void
TcpListener::rebalance0()
{
    Logger::debug("rebalancing...");

    TcpListener *least = m_server->get_least_conn_listener();
    TcpListener *most = m_server->get_most_conn_listener();

    if (least == nullptr)
    {
        Logger::warn("least = nullptr");
        return;
    }

    if (most == nullptr)
    {
        Logger::warn("most = nullptr");
        return;
    }

    if ((least->get_active_conn_count() + 1) < most->get_active_conn_count())
    {
        TcpListener *null = nullptr;
        Logger::info("Trying to move 1 conn from %d to %d", most->m_id, least->m_id);
        bool updated = most->m_least_conn_listener.compare_exchange_strong(
            null, least, std::memory_order_relaxed);
        Logger::info("m_least_conn_listener updated: %s", (updated?"true":"false"));
    }
}

void
TcpListener::instruct(const char *instruction, int size)
{
    if (m_pipe_fds[1] != -1)
    {
        write(m_pipe_fds[1], instruction, size);
    }
}


PipeReader::PipeReader(int fd) :
    m_fd(fd),
    m_index(0)
{
    m_buff2[0] = 0;
}

const char *
PipeReader::c_str(char *buff, size_t size) const
{
    snprintf(buff, size, "idx=%d buff=%s buff2=%s", m_index, m_buff, m_buff2);
    return buff;
}

// returns a single line, if available; otherwise return nullptr
char *
PipeReader::read_pipe()
{
    if (m_buff2[0] != 0)
    {
        m_index = std::strlen(m_buff2);
        std:strcpy(m_buff, m_buff2);
        m_buff2[0] = 0;

        for (int i = 0; i < m_index; i++)
        {
            if (m_buff[i] == '\n')
            {
                if (m_buff[i+1] != 0)
                {
                    std::strcpy(m_buff2, &m_buff[i+1]);
                    m_buff[i+1] = 0;
                }
                else
                {
                    m_index = 0;
                }

                return m_buff;
            }
        }
    }

    for (int k = 0; k < 16; k++)
    {
        int rc = read(m_fd, &m_buff[m_index], sizeof(m_buff)-m_index-1);

        if (rc >= 0)
        {
            for (int i = 0; i < rc; i++)
            {
                if (m_buff[m_index+i] == '\n')
                {
                    m_buff[m_index+rc] = 0;

                    if ((i+1) == rc)
                    {
                        m_index = 0;
                    }
                    else
                    {
                        std::strcpy(m_buff2, &m_buff[m_index+i+1]);
                        m_buff[m_index+i+1] = 0;
                    }

                    return m_buff;
                }
            }

            m_index += rc;
        }
        else if (errno != EAGAIN)
        {
            Logger::debug("failed to read_pipe(%d), errno=%d", m_fd, errno);
            break;
        }
        else
        {
            spin_yield(k);
        }
    }

    Logger::debug("pipe_reader, m_index = %d", m_index);
    return nullptr;
}


}
