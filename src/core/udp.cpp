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

#include <arpa/inet.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <unistd.h>
#include "config.h"
#include "logger.h"
#include "udp.h"


namespace tt
{


UdpListener::UdpListener(int id, int port) :
    m_id(id),
    m_port(port),
    m_fd(-1)
{
    ASSERT(m_port > 0);

    // create listener thread to receive udp msgs
    m_listener = std::thread(&UdpListener::receiver, this);
}

void
UdpListener::receiver()
{
    g_thread_id = "udp_receiver_" + std::to_string(m_id);

    // setup network
    m_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (m_fd == -1)
    {
        Logger::error("Failed to create udp socket, errno: %d", errno);
        return;
    }

    int one = 1;
    int retval = setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one));
    if (retval == -1)
        Logger::error("Failed to setsockopt(REUSEADDR), errno: %d", errno);

    retval = setsockopt(m_fd, SOL_SOCKET, SO_REUSEPORT, (char *)&one, sizeof(one));
    if (retval == -1)
        Logger::error("Failed to setsockopt(REUSEPORT), errno: %d", errno);

    struct sockaddr_in addr;

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(m_port);

    retval = bind(m_fd, (const sockaddr*)&addr, sizeof(addr));
    if (retval == -1)
        Logger::error("Failed to bind(%d), errno: %d", m_port, errno);

    // adjust receive buffer size
/*
 * Don't see any benefit of this so far...
 *
    int bufsz;
    socklen_t len = sizeof(bufsz);
    retval = getsockopt(m_fd, SOL_SOCKET, SO_RCVBUF, &bufsz, &len);
    if (retval == -1)
        Logger::error("Failed to getsockopt(SO_RCVBUF), errno: %d", errno);
    Logger::info("Increase socket receive buffer from %d to %d", bufsz, 2*bufsz);

    len = 2 * bufsz;
    retval = setsockopt(m_fd, SOL_SOCKET, SO_RCVBUF, &len, sizeof(len));
    if (retval == -1)
        Logger::error("Failed to setsockopt(SO_RCVBUF), errno: %d", errno);
*/

    // prepare to receive
    int batch_size = Config::get_int(CFG_UDP_BATCH_SIZE, CFG_UDP_BATCH_SIZE_DEF);
    int max_line = Config::get_int(CFG_TSDB_MAX_DP_LINE, CFG_TSDB_MAX_DP_LINE_DEF);

    struct mmsghdr msgs[batch_size];
    struct iovec vecs[batch_size];
    char buffs[batch_size][max_line+2];

    memset(msgs, 0, sizeof(msgs));

    for (int i = 0; i < batch_size; i++)
    {
        vecs[i].iov_base = buffs[i];
        vecs[i].iov_len = max_line;
        msgs[i].msg_hdr.msg_iov = &vecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    Tsdb *tsdb = nullptr;

    while (! is_shutdown_requested())
    {
        retval = recvmmsg(m_fd, msgs, batch_size, MSG_WAITFORONE, nullptr);

        if (retval == -1)
        {
            Logger::debug("recvmmsg() failed, errno: %d", errno);
            continue;
        }

        for (int i = 0; i < retval; i++)
        {
            int len = msgs[i].msg_len;
            buffs[i][len] = '\n';
            buffs[i][len+1] = 0;

            process_one_line(tsdb, buffs[i]);

            msgs[i].msg_len = 0;
            msgs[i].msg_hdr.msg_flags = 0;
        }
    }

    if (m_fd != -1) close(m_fd);
}

void
UdpListener::receiver2()
{
    g_thread_id = "udp_receiver_" + std::to_string(m_id);

    // setup network
    //m_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    m_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (m_fd == -1)
    {
        Logger::error("Failed to create udp socket, errno: %d", errno);
        return;
    }

    int one = 1;
    int retval = setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&one, sizeof(one));
    if (retval == -1)
        Logger::error("Failed to setsockopt(REUSEADDR), errno: %d", errno);

    retval = setsockopt(m_fd, SOL_SOCKET, SO_REUSEPORT, (char *)&one, sizeof(one));
    if (retval == -1)
        Logger::error("Failed to setsockopt(REUSEPORT), errno: %d", errno);

    struct sockaddr_in addr;

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(m_port);

    retval = bind(m_fd, (const sockaddr*)&addr, sizeof(addr));
    if (retval == -1)
        Logger::error("Failed to bind(%d), errno: %d", m_port, errno);

    // ready to receive
    int max_line = Config::get_int(CFG_TSDB_MAX_DP_LINE, CFG_TSDB_MAX_DP_LINE_DEF);
    char buff[max_line+1];
    Tsdb *tsdb = nullptr;

    while (! is_shutdown_requested())
    {
        retval = recvfrom(m_fd, buff, max_line, MSG_NOSIGNAL, nullptr, nullptr);

        if (retval > 0)
        {
            ASSERT(retval < max_line);
            buff[retval] = 0;
            process_one_line(tsdb, buff);
        }
    }

    if (m_fd != -1) close(m_fd);
}

bool
UdpListener::process_one_line(Tsdb* &tsdb, char *line)
{
    DataPoint dp;

    Logger::trace("udp process_one_line: %s", line);

    if (! dp.from_plain(line)) return false;

    if ((tsdb == nullptr) || !(tsdb->in_range(dp.get_timestamp())))
    {
        tsdb = Tsdb::inst(dp.get_timestamp());
    }

    ASSERT(tsdb != nullptr);
    return tsdb->add(dp);
}


bool
UdpServer::start(int port)
{
    int listener_cnt = Config::get_int(CFG_UDP_LISTENER_COUNT, CFG_UDP_LISTENER_COUNT_DEF);

    ASSERT(port > 0);
    ASSERT(listener_cnt > 0);

    // create listeners to receive and process data points
    for (int i = 0; i < listener_cnt; i++)
    {
        m_listeners.push_back(new UdpListener(i, port));
    }

    Logger::info("UdpServer created %d listeners", listener_cnt);
    return true;
}

void
UdpServer::shutdown(ShutdownRequest request)
{
    Stoppable::shutdown(request);

    for (UdpListener *listener: m_listeners)
    {
        listener->shutdown(request);
    }
}


}
