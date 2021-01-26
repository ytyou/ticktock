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

#pragma once

#include "stop.h"
#include "tsdb.h"


namespace tt
{


typedef bool (*UdpRequestHandler)();


class UdpListener : public Stoppable
{
public:
    UdpListener(int id, int port);
    ~UdpListener();

private:
    void receiver();            // thread loop
    void receiver2();           // thread loop
    bool process_one_line(Tsdb* &tsdb, char *line);

    int m_id;
    int m_port;
    int m_fd;

    std::thread m_listener;     // the thread that receives UDP msgs
};


class UdpServer : public Stoppable
{
public:
    bool start(int port);
    void shutdown(ShutdownRequest request = ShutdownRequest::ASAP);

private:
    UdpRequestHandler m_request_handler;    // current active handler

    std::vector<UdpListener*> m_listeners;
};


}
