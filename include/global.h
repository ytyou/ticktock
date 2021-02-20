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

#include <string>
#include <atomic>


namespace tt
{


#define TT_MAJOR_VERSION    0
#define TT_MINOR_VERSION    2
#define TT_PATCH_VERSION    0

class HttpServer;
class TcpServer;
class UdpServer;

extern const char* const EMPTY_STRING;
extern std::string EMPTY_STD_STRING;
extern const char* const METRIC_TAG_NAME;
extern const char* const HOST_TAG_NAME;
extern const std::string WHITE_SPACES;
extern std::string g_config_file;
extern std::string g_host_name;
extern thread_local std::string g_thread_id;
extern HttpServer *http_server_ptr;
extern TcpServer *tcp_server_ptr;
extern UdpServer *udp_server_ptr;
extern const long int g_page_size;
extern bool g_opt_reuse_port;       // reuse port when bind()?
extern bool g_tstamp_resolution_ms;
extern bool g_cluster_enabled;
extern bool g_self_meter_enabled;
extern std::atomic<bool> g_shutdown_requested;


}
