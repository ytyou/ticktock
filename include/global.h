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

#pragma once

#include <string>
#include <atomic>
#include <thread>
#include "type.h"


namespace tt
{


#define TT_MAJOR_VERSION    0
#define TT_MINOR_VERSION    20
#define TT_PATCH_VERSION    0

class HttpServer;
class TcpServer;
class UdpServer;

extern const char* const EMPTY_STRING;
extern const std::string EMPTY_STD_STRING;
extern const char* const METRIC_TAG_NAME;
extern const char* const HOST_TAG_NAME;
extern const char* const TYPE_TAG_NAME;
extern const std::string WHITE_SPACES;
extern std::string g_config_file;
extern std::string g_host_name;
extern thread_local std::string g_thread_id;
extern HttpServer *http_server_ptr;
extern TcpServer *tcp_server_ptr;
extern UdpServer *udp_server_ptr;
extern PageSize g_page_size;
extern PageCount g_page_count;
extern const long int g_sys_page_size;
extern bool g_opt_reuse_port;       // reuse port when bind()?
extern bool g_tstamp_resolution_ms;
extern bool g_cluster_enabled;
extern bool g_self_meter_enabled;
extern std::atomic<bool> g_shutdown_requested;
extern std::atomic<std::thread::id> g_handler_thread_id;
extern std::string g_working_dir;
extern uint32_t g_rollup_interval_1h;   // 1-hour
extern uint32_t g_rollup_interval_1d;   // 1-day
extern short g_rollup_compressor_version;   // 0 means no compression
extern bool g_quiet;    // minimal console output


}
