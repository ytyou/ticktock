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

#include <string>
#include <unistd.h>
#include "config.h"
#include "global.h"


namespace tt
{


const char* const EMPTY_STRING = "";
std::string EMPTY_STD_STRING("");
const char* const METRIC_TAG_NAME = "metric";
const char* const HOST_TAG_NAME = "host";
const char* const TYPE_TAG_NAME = "type";
const std::string WHITE_SPACES = " \n\r\t\f\v";
std::string g_host_name;
HttpServer *http_server_ptr = nullptr;
TcpServer *tcp_server_ptr = nullptr;
UdpServer *udp_server_ptr = nullptr;
bool g_opt_reuse_port = false;      // reuse port when bind()? controlled by cmd line option -r
bool g_tstamp_resolution_ms = true;
bool g_cluster_enabled = false;
bool g_self_meter_enabled = CFG_TSDB_SELF_METER_ENABLED_DEF;
std::atomic<bool> g_shutdown_requested{false};

std::string g_config_file("tt.conf");
thread_local std::string g_thread_id("unknown");
std::atomic<std::thread::id> g_handler_thread_id;

long int g_page_size;
const long int g_sys_page_size = sysconf(_SC_PAGE_SIZE);


}
