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

#include "admin.h"
#include "append.h"
#include "global.h"
#include "cp.h"
#include "kv.h"
#include "logger.h"
#include "stats.h"
#include "tsdb.h"
#include "timer.h"


namespace tt
{


static const char *HTTP_MSG_PONG = "pong";

void
Admin::init()
{
    if (Config::inst()->get_int(CFG_TSDB_MIN_DISK_SPACE, CFG_TSDB_MIN_DISK_SPACE_DEF) > 0)
    {
        Task task;
        task.doit = &Admin::shutdown_if_disk_full;
        Timer::inst()->add_task(task, 300, "admin_disk_guard");
    }
}

bool
Admin::http_post_api_admin_handler(HttpRequest& request, HttpResponse& response)
{
    char buff[512]; // TODO: no magic numbers

    Logger::info("Handling admin request: %T", &request);

    if (request.params == nullptr)
    {
        const char *msg = "params empty";
        response.init(400, HttpContentType::PLAIN, strlen(msg), msg);
        return false;
    }

    KeyValuePair *params = KeyValuePair::parse_in_place(request.params, '&');
    Logger::info("params: %s", KeyValuePair::to_json(params, buff, sizeof(buff)));

    const char *cmd = KeyValuePair::get_value(params, "cmd");

    if (cmd == nullptr)
    {
        const char *msg = "cmd missing";
        response.init(400, HttpContentType::PLAIN, strlen(msg), msg);
        return false;
    }

    bool status = true;
    std::string msg;

    try
    {
        if (strcmp(cmd, "append") == 0)
        {
            status = cmd_append(params, response);
        }
        else if (strcmp(cmd, "cfg") == 0)
        {
            status = cmd_cfg(params, response);
        }
        else if (strcmp(cmd, "compact") == 0)
        {
            status = cmd_compact(params, response);
        }
        else if (strcmp(cmd, "cp") == 0)
        {
            status = cmd_cp(params, response);
        }
        else if (strcmp(cmd, "gc") == 0)
        {
            status = cmd_gc(params, response);
        }
        else if (strcmp(cmd, "log") == 0)
        {
            status = cmd_log(params, response);
        }
        else if (strcmp(cmd, "ping") == 0)
        {
            status = cmd_ping(params, response);
        }
        else if (strcmp(cmd, "rollup") == 0)
        {
            status = cmd_rollup(params, response);
        }
        else if (strcmp(cmd, "stat") == 0)
        {
            status = cmd_stat(params, response);
        }
        else if (strcmp(cmd, "stop") == 0)
        {
            status = cmd_stop(params, response);
        }
        else
        {
            msg = "unrecognized cmd: ";
            msg += cmd;
            status = false;
        }
    }
    catch (const std::exception& ex)
    {
        status = false;
        msg = ex.what();
        Logger::error("Failed to execute cmd %s: %s", cmd, ex.what());
    }
    catch (...)
    {
        status = false;
        msg = "Failed to execute cmd: ";
        msg += cmd;
        Logger::error("Failed to execute cmd %s for unknown reasons", cmd);
    }

    if (params != nullptr)
        MemoryManager::free_recyclables(params);

    if (! status)
    {
        if (msg.empty())
            response.init(400, HttpContentType::PLAIN);
        else
            response.init(400, HttpContentType::PLAIN, msg.size(), msg.c_str());
    }

    return status;
}

bool
Admin::cmd_append(KeyValuePair *params, HttpResponse& response)
{
    TaskData data;

    data.integer = 1;   // indicate this is from interactive cmd (vs. scheduled task)
    AppendLog::flush_all(data);
    char buff[32];
    int len = snprintf(buff, sizeof(buff), "append log generated");
    response.init(200, HttpContentType::PLAIN, len, buff);
    return true;
}

bool
Admin::cmd_cfg(KeyValuePair *params, HttpResponse& response)
{
    for (KeyValuePair *kv = params; kv != nullptr; kv = kv->next())
        Config::inst()->set_value(kv->m_key, kv->m_value);

    response.init(200);
    return true;
}

bool
Admin::cmd_compact(KeyValuePair *params, HttpResponse& response)
{
    TaskData data;

    data.integer = 1;   // indicate this is from interactive cmd (vs. scheduled task)
    Tsdb::compact(data);
    char buff[32];
    int len = snprintf(buff, sizeof(buff), "1 tsdbs compacted");
    response.init(200, HttpContentType::PLAIN, len, buff);
    return true;
}

bool
Admin::cmd_cp(KeyValuePair *params, HttpResponse& response)
{
    const char *leader = KeyValuePair::get_value(params, "leader");
    char *buff = MemoryManager::alloc_network_buffer();
    size_t size = MemoryManager::get_network_buffer_size();
    int len = CheckPointManager::get_persisted(leader, buff, size);
    ASSERT(len > 0);
    response.init(200, HttpContentType::JSON, len, buff);
    MemoryManager::free_network_buffer(buff);
    return true;
}

bool
Admin::cmd_gc(KeyValuePair *params, HttpResponse& response)
{
    TaskData data;
    data.integer = 1;   // indicate this is from interactive cmd (vs. scheduled task)
    MemoryManager::collect_garbage(data);
    response.init(200);
    return true;
}

bool
Admin::cmd_log(KeyValuePair *params, HttpResponse& response)
{
    const char *level = KeyValuePair::get_value(params, "level");

    if (level != nullptr)
        Logger::set_level(level);

    const char *action = KeyValuePair::get_value(params, "action");

    if (action != nullptr)
    {
        if (strcmp(action, "reopen") == 0)
        {
            Logger::inst()->reopen();
        }
        else
        {
            response.init(400);
            Logger::error("Unknown action %s for cmd log", action);
            return false;
        }
    }

    response.init(200);
    return true;
}

bool
Admin::cmd_ping(KeyValuePair *params, HttpResponse& response)
{
    response.init(200, HttpContentType::PLAIN, strlen(HTTP_MSG_PONG), HTTP_MSG_PONG);
    return true;
}

bool
Admin::cmd_rollup(KeyValuePair *params, HttpResponse& response)
{
    TaskData data;

    data.integer = 1;   // indicate this is from interactive cmd (vs. scheduled task)
    Tsdb::rollup(data);
    char buff[32];
    int len;
    if (data.integer > 0)
        len = snprintf(buff, sizeof(buff), "%d tsdbs rolled-up", data.integer);
    else
        len = snprintf(buff, sizeof(buff), "no tsdb was rolled-up");
    response.init(200, HttpContentType::PLAIN, len, buff);
    return true;
}

bool
Admin::cmd_stat(KeyValuePair *params, HttpResponse& response)
{
    char buff[64];
    int len = snprintf(buff, sizeof(buff), "{}");
    response.init(200, HttpContentType::JSON, len, buff);
    return true;
}

bool
Admin::cmd_stop(KeyValuePair *params, HttpResponse& response)
{
    Logger::info("Shutdown initiated");

    g_shutdown_requested = true;

    // This is called from an HTTP worker, which should NOT shutdown
    // HTTP server directly. Schedule a task to do it in a different thread!
    Task task;
    task.doit = &Admin::shutdown;
    Timer::inst()->add_task(task, 1, "admin_stop");

    const char *msg = "Exiting now.";
    response.init(200, HttpContentType::PLAIN, std::strlen(msg), msg);
    return true;
}

bool
Admin::shutdown(TaskData& data)
{
    Logger::trace("Admin::shutdown() entered");
    if ((http_server_ptr != nullptr) && ! http_server_ptr->is_shutdown_requested())
        http_server_ptr->shutdown();
    return false;
}

bool
Admin::shutdown_if_disk_full(TaskData& data)
{
    long page_cnt = Config::inst()->get_int(CFG_TSDB_PAGE_COUNT, CFG_TSDB_PAGE_COUNT_DEF);
    long min_disk = Config::inst()->get_int(CFG_TSDB_MIN_DISK_SPACE, CFG_TSDB_MIN_DISK_SPACE_DEF);

    if (Stats::get_disk_avail() < (min_disk*g_page_size*page_cnt))
    {
        Logger::warn("Disk full, shutting down...");
        shutdown(data);
    }

    return false;
}


}
