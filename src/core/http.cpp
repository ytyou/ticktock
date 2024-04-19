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

#include <cassert>
#include <thread>
#include <cstring>
#include <unistd.h>
#include "admin.h"
#include "aggregate.h"
#include "append.h"
#include "global.h"
#include "http.h"
#include "config.h"
#include "memmgr.h"
#include "logger.h"
#include "leak.h"
#include "query.h"
#include "stats.h"
#include "zlib/zlib.h"


namespace tt
{


// TODO: these each consumes 1MB buffer, unnecessarily;
//       and they can't contain X-Request-ID;
//static HttpResponse HTTP_200 = {200, HttpContentType::HTML};
//static HttpResponse HTTP_400 = {400, HttpContentType::HTML};
//static HttpResponse HTTP_404 = {404, HttpContentType::HTML};
//static HttpResponse HTTP_408 = {408, HttpContentType::HTML};
//static HttpResponse HTTP_411 = {411, HttpContentType::HTML};
//static HttpResponse HTTP_413 = {413, HttpContentType::HTML};


const char *HTTP_METHOD_GET = "GET";
const char *HTTP_METHOD_POST = "POST";

const char *CRLF = "\r\n";

// paths we handle
const char *HTTP_API_ADMIN = "/api/admin";
const char *HTTP_API_AGGREGATORS = "/api/aggregators";
const char *HTTP_API_CONFIG = "/api/config";
const char *HTTP_API_CONFIG_FILTERS = "/api/config/filters";
const char *HTTP_API_PUT = "/api/put";
const char *HTTP_API_QUERY = "/api/query";
const char *HTTP_API_STATS = "/api/stats";
const char *HTTP_API_SUGGEST = "/api/suggest";
const char *HTTP_API_VERSION = "/api/version";
const char *HTTP_API_WRITE = "/api/write";


// These strings can't be longer than MAX_CONTENT_TYPE_SIZE (32)
static const char *HTTP_CONTENT_TYPES[] =
{
    "text/html",
    "application/json",
    "text/plain"
};

//int HttpServer::m_max_resend = 0;


std::map<const char*,HttpRequestHandler,cstr_less> HttpServer::m_get_handlers;
std::map<const char*,HttpRequestHandler,cstr_less> HttpServer::m_put_handlers;
std::map<const char*,HttpRequestHandler,cstr_less> HttpServer::m_post_handlers;


/* HttpServer Implementation
 */
HttpServer::HttpServer()
{
    m_fd_type = FileDescriptorType::FD_HTTP;
}

void
HttpServer::init()
{
    TcpServer::init();
    //m_max_resend = Config::get_int(CFG_HTTP_MAX_RETRIES, CFG_HTTP_MAX_RETRIES_DEF);

    add_get_handler(HTTP_API_AGGREGATORS, &Aggregator::http_get_api_aggregators_handler);
    add_get_handler(HTTP_API_CONFIG, &HttpServer::http_get_api_config_handler);
    add_get_handler(HTTP_API_CONFIG_FILTERS, &QueryExecutor::http_get_api_config_filters_handler);
    add_get_handler(HTTP_API_QUERY, &QueryExecutor::http_get_api_query_handler);
    add_get_handler(HTTP_API_STATS, &HttpServer::http_get_api_stats_handler);
    add_get_handler(HTTP_API_SUGGEST, &Tsdb::http_get_api_suggest_handler);
    add_get_handler(HTTP_API_VERSION, &HttpServer::http_get_api_version_handler);

    if (Config::inst()->exists(CFG_HTTP_REQUEST_FORMAT))
    {
        if (Config::inst()->get_str(CFG_HTTP_REQUEST_FORMAT) == "json")
            add_post_handler(HTTP_API_PUT, &Tsdb::http_api_put_handler_json);
        else
            add_post_handler(HTTP_API_PUT, &Tsdb::http_api_put_handler_plain);
    }
    else
        add_post_handler(HTTP_API_PUT, &Tsdb::http_api_put_handler);

    add_post_handler(HTTP_API_WRITE, &Tsdb::http_api_write_handler);
    add_post_handler(HTTP_API_QUERY, &QueryExecutor::http_post_api_query_handler);
    add_post_handler(HTTP_API_ADMIN, &Admin::http_post_api_admin_handler);
}

int
HttpServer::get_responders_per_listener(int which) const
{
    return Config::inst()->get_http_responders_per_listener(which);
    //int n = Config::get_int(CFG_HTTP_RESPONDERS_PER_LISTENER, CFG_HTTP_RESPONDERS_PER_LISTENER_DEF);
    //return (n > 0) ? n : CFG_HTTP_RESPONDERS_PER_LISTENER_DEF;
}

int
HttpServer::get_listener_count(int which) const
{
    return Config::inst()->get_http_listener_count(which);
}

TcpConnection *
HttpServer::create_conn() const
{
    HttpConnection *conn = (HttpConnection*)MemoryManager::alloc_recyclable(RecyclableType::RT_HTTP_CONNECTION);
    conn->forward = false;
    return (TcpConnection*)conn;
}

Task
HttpServer::get_recv_data_task(TcpConnection *conn) const
{
    Task task;

    task.doit = &HttpServer::recv_http_data;
    task.data.pointer = conn;

    return task;
}

// we are in edge-triggered mode, must read all data
bool
HttpServer::recv_http_data(TaskData& data)
{
    size_t buff_size = MemoryManager::get_network_buffer_size() - 8;
    HttpConnection *conn = static_cast<HttpConnection*>(data.pointer);

    Logger::http("recv_http_data: conn=%p", conn->fd, conn);

    char* buff;

    if (conn->buff != nullptr)
    {
        if (conn->state & TCS_NEW)
        {
            buff = conn->buff;
            conn->buff = nullptr;
            conn->state &= ~TCS_NEW;
        }
        else
            return HttpServer::recv_http_data_cont(conn);
    }
    else
    {
        conn->state &= ~TCS_NEW;
        buff = MemoryManager::alloc_network_buffer();
    }

    int fd = conn->fd;
    int len = 0;
    bool conn_error = false;    // try to keep-alive
    bool free_buff = true;

    while (len < buff_size)
    {
        // the MSG_DONTWAIT is not really needed, since
        // the socket itself is non-blocking
        int cnt = recv(fd, &buff[len], buff_size-len, MSG_DONTWAIT);

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
    }

    if (len >= buff_size)
    {
        HttpServer::send_response(conn, 413);
        conn_error = true;
        Logger::debug("received request of size %d, returning 413", len);
    }
    else if (len > 0)
    {
        buff[len] = 0;
        buff[len+1] = ' ';
        buff[len+2] = 0;
        buff[len+3] = '\r';
        buff[len+4] = 0;
        buff[len+5] = ';';
        buff[len+6] = 0;

        Logger::http("Recved request on %p: len=%d\n%s", fd, conn, len, buff);

        conn->request.init();
        parse_header(buff, len, conn->request);

        if (conn->request.header_ok)
        {
            if (UNLIKELY(conn->request.length < 0))
            {
                HttpServer::send_response(conn, 411);
                conn_error = true;
                Logger::http("request length negative, will close connection", fd);
            }
            else if (conn->request.close)
            {
                conn_error = true;
                Logger::http("will close connection", fd);
            }
        }

        conn->response.id = conn->request.id;

        // Check if we have recv'ed the complete request.
        // If not, we need to stop right here, and inform
        // the listener to forward the rest of the request
        // our way.
        if (conn->request.is_complete())
        {
            if (! process_request(conn->request, conn->response))
                conn_error = true;

            free_buff = HttpServer::send_response(conn);

            // This needs to be done AFTER send_response(),
            // or you are risking 2 threads both sending responses on the same fd.
            //conn->buff = nullptr;
        }
        else
        {
            free_buff = false;
            conn_error = false;
            conn->buff = buff;
            conn->offset = len;
        }
    }
    else
    {
        Logger::http("received request of size %d", fd, len);
    }

    if (free_buff)
    {
        conn->buff = nullptr;
        MemoryManager::free_network_buffer(buff);
        conn->response.recycle();
    }

    // closing the fd will deregister it from epoll
    // since we never dup() or fork(); but let's
    // deregister it anyway, just in case.
    if (conn_error)
    {
        //Logger::info("Closing connection (%d)!!!", fd);
        //listener->deregister_with_epoll(fd);
        //close(fd);
        conn->state |= TCS_ERROR;
    }

    int n = --conn->pending_tasks;
    ASSERT(n >= 0);

    if ((n <= 0) && (conn->state & TCS_CLOSED))
        conn->close();

    return false;
}

bool
HttpServer::recv_http_data_cont(HttpConnection *conn)
{
    size_t buff_size = MemoryManager::get_network_buffer_size() - 8;
    int fd = conn->fd;
    char* buff = conn->buff;
    int len = conn->offset;
    bool conn_error = conn->request.close;
    bool free_buff = true;

    while (len < buff_size)
    {
        // the MSG_DONTWAIT is not really needed, since
        // the socket itself is non-blocking
        int cnt = recv(fd, &buff[len], buff_size-len, MSG_DONTWAIT);

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
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) break;
            Logger::warn("recv_cont(%d) failed, errno = %d", fd, errno);
            conn_error = true;
            break;
        }
    }

    if (len >= buff_size)
    {
        HttpServer::send_response(conn, 413);
        conn_error = true;
        Logger::debug("received request of size %d, returning 413", len);
    }
    else if (len > conn->offset)
    {
        // now do something about buff
        buff[len] = 0;
        buff[len+1] = ' ';
        buff[len+2] = 0;
        buff[len+3] = '\r';
        buff[len+4] = 0;
        buff[len+5] = ';';
        buff[len+6] = 0;

        Logger::http("recv-cont'ed: offset=%d, len=%d\n%s", fd,
            conn->offset, len, &buff[conn->offset]);

        // in case this is a new request, instead of second half of
        // the request we've received previously
        int offset = conn->offset;
        if ((buff[offset] == 'P') || (buff[offset] == 'G'))
        {
            if ((std::strncmp(&buff[offset], "GET ", 4) == 0) &&
                (std::strncmp(&buff[offset], "PUT ", 4) == 0) &&
                (std::strncmp(&buff[offset], "POST ", 5) == 0))
            {
                // this appears to be the beginning of a new request
                len -= offset;
                std::memmove(&buff[0], &buff[offset], len+5);
                conn->request.header_ok = false;    // re-parse header
            }
        }

        if (! conn->request.header_ok)
        {
            undo_header(conn->request);
            Logger::http("Re-parse request: %s", fd, buff);
            parse_header(buff, len, conn->request);
        }

        conn->response.id = conn->request.id;

        if (conn->request.header_ok)
        {
            conn->request.complete =
                (conn->request.length <= ((uint64_t)&buff[len] - (uint64_t)conn->request.content));
        }

        // Check if we have recv'ed the complete request.
        // If not, we need to stop right here, and inform
        // the listener to forward the rest of the request
        // our way.
        if (UNLIKELY(! conn->request.is_complete()))
        {
            free_buff = false;
            //conn->buff = buff;
            conn->offset = len;
            Logger::http("request.length = %d, len = %d, offset = %d", fd,
                conn->request.length, len, conn->offset);
        }
        else if (UNLIKELY(! conn->request.header_ok))
        {
            free_buff = HttpServer::send_response(conn, 400);
        }
        else
        {
            Logger::debug("request is finally complete");
            if (! process_request(conn->request, conn->response))
                conn_error = true;
            free_buff = HttpServer::send_response(conn);
        }
    }
    else
    {
        free_buff = false;
        //conn->buff = buff;
        Logger::http("did not receive anything this time", fd);
    }

    if (free_buff)
    {
        conn->buff = nullptr;
        MemoryManager::free_network_buffer(buff);
    }

    // closing the fd will deregister it from epoll
    // since we never dup() or fork(); but let's
    // deregister it anyway, just in case.
    if (conn_error)
    {
        //Logger::info("Closing connection (%d)!!!", fd);
        //deregister_with_epoll(fd);
        //close(fd);
        conn->state |= TCS_ERROR;
    }

    int n = --conn->pending_tasks;
    ASSERT(n >= 0);

    if ((n <= 0) && (conn->state & TCS_CLOSED))
        conn->close();

    return false;
}

void
HttpServer::add_get_handler(const char *path, HttpRequestHandler handler)
{
    if ((path == nullptr) || (handler == nullptr)) return;
    if (get_get_handler(path) != nullptr)
        Logger::error("duplicate get handlers for path: %s", path);
    m_get_handlers[path] = handler;
}

void
HttpServer::add_put_handler(const char *path, HttpRequestHandler handler)
{
    if ((path == nullptr) || (handler == nullptr)) return;
    if (get_put_handler(path) != nullptr)
        Logger::error("duplicate put handlers for path: %s", path);
    m_put_handlers[path] = handler;
}

void
HttpServer::add_post_handler(const char *path, HttpRequestHandler handler)
{
    if ((path == nullptr) || (handler == nullptr)) return;
    if (get_post_handler(path) != nullptr)
        Logger::error("duplicate post handlers for path: %s", path);
    m_post_handlers[path] = handler;
    ASSERT(get_post_handler(path) != nullptr);
}

HttpRequestHandler
HttpServer::get_get_handler(const char *path)
{
    if (path == nullptr) return nullptr;
    auto search = m_get_handlers.find(path);
    if (search == m_get_handlers.end()) return nullptr;
    return search->second;
}

HttpRequestHandler
HttpServer::get_put_handler(const char *path)
{
    if (path == nullptr) return nullptr;
    auto search = m_put_handlers.find(path);
    if (search == m_put_handlers.end()) return nullptr;
    return search->second;
}

HttpRequestHandler
HttpServer::get_post_handler(const char *path)
{
    if (path == nullptr) return nullptr;
    auto search = m_post_handlers.find(path);
    if (search == m_post_handlers.end()) return nullptr;
    return search->second;
}


bool
HttpServer::resend_response(TaskData& data)
{
    if (g_shutdown_requested) return false;
    HttpConnection *conn = (HttpConnection*)data.pointer;
    bool success = send_response(conn);
    if (success)
    {
        if (conn->buff != nullptr)
        {
            MemoryManager::free_network_buffer(conn->buff);
            conn->buff = nullptr;
        }
    }
    --conn->pending_tasks;
    return success;
}

bool
HttpServer::send_response(HttpConnection *conn, uint16_t status)
{
    conn->response.id = conn->request.id;
    conn->response.init(status, HttpContentType::PLAIN);
    return send_response(conn);
}

bool
HttpServer::send_response(HttpConnection *conn)
{
    size_t target;
    ssize_t sent = conn->sent;
    int fd = conn->fd;
    HttpResponse& response = conn->response;
    //size_t max_chunk = 1000000;

    while (sent < response.response_size)
    {
        char *buff = response.response + sent;
        target = response.response_size - sent;

        //ssize_t sent = send(fd, buff, std::min(target,max_chunk), MSG_DONTWAIT);
        ssize_t n = send(fd, buff, target, MSG_DONTWAIT);

        if (n < 0)
        {
            if (errno != EAGAIN)
            {
                conn->state |= TCS_ERROR;
                Logger::warn("send(%d) failed with errno=%d; conn will be closed", fd, errno);
                break;
            }

            if (g_shutdown_requested)
                break;
        }
        else if (n == 0)
        {
            break;
        }
        else
        {
            sent += n;
        }
    }

    if (sent >= response.response_size)
    {
        Logger::http("Sent %d(%d) bytes:\n%s", fd,
            response.response_size, sent, response.response);
        return true;
    }
    else if ((conn->state & TCS_ERROR) == 0)
    {
        // requeue it to try later
        //Task task;
        //task.doit = &HttpServer::resend_response;
        //task.data.pointer = conn;
        conn->listener->resubmit_by_responder('h', conn);
    }

    return false;
}

// parse raw request sitting in the buff into an HttpRequest
bool
HttpServer::parse_header(char *buff, int len, HttpRequest& request)
{
    size_t buff_size = MemoryManager::get_network_buffer_size();
    char *curr1 = buff, *curr2, *curr3;

    // parse 1st line
    for (curr2 = curr1; *curr2 != ' '; curr2++) /* do nothing */;
    *curr2 = 0;
    request.method = curr1;
    curr1 = curr2 + 1;

    if (*curr1 == 0) return false;

    for (curr2 = curr1; *curr2 != ' '; curr2++)
    {
        if (*curr2 == '?')
        {
            *curr2 = 0;
            request.params = curr2 + 1;
        }
    }
    *curr2 = 0;
    request.path = curr1;
    curr1 = curr2 + 1;

    if (*curr1 == 0) return false;

    request.version = curr1;
    curr2 = strchr(curr1, '\r');
    if (curr2 == nullptr) { request.version = nullptr; return false; }
    *curr2 = 0;
    curr1 = curr2 + 1;
    if (*curr1 == '\n') curr1++;

    // parse header
    while (*curr1 != '\r')
    {
        // we only care about the following 3 entries in the header
        if (curr1[0] == 'C')
        {
            if (strncmp(curr1, "Content-Length:", 15) == 0)
            {
                for (curr1 = curr1+15; *curr1 == ' '; curr1++) /* do nothing */;
                request.length = atoi(curr1);
            }
            else if (strncmp(curr1, "Content-Encoding:", 17) == 0)
            {
                for (curr1 = curr1+17; *curr1 == ' '; curr1++) /* do nothing */;
                request.encoding = *curr1;
            }
            else if (strncmp(curr1, "Connection:", 11) == 0)
            {
                for (curr1 = curr1+11; *curr1 == ' '; curr1++) /* do nothing */;
                request.close = (strncmp(curr1, "close", 5) == 0);
            }
        }
        else if (strncmp(curr1, "X-Request-ID:", 13) == 0)
        {
            for (curr1 = curr1+13; *curr1 == ' '; curr1++) /* do nothing */;
            request.id = curr1;
            curr1 = strchr(curr1, '\r');
            if (curr1 == nullptr) { request.id = nullptr; return false; }   // bad request
            *curr1 = 0;
            if ((curr1 - request.id) > MAX_ID_SIZE) request.id[MAX_ID_SIZE] = 0;
            curr1++;
        }

        curr1 = strchr(curr1, '\n');
        if (curr1 == nullptr) return false; // bad request
        curr1++;
    }

    if (*curr1 == '\r') curr1++;
    if (*curr1 == '\n') { curr1++; request.header_ok = true; }

    // parse body
    if (request.header_ok)
    {
        if (request.length > 0)
        {
            request.content = curr1;
            request.complete = ((len - ((uint64_t)curr1 - (uint64_t)buff)) >= request.length);
        }
        else
        {
            request.complete = true;
        }
    }
    else
        request.complete = false;

    return true;
}

void
HttpServer::undo_header(HttpRequest& request)
{
    if (request.method != nullptr)
    {
        request.method[std::strlen(request.method)] = ' ';
        request.method = nullptr;
    }

    if (request.params != nullptr)
    {
        *(request.params-1) = '?';
        request.params = nullptr;
    }

    if (request.path != nullptr)
    {
        request.path[std::strlen(request.path)] = ' ';
        request.path = nullptr;
    }

    if (request.version != nullptr)
    {
        char *p;
        for (p = request.version; *p != 0; p++) /* do nothing */;
        *p = '\r';
        request.version = nullptr;
    }

    if (request.id != nullptr)
    {
        request.id[std::strlen(request.id)] = '\r';
        request.id = nullptr;
    }
}

bool
HttpServer::process_request(HttpRequest& request, HttpResponse& response)
{
    if (request.method == nullptr) return false;
    if (request.path == nullptr) return false;

    HttpRequestHandler handler = nullptr;

    if (strcmp(request.method, "POST") == 0)
    {
        handler = get_post_handler(request.path);
    }
    else if (strcmp(request.method, "GET") == 0)
    {
        handler = get_get_handler(request.path);
    }

    if (handler != nullptr)
    {
        char *buff = nullptr;
        bool success = false;

        try
        {
            if (request.encoding != 0)
            {
                buff = MemoryManager::alloc_network_buffer();
                if (! request.uncompress(buff, MemoryManager::get_network_buffer_size()))
                    throw std::runtime_error("failed to uncompress content");
                Logger::http("%s", 0, request.content);
            }

            success = (*handler)(request, response);
        }
        catch (const std::exception& ex)
        {
            char *buff = response.get_buffer();
            int size = response.get_buffer_size();
            std::strncpy(buff, ex.what(), size-1);
            buff[size-1] = 0;
            response.init(400, HttpContentType::PLAIN, std::strlen(buff));
            Logger::debug("Failed to process http request: %s", ex.what());
        }
        catch (...)
        {
            response.init(400, HttpContentType::PLAIN);
            Logger::debug("Failed to process http request with unknown exception");
        }

        if (buff != nullptr)
            MemoryManager::free_network_buffer(buff);

        return success;
    }
    else
    {
        response.init(404, HttpContentType::PLAIN);
        Logger::error("Unhandled request: %T", &request);
    }

    return false;
}

bool
HttpServer::http_get_api_config_handler(HttpRequest& request, HttpResponse& response)
{
    char *buff = MemoryManager::alloc_network_buffer();
    Config::inst()->c_str(buff, MemoryManager::get_network_buffer_size());
    response.init(200, HttpContentType::JSON, std::strlen(buff), buff);
    MemoryManager::free_network_buffer(buff);
    return true;
}

bool
HttpServer::http_get_api_help_handler(HttpRequest& request, HttpResponse& response)
{
    const char *msg = "available commands: put stats version help diediedie";
    response.init(200, HttpContentType::PLAIN, std::strlen(msg), msg);
    return true;
}

bool
HttpServer::http_get_api_stats_handler(HttpRequest& request, HttpResponse& response)
{
    char *buff = MemoryManager::alloc_network_buffer();
    int len = Stats::collect_stats(buff, MemoryManager::get_network_buffer_size());
    response.init(200, HttpContentType::PLAIN, len, buff);
    MemoryManager::free_network_buffer(buff);
    return true;
}

bool
HttpServer::http_get_api_version_handler(HttpRequest& request, HttpResponse& response)
{
    char buff[32];
    sprintf(buff, "TickTockDB version: %d.%d.%d", TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION);
    response.init(200, HttpContentType::PLAIN, std::strlen(buff), buff);
    return true;
}


HttpResponse::HttpResponse() :
    id(nullptr),
    buffer(nullptr),
    response(nullptr)
{
    init();
    //init(0, HttpContentType::PLAIN);
}

HttpResponse::HttpResponse(uint16_t code, HttpContentType type) :
    buffer(nullptr),
    response((char*)malloc(256))  // TODO: better memory management??? needs to be big enough for max header
{
    init(code, type);
}

HttpResponse::HttpResponse(uint16_t code, HttpContentType type, size_t length, char *body) :
    buffer(nullptr),
    response(nullptr)
{
    init(code, type, length, body);
}

void
HttpResponse::init()
{
    response_size = 0;
    id = nullptr;
    status_code = 0;
    content_type = HttpContentType::JSON;
    content_length = 0;
    response = buffer = nullptr;
    //response = buffer = MemoryManager::alloc_network_buffer();
}

void
HttpResponse::init(uint16_t code, HttpContentType type)
{
    status_code = code;
    content_type = type;
    content_length = 0;

    if (response == nullptr)
    {
        response = buffer = MemoryManager::alloc_network_buffer_small();
        buffer_size = MemoryManager::get_network_buffer_small_size();
        //response = buffer = MemoryManager::alloc_network_buffer();
        //buffer_size = MemoryManager::get_network_buffer_size();
    }

    if (id == nullptr)
    {
        response_size = sprintf(response, "HTTP/1.1 %3d %s%sContent-Length: 0%sContent-Type: %s%s%s",
            (int)status_code, status_code_to_reason(status_code),
            CRLF, CRLF, HTTP_CONTENT_TYPES[type], CRLF, CRLF);
    }
    else
    {
        response_size = sprintf(response, "HTTP/1.1 %3d %s%sContent-Length: 0%sContent-Type: %s%sX-Request-ID: %s%s%s",
            (int)status_code, status_code_to_reason(status_code), CRLF,
            CRLF,
            HTTP_CONTENT_TYPES[type], CRLF,
            id, CRLF, CRLF);
    }
}

void
HttpResponse::init(uint16_t code, HttpContentType type, size_t length)
{
    ASSERT(length > 0);
    ASSERT((100 <= code) && (code <= 999));

    // prepend the HTTP headers
    char *body = get_buffer(length);
    char first = *body; // save
    size_t size = buffer - body;
    size_t digits = std::to_string(length).size();
    const char *reason = status_code_to_reason(code);

    status_code = code;
    content_type = type;
    content_length = length;
    int offset = (id == nullptr) ? 51 : (67 + std::strlen(id));
    response = body - std::strlen(reason) - std::strlen(HTTP_CONTENT_TYPES[type]) - digits - offset;

    ASSERT(buffer <= response);
    ASSERT(std::strlen(reason) <= MAX_REASON_SIZE);
    ASSERT(std::strlen(HTTP_CONTENT_TYPES[type]) <= MAX_CONTENT_TYPE_SIZE);

    if (id == nullptr)
    {
        response_size = std::snprintf(response, (uint32_t)size,
            "HTTP/1.1 %3d %s%sContent-Type: %s%sContent-Length: %d%s%s",
            (int)status_code, reason, CRLF,
            HTTP_CONTENT_TYPES[type], CRLF,
            (int)content_length,
            CRLF, CRLF);
    }
    else
    {
        response_size = std::snprintf(response, (uint32_t)size,
            "HTTP/1.1 %3d %s%sContent-Type: %s%sContent-Length: %d%sX-Request-ID: %s%s%s",
            (int)status_code, reason, CRLF,
            HTTP_CONTENT_TYPES[type], CRLF,
            (int)content_length, CRLF,
            id, CRLF, CRLF);
    }

    *body = first;  // restore

    response_size += length;
    ASSERT(std::strlen(response) == response_size);
}

void
HttpResponse::init(uint16_t code, HttpContentType type, size_t length, const char *body)
{
    //size_t buff_size = MemoryManager::get_network_buffer_size() - 1;

    //ASSERT(buff_size > length);

    status_code = code;
    content_type = type;
    content_length = length;

    get_buffer(length); // make sure buffer is allocated

    if (id == nullptr)
    {
        response_size = snprintf(response, buffer_size,
            "HTTP/1.1 %3d %s%sContent-Length: %d%sContent-Type: %s%s%s%s",
            (int)status_code, status_code_to_reason(status_code), CRLF,
            (int)content_length, CRLF,
            HTTP_CONTENT_TYPES[type],
            CRLF, CRLF,
            (body == nullptr) ? "" : body);
    }
    else
    {
        response_size = snprintf(response, buffer_size,
            "HTTP/1.1 %3d %s%sContent-Length: %d%sContent-Type: %s%sX-Request-ID: %s%s%s%s",
            (int)status_code, status_code_to_reason(status_code), CRLF,
            (int)content_length, CRLF,
            HTTP_CONTENT_TYPES[type], CRLF,
            id, CRLF, CRLF,
            (body == nullptr) ? "" : body);
    }

#ifdef _DEBUG
    if (content_length > 0)
    {
        if ((body == nullptr) || (strlen(body) != content_length))
            Logger::error("invalid response: %T", this);
    }
#endif
}

void
HttpResponse::recycle()
{
    if (buffer != nullptr)
    {
        if (buffer_size == MemoryManager::get_network_buffer_size())
            MemoryManager::free_network_buffer(buffer);
        else
            MemoryManager::free_network_buffer_small(buffer);
        response = buffer = nullptr;
    }
    else if (response != nullptr)
    {
        free(response);
        response = nullptr;
    }
}

char *
HttpResponse::get_buffer()
{
    return get_buffer(MemoryManager::get_network_buffer_size());
}

char *
HttpResponse::get_buffer(std::size_t length)
{
    if ((buffer != nullptr) && (buffer_size <= length))
        recycle();

    if (buffer == nullptr)
    {
        if (MAX_SMALL_PAYLOAD <= length)
        {
            response = buffer = MemoryManager::alloc_network_buffer();
            buffer_size = MemoryManager::get_network_buffer_size();
        }
        else
        {
            response = buffer = MemoryManager::alloc_network_buffer_small();
            buffer_size = MemoryManager::get_network_buffer_small_size();
        }
    }

    return buffer + MAX_HEADER_SIZE;
}

char *
HttpResponse::get_body() const
{
    ASSERT(response != nullptr);
    char *body = std::strstr(response, "\r\n\r\n");
    if (body != nullptr) body += 4;
    return body;
}

HttpResponse::~HttpResponse()
{
    recycle();
}

const char *
HttpResponse::status_code_to_reason(uint16_t status_code)
{
    const char *reason;

    // WARN: Max length of 'reason' string is defined as MAX_REASON_SIZE (32)
    switch (status_code)
    {
        case 200:   reason = "OK"; break;
        case 400:   reason = "Bad Request"; break;
        case 404:   reason = "Not Found"; break;
        case 408:   reason = "Request Timeout"; break;
        case 411:   reason = "Length Required"; break;
        case 413:   reason = "Request Entity Too Large"; break;
        case 500:   reason = "Server Error"; break;
        case 503:   reason = "Out of Memory"; break;
        default:    reason = nullptr; break;
    }

    return reason;
}

const char *
HttpResponse::c_str(char *buff) const
{
    if (buff == nullptr) return EMPTY_STRING;

    snprintf(buff, c_size(), "status=%d content-type:%d content-length:%d response-size:%d response:\n%s",
        status_code, content_type, (int)content_length, response_size, (content_length>0)?response:"");
    return buff;
}


void
HttpRequest::init()
{
    close = false;
    method = nullptr;
    path = nullptr;
    params = nullptr;
    version = nullptr;
    encoding = 0;
    content = nullptr;
    id = nullptr;
    length = 0;
    complete = false;
    forward = g_cluster_enabled;
    header_ok = false;
}

void
HttpRequest::parse_params(JsonMap& pairs)
{
    std::vector<char*> tokens;
    tokenize(params, '&', tokens);

    for (const auto& token: tokens)
    {
        char *key, *value;

        if (tokenize(token, key, value, '='))
        {
            JsonValue *v =
                (JsonValue*)MemoryManager::alloc_recyclable(RecyclableType::RT_JSON_VALUE);
            v->set_value(value);
            pairs[key] = v;
        }
        else
        {
            Logger::warn("Failed to parse uri query params: %s", token);
        }
    }
}

bool
HttpRequest::uncompress(char *buff, size_t size)
{
    if (encoding == 0)
        return true;    // nothing to do
    if (encoding != 'd' && encoding != 'g' && encoding != 'x')
        throw std::runtime_error("unsupported encoding");

    int ret;
    unsigned cnt;
    z_stream strm;

    // allocate inflate state
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;

    if (encoding == 'd')
        ret = inflateInit(&strm);
    else
        ret = inflateInit2(&strm, 16+MAX_WBITS);

    if (ret != Z_OK) return false;

    {
        strm.avail_in = length;
        strm.next_in = (unsigned char*)content;

        /* run inflate() on input until output buffer not full */
        strm.avail_out = size;
        strm.next_out = (unsigned char*)&buff[0];
        ret = inflate(&strm, Z_NO_FLUSH);
        ASSERT(ret != Z_STREAM_ERROR);  /* state not clobbered */

        switch (ret)
        {
            case Z_NEED_DICT:
                ret = Z_DATA_ERROR;     /* and fall through */
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                inflateEnd(&strm);
                return false;
        }

        if (strm.avail_in != 0)
            return false;

        length = size - strm.avail_out;
        content = buff;
        content[length] = 0;
    }

    /* clean up and return */
    inflateEnd(&strm);
    return ret == Z_STREAM_END;
}

const char *
HttpRequest::c_str(char *buff) const
{
    if (buff == nullptr) return EMPTY_STRING;

    snprintf(buff, c_size(), "[%s %s %s %s, close:%s, len:%d, body:%s]",
        method, path, params, version, close?"true":"false", length, (content==nullptr)?"":content);
    return buff;
}


}
