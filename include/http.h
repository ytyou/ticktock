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

#include <atomic>
#include <chrono>
#include <map>
#include <thread>
#include <unistd.h>
#include "json.h"
#include "stop.h"
#include "task.h"
#include "tcp.h"
#include "memmgr.h"


namespace tt
{


// paths we handle
extern const char *HTTP_API_AGGREGATORS;
extern const char *HTTP_API_CONFIG_FILTERS;
extern const char *HTTP_API_PUT;
extern const char *HTTP_API_QUERY;
extern const char *HTTP_API_STATS;
extern const char *HTTP_API_SUGGEST;
extern const char *HTTP_API_VERSION;

#define MAX_REASON_SIZE         32
#define MAX_CONTENT_TYPE_SIZE   32
// This can only accommodate Content-Type and Content-Length.
#define MAX_HEADER_SIZE         (64 + MAX_REASON_SIZE + MAX_CONTENT_TYPE_SIZE)

enum HttpContentType
{
    HTML = 0,
    JSON = 1,
    PLAIN = 2
};


class HttpResponse
{
public:
    int response_size;
    char *response;     // points to somewhere in the 'buffer', in most cases
    char *buffer;       // original raw buffer
    uint16_t status_code;
    HttpContentType content_type;
    size_t content_length;

    HttpResponse();
    HttpResponse(uint16_t code, HttpContentType type, size_t length, char *body);

    // WARNING: This should never be used except for initializing
    //          global/static/live-forever variables with no content/body.
    HttpResponse(uint16_t code, HttpContentType type);

    inline char *get_buffer() { ASSERT(buffer != nullptr); return buffer + MAX_HEADER_SIZE; }
    inline size_t get_buffer_size() const { return MemoryManager::get_network_buffer_size() - MAX_HEADER_SIZE; }

    void init(uint16_t code, HttpContentType type = HttpContentType::JSON);
    void init(uint16_t code, HttpContentType type, size_t length);
    void init(uint16_t code, HttpContentType type, size_t length, const char *body);
    char *get_body() const;

    const char *c_str(char *buff, size_t size) const;

    virtual ~HttpResponse();

private:
    static const char *status_code_to_reason(uint16_t status_code);
};


class HttpRequest
{
public:
    bool close;     // Connection: close
    const char *method;   // GET, POST, etc.
    const char *path;     // /status
    char *params;   // q=abc
    char *version;  // HTTP/1.1
    char *content;  // body
    int length;     // Content-Length
    bool complete;

    inline bool is_complete() const
    {
        return complete;
    }

    void init();
    void parse_params(JsonMap& pairs);
    const char *c_str(char *buff, size_t size) const;
};


typedef bool (*HttpRequestHandler)(HttpRequest& request, HttpResponse& response);


class HttpConnection : public TcpConnection
{
public:
    HttpRequest request;
    HttpResponse response;
};


class HttpServer : public TcpServer
{
public:
    HttpServer();

    static void init();

    static void add_get_handler(const char *path, HttpRequestHandler handler);
    static void add_put_handler(const char *path, HttpRequestHandler handler);
    static void add_post_handler(const char *path, HttpRequestHandler handler);

    static HttpRequestHandler get_get_handler(const char *path);
    static HttpRequestHandler get_put_handler(const char *path);
    static HttpRequestHandler get_post_handler(const char *path);

protected:
    TcpConnection *create_conn() const;
    Task get_recv_data_task(TcpConnection *conn) const;

    // task func
    static bool recv_http_data(TaskData& data);
    static bool recv_http_data_cont(HttpConnection *conn);

private:
    static bool parse_header(char *buff, int len, HttpRequest& request);
    static bool process_request(HttpRequest& request, HttpResponse& response);
    static bool send_response(int fd, HttpResponse& response);

    // request handlers
    static std::map<const char*,HttpRequestHandler,cstr_less> m_get_handlers;
    static std::map<const char*,HttpRequestHandler,cstr_less> m_put_handlers;
    static std::map<const char*,HttpRequestHandler,cstr_less> m_post_handlers;
};


}
