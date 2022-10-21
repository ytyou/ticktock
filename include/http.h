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

#include <atomic>
#include <chrono>
#include <map>
#include <thread>
#include <unistd.h>
#include "json.h"
#include "serial.h"
#include "stop.h"
#include "task.h"
#include "tcp.h"
#include "memmgr.h"


namespace tt
{


// paths we handle
extern const char *HTTP_API_AGGREGATORS;
extern const char *HTTP_API_CONFIG;
extern const char *HTTP_API_CONFIG_FILTERS;
extern const char *HTTP_API_PUT;
extern const char *HTTP_API_QUERY;
extern const char *HTTP_API_STATS;
extern const char *HTTP_API_SUGGEST;
extern const char *HTTP_API_VERSION;

#define MAX_ID_SIZE             64
#define MAX_REASON_SIZE         32
#define MAX_CONTENT_TYPE_SIZE   32
// This can only accommodate Content-Type, Content-Length, and X-Request-ID.
#define MAX_HEADER_SIZE         (70 + MAX_ID_SIZE + MAX_REASON_SIZE + MAX_CONTENT_TYPE_SIZE)

enum HttpContentType : unsigned char
{
    HTML = 0,
    JSON = 1,
    PLAIN = 2
};


class HttpResponse : public Serializable
{
public:
    int response_size;
    char *response;     // points to somewhere in the 'buffer', in most cases
    char *id;           // X-Request-ID
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

    void init();
    void init(uint16_t code, HttpContentType type = HttpContentType::JSON);
    void init(uint16_t code, HttpContentType type, size_t length);
    void init(uint16_t code, HttpContentType type, size_t length, const char *body);
    char *get_body() const;

    inline size_t c_size() const override { return 8192; }
    const char *c_str(char *buff) const override;

    void recycle();
    virtual ~HttpResponse();

private:
    char *buffer;       // original raw buffer

    static const char *status_code_to_reason(uint16_t status_code);
};


class HttpRequest : public Serializable
{
public:
    bool close;     // Connection: close
    const char *method;   // GET, POST, etc.
    const char *path;     // /status
    char *params;   // q=abc
    char *version;  // HTTP/1.1
    char *content;  // body
    char *id;       // X-Request-ID
    int length;     // Content-Length
    bool complete;
    bool forward;
    bool header_ok; // header parsed ok?

    inline bool is_complete() const
    {
        return complete;
    }

    void init();
    void parse_params(JsonMap& pairs);

    inline size_t c_size() const override { return 4096; }
    const char *c_str(char *buff) const override;
};


typedef bool (*HttpRequestHandler)(HttpRequest& request, HttpResponse& response);


class HttpConnection : public TcpConnection
{
public:
    HttpRequest request;
    HttpResponse response;
    ssize_t sent;   // number of bytes of response already sent

    void init() override
    {
        sent = 0;
        request.init();
        response.init();
        TcpConnection::init();
    }

    bool recycle() override
    {
        response.recycle();
        return TcpConnection::recycle();
    }
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

    // individual request handlers
    static bool http_get_api_config_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_help_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_stats_handler(HttpRequest& request, HttpResponse& response);
    static bool http_get_api_version_handler(HttpRequest& request, HttpResponse& response);

    static bool resend_response(TaskData& data);

    virtual inline const char *get_name() const { return "http"; }

protected:
    TcpConnection *create_conn() const override;
    Task get_recv_data_task(TcpConnection *conn) const override;
    int get_responders_per_listener() const override;

    // task func
    static bool recv_http_data(TaskData& data);
    static bool recv_http_data_cont(HttpConnection *conn);

private:
    static bool parse_header(char *buff, int len, HttpRequest& request);
    static bool process_request(HttpRequest& request, HttpResponse& response);
    static bool send_response(HttpConnection *conn);
    static bool send_response(HttpConnection *conn, uint16_t status);

    // request handlers
    static std::map<const char*,HttpRequestHandler,cstr_less> m_get_handlers;
    static std::map<const char*,HttpRequestHandler,cstr_less> m_put_handlers;
    static std::map<const char*,HttpRequestHandler,cstr_less> m_post_handlers;

    //static int m_max_resend;
};


}
