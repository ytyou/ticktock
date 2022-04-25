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

#include <vector>
#include <condition_variable>
#include "http.h"
#include "recycle.h"
#include "stop.h"


namespace tt
{


class ReplicationCheckPoint
{
public:
    //ReplicationCheckPoint(uint64_t first);

private:
    std::string m_stream_id;
    uint64_t m_first;   // incremented after each restart
    uint64_t m_second;  // incremented from 0 after each restart
};


// file backed buffer
class ReplicationBuffer : public Recyclable
{
public:
    ReplicationBuffer();
    ~ReplicationBuffer();

    size_t append(const char *data, size_t len);
    void set_check_point() {};

    // Recyclable methods
    void init() override;
    bool recycle() override;

private:
    char *m_buff;
    size_t m_buff_size;
    size_t m_data_size;
    std::mutex m_lock;
    ReplicationCheckPoint m_check_point;

    bool m_in_mem;
};


class ReplicationCursor
{
private:
    ReplicationBuffer *m_buffer;
    int32_t m_offset;
};


class ReplicationStream
{
public:
    ReplicationStream(int32_t id);
    ~ReplicationStream();

    bool append(const char *data, size_t len);
    int32_t get_data(ReplicationCursor& cursor, const char* &buff);
    void prune(ReplicationCheckPoint& check_point);
    int32_t get_rep(char *buff, size_t len);

    int32_t get_buffer_count(bool in_mem);

private:
    void alloc_buffer();

    int32_t m_id;
    std::mutex m_lock;
    std::atomic<uint64_t> m_check_point;
    std::condition_variable m_signal;

    int32_t m_in_mem_count;
    int32_t m_total_count;

    ReplicationBuffer *m_buffers;
    ReplicationBuffer *m_buff_last;
};


class ReplicationServer : public Stoppable
{
public:
    ReplicationServer(int32_t id, const char *address, int32_t tcp_port, int32_t http_port);
    ~ReplicationServer();

private:
    void do_work(ReplicationStream *stream);
    void connect(ReplicationStream *stream);
    bool send(const char *buff, int32_t len);
    void close();

    int32_t m_id;
    int32_t m_tcp_port;
    int32_t m_http_port;
    std::string m_address;

    int m_fd;
    ReplicationCursor m_cursor;   // cursor into ReplicationStream
    std::vector<std::thread> m_workers;
};


// persisted on disk
class ReplicationState
{
};


class ReplicationManager
{
public:
    static void init();
    static int32_t get_id() { return m_id; }
    static int64_t get_start() { return m_start; }
    static int32_t get_max_buffers() { return m_max_buff; }

    static inline bool is_local() { return m_local; }
    static inline bool is_remote() { return m_remote; }

    static std::vector<ReplicationStream*>& get_streams() { return m_streams; }
    static int32_t handshake(char *rep, HttpResponse& response);    // used by replica to exchange replication info with leader
    static bool forward(HttpRequest& request, HttpResponse& response);
    static bool flush();    // used by replicas
    static int32_t checkpoint(char *cp, HttpResponse& response);    // leader -> replica
    static void shutdown(bool wait);

    static int32_t get_buffer_count(bool in_mem);

private:
    static bool m_local;    // store data locally?
    static bool m_remote;   // forward data to replicas?
    static int32_t m_id;    // our unique ID among all replicas/leaders
    static int64_t m_start; // number of restarts
    static int32_t m_max_buff;  // max no. in-memory buffers allowed, per stream

    static std::vector<ReplicationStream*> m_streams;
    static std::vector<ReplicationServer*> m_replicas;
};


}
