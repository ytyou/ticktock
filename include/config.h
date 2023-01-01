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

#include <map>
#include <memory>
#include "task.h"


// All the recognized configurations in the config file,
// along with their default values, if any.

#define CFG_APPEND_LOG_DIR                      "append.log.dir"
#define CFG_APPEND_LOG_ENABLED                  "append.log.enabled"
#define CFG_APPEND_LOG_ENABLED_DEF              false
#define CFG_APPEND_LOG_FLUSH_FREQUENCY          "append.log.flush.frequency"
#define CFG_APPEND_LOG_FLUSH_FREQUENCY_DEF      "2s"
#define CFG_APPEND_LOG_ROTATION_FREQUENCY       "append.log.rotation.frequency"
#define CFG_APPEND_LOG_ROTATION_FREQUENCY_DEF   "1h"
#define CFG_APPEND_LOG_RETENTION_COUNT          "append.log.retention.count"
#define CFG_APPEND_LOG_RETENTION_COUNT_DEF      2
#define CFG_CLUSTER_SERVERS                     "cluster.servers"
#define CFG_CLUSTER_PARTITIONS                  "cluster.partitions"
#define CFG_CLUSTER_BACKLOG_ROTATION_SIZE       "cluster.backlog.rotation.size"
#define CFG_CLUSTER_BACKLOG_ROTATION_SIZE_DEF   "10mb"
#define CFG_CONFIG_RELOAD_ENABLED               "config.reload.enabled"
#define CFG_CONFIG_RELOAD_ENABLED_DEF           false
#define CFG_CONFIG_RELOAD_FREQUENCY             "config.reload.frequency"
#define CFG_CONFIG_RELOAD_FREQUENCY_DEF         "5min"
#define CFG_HASH_ROTATION_FREQUENCY             "hash.rotation.frequency"
#define CFG_HASH_ROTATION_FREQUENCY_DEF         "5min"
#define CFG_HTTP_LISTENER_COUNT                 "http.listener.count"
#define CFG_HTTP_LISTENER_COUNT_DEF             2
//#define CFG_HTTP_MAX_RETRIES                    "http.max.retries"
//#define CFG_HTTP_MAX_RETRIES_DEF                1024
#define CFG_HTTP_REQUEST_FORMAT                 "http.request.format"
#define CFG_HTTP_REQUEST_FORMAT_DEF             "plain"
#define CFG_HTTP_RESPONDERS_PER_LISTENER        "http.responders.per.listener"
#define CFG_HTTP_RESPONDERS_PER_LISTENER_DEF    4
#define CFG_HTTP_SERVER_PORT                    "http.server.port"
#define CFG_HTTP_SERVER_PORT_DEF                6182
#define CFG_LOG_FILE                            "log.file"
#define CFG_LOG_FILE_DEF                        "/var/log/ticktock.log"
#define CFG_LOG_LEVEL                           "log.level"
#define CFG_LOG_LEVEL_DEF                       "INFO"
#define CFG_LOG_RETENTION_COUNT                 "log.retention.count"
#define CFG_LOG_RETENTION_COUNT_DEF             10
#define CFG_LOG_ROTATION_SIZE                   "log.rotation.size"
#define CFG_LOG_ROTATION_SIZE_DEF               "10mb"
#define CFG_QUERY_EXECUTOR_PARALLEL             "query.executor.parallel"
#define CFG_QUERY_EXECUTOR_PARALLEL_DEF         true
#define CFG_QUERY_EXECUTOR_QUEUE_SIZE           "query.executor.queue.size"
#define CFG_QUERY_EXECUTOR_QUEUE_SIZE_DEF       1024
#define CFG_QUERY_EXECUTOR_THREAD_COUNT         "query.executor.thread.count"
#define CFG_QUERY_EXECUTOR_THREAD_COUNT_DEF     8
#define CFG_STATS_FREQUENCY                     "stats.frequency"
#define CFG_STATS_FREQUENCY_DEF                 "30s"
#define CFG_TCP_CONNECTION_IDLE_TIMEOUT         "tcp.connection.idle.timeout"
#define CFG_TCP_CONNECTION_IDLE_TIMEOUT_DEF     "10min"
#define CFG_TCP_LISTENER_COUNT                  "tcp.listener.count"
#define CFG_TCP_LISTENER_COUNT_DEF              2
#define CFG_TCP_MAX_EPOLL_EVENTS                "tcp.max.epoll.events"
#define CFG_TCP_MAX_EPOLL_EVENTS_DEF            128
#define CFG_TCP_BUFFER_SIZE                     "tcp.buffer.size"
#define CFG_TCP_BUFFER_SIZE_DEF                 "512kb"
#define CFG_TCP_MIN_FILE_DESCRIPTOR             "tcp.min.file.descriptor"
#define CFG_TCP_MIN_FILE_DESCRIPTOR_DEF         30
#define CFG_TCP_MIN_HTTP_STEP                   "tcp.min.http.step"
#define CFG_TCP_MIN_HTTP_STEP_DEF               64
#define CFG_TCP_RESPONDERS_PER_LISTENER         "tcp.responders.per.listener"
#define CFG_TCP_RESPONDERS_PER_LISTENER_DEF     2
#define CFG_TCP_RESPONDERS_QUEUE_SIZE           "tcp.responders.queue.size"
#define CFG_TCP_RESPONDERS_QUEUE_SIZE_DEF       128
#define CFG_TCP_SERVER_PORT                     "tcp.server.port"
#define CFG_TCP_SERVER_PORT_DEF                 6181
#define CFG_TCP_SOCKET_RCVBUF_SIZE              "tcp.socket.rcvbuf.size"
#define CFG_TCP_SOCKET_RCVBUF_SIZE_DEF          "106496b"
#define CFG_TCP_SOCKET_SNDBUF_SIZE              "tcp.socket.sndbuf.size"
#define CFG_TIMER_GRANULARITY                   "timer.granularity"
#define CFG_TIMER_GRANULARITY_DEF               "1s"
#define CFG_TIMER_QUEUE_SIZE                    "timer.queue.size"
#define CFG_TIMER_QUEUE_SIZE_DEF                32
#define CFG_TIMER_THREAD_COUNT                  "timer.thread.count"
#define CFG_TIMER_THREAD_COUNT_DEF              1
#define CFG_TS_LOCK_PROBABILITY                 "ts.lock.probability"
#define CFG_TS_LOCK_PROBABILITY_DEF             0.01
#define CFG_TSDB_ARCHIVE_THRESHOLD              "tsdb.archive.threshold"
#define CFG_TSDB_ARCHIVE_THRESHOLD_DEF          "1w"
#define CFG_TSDB_COMPACT_FREQUENCY              "tsdb.compact.frequency"
#define CFG_TSDB_COMPACT_FREQUENCY_DEF          "2h"
#define CFG_TSDB_COMPRESSOR_VERSION             "tsdb.compressor.version"
#define CFG_TSDB_COMPRESSOR_VERSION_DEF         1
#define CFG_TSDB_DATA_DIR                       "tsdb.data.dir"
#define CFG_TSDB_DATA_DIR_DEF                   "/tmp"
#define CFG_TSDB_OFF_HOUR_BEGIN                 "tsdb.off_hour.begin"
#define CFG_TSDB_OFF_HOUR_BEGIN_DEF             0
#define CFG_TSDB_OFF_HOUR_END                   "tsdb.off_hour.end"
#define CFG_TSDB_OFF_HOUR_END_DEF               5
#define CFG_TSDB_PAGE_COUNT                     "tsdb.page.count"
#define CFG_TSDB_PAGE_COUNT_DEF                 32768
#define CFG_TSDB_PAGE_SIZE                      "tsdb.page.size"
#define CFG_TSDB_FLUSH_FREQUENCY                "tsdb.flush.frequency"
#define CFG_TSDB_FLUSH_FREQUENCY_DEF            "5min"
#define CFG_TSDB_GC_FREQUENCY                   "tsdb.gc.frequency"
#define CFG_TSDB_MAX_DP_LINE                    "tsdb.max.dp.line"
#define CFG_TSDB_MAX_DP_LINE_DEF                256
#define CFG_TSDB_MIN_DISK_SPACE                 "tsdb.min.disk.space"
#define CFG_TSDB_MIN_DISK_SPACE_DEF             4
#define CFG_TSDB_READ_ONLY_THRESHOLD            "tsdb.read_only.threshold"
#define CFG_TSDB_READ_ONLY_THRESHOLD_DEF        "1h"
#define CFG_TSDB_RETENTION_THRESHOLD            "tsdb.retention.threshold"
#define CFG_TSDB_ROTATION_FREQUENCY             "tsdb.rotation.frequency"
#define CFG_TSDB_ROTATION_FREQUENCY_DEF         "1d"
#define CFG_TSDB_SELF_METER_ENABLED             "tsdb.self_meter.enabled"
#define CFG_TSDB_SELF_METER_ENABLED_DEF         false
#define CFG_TSDB_THRASHING_THRESHOLD            "tsdb.thrashing.threshold"
#define CFG_TSDB_THRASHING_THRESHOLD_DEF        "10min"
#define CFG_TSDB_TIMESTAMP_RESOLUTION           "tsdb.timestamp.resolution"
#define CFG_TSDB_TIMESTAMP_RESOLUTION_DEF       "second"
#define CFG_UDP_LISTENER_COUNT                  "udp.listener.count"
#define CFG_UDP_LISTENER_COUNT_DEF              2
#define CFG_UDP_BATCH_SIZE                      "udp.batch.size"
#define CFG_UDP_BATCH_SIZE_DEF                  256
#define CFG_UDP_SERVER_ENABLED                  "udp.server.enabled"
#define CFG_UDP_SERVER_ENABLED_DEF              false
#define CFG_UDP_SERVER_PORT                     "udp.server.port"
#define CFG_UDP_SERVER_PORT_DEF                 6181


namespace tt
{


class Property
{
public:
    Property(const std::string& name, const std::string& value) :
        m_name(name),
        m_value(value)
    {
    }

    inline bool as_bool() const
    {
        return starts_with(m_value, 't') || starts_with(m_value, 'T');
    }

    inline uint64_t as_bytes() const
    {
        return Property::as_bytes(m_value);
    }

    static uint64_t as_bytes(const std::string& value)
    {
        uint64_t bytes = std::stoull(value);
        bytes *= get_bytes_factor(value);
        return bytes;
    }

    inline float as_float() const
    {
        return std::stof(m_value);
    }

    inline int as_int() const
    {
        return std::stoi(m_value);
    }

    inline Timestamp as_time(TimeUnit unit) const
    {
        return Property::as_time(m_value, unit);
    }

    static Timestamp as_time(const std::string& val, TimeUnit unit)
    {
        Timestamp time = std::stoull(val);
        TimeUnit u = to_time_unit(val.c_str(), val.size());
        if (u == TimeUnit::UNKNOWN) throw std::exception();
        return convert_time(time, u, unit);
    }

    inline const std::string& as_str() const
    {
        return m_value;
    }

    inline void set_value(const std::string& value)
    {
        m_value = value;
    }

    inline const std::string& get_name() const
    {
        return m_name;
    }

private:
    std::string m_name;
    std::string m_value;
};


class Config
{
public:
    Config() = delete;

    static void init();     // call this first and once only
    static bool exists(const std::string& name);
    static bool get_bool(const std::string& name, bool def_value);
    static int get_int(const std::string& name);
    static int get_int(const std::string& name, int def_value);
    static float get_float(const std::string& name);
    static float get_float(const std::string& name, float def_value);
    static const std::string& get_str(const std::string& name);
    static const std::string& get_str(const std::string& name, const std::string& def_value);

    static uint64_t get_bytes(const std::string& name);
    static uint64_t get_bytes(const std::string& name, const std::string& def_value);
    static Timestamp get_time(const std::string& name, const TimeUnit unit);
    static Timestamp get_time(const std::string& name, const TimeUnit unit, const std::string& def_value);

    // will override existing value, if any
    static void set_value(const std::string& name, const std::string& value);
    static void set_value_no_lock(const std::string& name, const std::string& value);

    static void add_override(const char *name, const char *value);
    static const char *c_str(char *buff, size_t size);

private:
    static std::shared_ptr<Property> get_property(const std::string& name);
    static std::shared_ptr<Property> get_override(const std::string& name);
    static bool reload(TaskData& data);

    static std::mutex m_lock;
    static std::map<std::string, std::shared_ptr<Property>> m_properties;

    // these came from command-line options; they take precedence over
    // m_properties which came from config file;
    static std::map<std::string, std::shared_ptr<Property>> m_overrides;
};


}
