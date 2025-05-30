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
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>
#include "global.h"
#include "config.h"
#include "timer.h"
#include "logger.h"
#include "utils.h"


namespace tt
{


Config *Config::m_instance = nullptr;
std::map<std::string, std::shared_ptr<Property>> Config::m_overrides;


void
Config::init()
{
    m_instance = new Config(g_config_file);
    m_instance->load(true);
    m_instance->verify();

    // timezone
    if (m_instance->exists(CFG_TSDB_TIMEZONE))
        g_timezone = m_instance->get_str(CFG_TSDB_TIMEZONE);
    else
    {
        char *tz = ::getenv("TZ");

        if (tz != nullptr)
            g_timezone = tz;
        else
            g_timezone = CFG_TSDB_TIMEZONE_DEF;
    }
    ASSERT(! g_timezone.empty());

    g_tstamp_resolution_ms = ts_resolution_ms();
    g_cluster_enabled = m_instance->exists(CFG_CLUSTER_SERVERS);
    g_self_meter_enabled = m_instance->get_bool(CFG_TSDB_SELF_METER_ENABLED, CFG_TSDB_SELF_METER_ENABLED_DEF);
    g_rollup_enabled = m_instance->get_bool(CFG_TSDB_ROLLUP_ENABLED, CFG_TSDB_ROLLUP_ENABLED_DEF);

    // Load config in data directory to override anything in regular config or overrides.
    // These are the settings that can't be changed once TT starts running.
    std::string data_dir = get_data_dir();
    create_dir(data_dir);   // make sure data directory exists
    Config cfg(data_dir + "/config");

    // verify version of data is at least 0.20.x
    if (file_exists(cfg.m_file_name))
        cfg.load(false);

    if (! is_dir_empty(data_dir))
    {
        const std::string& ver = cfg.get_str("ticktockdb.version");
        if (ver.empty())
            throw std::runtime_error("ticktockdb.version config missing; TickTockDB version mismatch! Please see\nhttps://github.com/ytyou/ticktock/wiki/How-to-migrate-data-from-old-versions-to-new-one%3F");
        std::vector<std::string> tokens;
        if (! tokenize(ver, tokens, '.'))
            throw std::runtime_error("bad ticktockdb.version format; TickTockDB version mismatch! Please see\nhttps://github.com/ytyou/ticktock/wiki/How-to-migrate-data-from-old-versions-to-new-one%3F");
        if (tokens.size() != 3)
            throw std::runtime_error("bad ticktockdb.version format; TickTockDB version mismatch! Please see\nhttps://github.com/ytyou/ticktock/wiki/How-to-migrate-data-from-old-versions-to-new-one%3F");
        if (std::stoi(tokens[0]) <= 0 && std::stoi(tokens[1]) < 20)
            throw std::runtime_error("ticktockdb.version too old; TickTockDB version mismatch! Please see\nhttps://github.com/ytyou/ticktock/wiki/How-to-migrate-data-from-old-versions-to-new-one%3F");
    }

    if (cfg.exists(CFG_TSDB_ROLLUP_BUCKETS))
    {
        m_instance->set_value_no_lock(CFG_TSDB_ROLLUP_BUCKETS, cfg.get_str(CFG_TSDB_ROLLUP_BUCKETS));
    }
    else
    {
        cfg.set_value_no_lock(CFG_TSDB_ROLLUP_BUCKETS, std::to_string(CFG_TSDB_ROLLUP_BUCKETS_DEF));
    }

    cfg.persist();

    // schedule task to reload() periodically
    //if (Config::get_bool(CFG_CONFIG_RELOAD_ENABLED, CFG_CONFIG_RELOAD_ENABLED_DEF))
    //{
        //Task task;
        //task.doit = &Config::reload;
        //int freq_sec =
            //Config::get_time(CFG_CONFIG_RELOAD_FREQUENCY, TimeUnit::SEC, CFG_CONFIG_RELOAD_FREQUENCY_DEF);
        //Timer::inst()->add_task(task, freq_sec, "config_reload");
    //}
}

Config::Config(const std::string& file_name) :
    m_file_name(file_name)
{
}

void
Config::load(bool override)
{
    std::lock_guard<std::mutex> guard(m_lock);

    try
    {
        m_properties.clear();

        std::fstream file(m_file_name, std::ios::in);
        std::string line;

        if (! file.is_open())
            throw std::ios_base::failure("Failed to open config file for read");

        while (std::getline(file, line))
        {
            if (starts_with(line, ';')) continue;   // comments
            if (starts_with(line, '#')) continue;   // comments

            std::tuple<std::string,std::string> kv;
            if (! tokenize(line, kv, '=')) continue;

            std::string key = std::get<0>(kv);
            std::string value = std::get<1>(kv);
            set_value_no_lock(key, value);
        }

        if (override)
        {
            // let command-line options override config file
            for (auto const& override: m_overrides)
                set_value_no_lock(override.first, override.second->as_str());
        }
    }
    catch (std::exception& ex)
    {
        if (! g_quiet)
            fprintf(stderr, "Failed to load config file %s: %s\n", m_file_name.c_str(), ex.what());
    }
}

Config::~Config()
{
    for (auto& prop: m_properties)
        prop.second.reset();
    m_properties.clear();
}

void
Config::verify()
{
    std::unordered_set<std::string> bools;  // configs of bool value
    std::unordered_set<std::string> bytes;  // configs of byte value
    std::unordered_set<std::string> ints;   // configs of int value
    std::unordered_set<std::string> times;  // configs of timestamp value

    // the following configs expect 'bool' value
    bools.emplace(CFG_APPEND_LOG_ENABLED);
    bools.emplace(CFG_CONFIG_RELOAD_ENABLED);
    bools.emplace(CFG_TCP_SERVER_ENABLED);
    bools.emplace(CFG_TSDB_ROLLUP_ENABLED);
    bools.emplace(CFG_TSDB_SELF_METER_ENABLED);
    bools.emplace(CFG_UDP_SERVER_ENABLED);

    // the following configs expect 'byte' value
    bytes.emplace(CFG_CLUSTER_BACKLOG_ROTATION_SIZE);
    bytes.emplace(CFG_LOG_ROTATION_SIZE);
    bytes.emplace(CFG_TCP_BUFFER_SIZE);
    bytes.emplace(CFG_TCP_SOCKET_RCVBUF_SIZE);
    bytes.emplace(CFG_TCP_SOCKET_SNDBUF_SIZE);
    bytes.emplace(CFG_TSDB_PAGE_SIZE);

    // the following configs expect 'int' value
    ints.emplace(CFG_HTTP_LISTENER_COUNT);
    ints.emplace(CFG_HTTP_RESPONDERS_PER_LISTENER);
    ints.emplace(CFG_LOG_RETENTION_COUNT);
    ints.emplace(CFG_TCP_LISTENER_COUNT);
    ints.emplace(CFG_TCP_MAX_EPOLL_EVENTS);
    ints.emplace(CFG_TCP_MIN_FILE_DESCRIPTOR);
    ints.emplace(CFG_TCP_MIN_HTTP_STEP);
    ints.emplace(CFG_TCP_RESPONDERS_PER_LISTENER);
    ints.emplace(CFG_TCP_RESPONDERS_QUEUE_SIZE);
    ints.emplace(CFG_TIMER_QUEUE_SIZE);
    ints.emplace(CFG_TIMER_THREAD_COUNT);
    ints.emplace(CFG_TS_LOCK_PROBABILITY);
    ints.emplace(CFG_TSDB_COMPACT_BATCH_SIZE);
    ints.emplace(CFG_TSDB_COMPRESSOR_PRECISION);
    ints.emplace(CFG_TSDB_COMPRESSOR_VERSION);
    ints.emplace(CFG_TSDB_MAX_DP_LINE);
    ints.emplace(CFG_TSDB_METRIC_BUCKETS);
    ints.emplace(CFG_TSDB_MIN_DISK_SPACE);
    ints.emplace(CFG_TSDB_OFF_HOUR_BEGIN);
    ints.emplace(CFG_TSDB_OFF_HOUR_END);
    ints.emplace(CFG_TSDB_PAGE_COUNT);
    ints.emplace(CFG_TSDB_RETENTION_THRESHOLD);
    ints.emplace(CFG_TSDB_ROLLUP_BUCKETS);
    ints.emplace(CFG_TSDB_ROLLUP_COMPRESSOR_PRECISION);
    ints.emplace(CFG_TSDB_ROLLUP_LEVEL1_COMPRESSOR_VERSION);
    ints.emplace(CFG_TSDB_ROLLUP_LEVEL2_COMPRESSOR_VERSION);
    ints.emplace(CFG_UDP_LISTENER_COUNT);
    ints.emplace(CFG_UDP_BATCH_SIZE);
    ints.emplace(CFG_UDP_SERVER_PORT);

    // the following configs expect 'time' value
    times.emplace(CFG_APPEND_LOG_FLUSH_FREQUENCY);
    times.emplace(CFG_CONFIG_RELOAD_FREQUENCY);
    times.emplace(CFG_STATS_FREQUENCY);
    times.emplace(CFG_TCP_CONNECTION_IDLE_TIMEOUT);
    times.emplace(CFG_TIMER_GRANULARITY);
    times.emplace(CFG_TS_ARCHIVE_THRESHOLD);
    times.emplace(CFG_TSDB_ARCHIVE_THRESHOLD);
    times.emplace(CFG_TSDB_COMPACT_FREQUENCY);
    times.emplace(CFG_TSDB_COMPACT_THRESHOLD);
    times.emplace(CFG_TSDB_FLUSH_FREQUENCY);
    times.emplace(CFG_TSDB_GC_FREQUENCY);
    times.emplace(CFG_TSDB_READ_ONLY_THRESHOLD);
    times.emplace(CFG_TSDB_ROLLUP_FREQUENCY);
    times.emplace(CFG_TSDB_ROLLUP_PAUSE);
    times.emplace(CFG_TSDB_ROLLUP_THRESHOLD);
    times.emplace(CFG_TSDB_ROTATION_FREQUENCY);
    times.emplace(CFG_TSDB_THRASHING_THRESHOLD);

    verify(m_properties, bools, bytes, ints, times);
    verify(m_overrides, bools, bytes, ints, times);
}

void
Config::verify(std::map<std::string, std::shared_ptr<Property>>& props,
    std::unordered_set<std::string>& bools,
    std::unordered_set<std::string>& bytes,
    std::unordered_set<std::string>& ints,
    std::unordered_set<std::string>& times)
{

    for (const auto& prop: props)
    {
        const std::string& name = prop.second->get_name();

        if (bools.find(name) != bools.end())
        {
            const std::string& s = prop.second->as_str();
            if (strcasecmp(s.c_str(), "true") != 0 && strcasecmp(s.c_str(), "false") != 0)
                throw std::runtime_error("Invalid boolean config: " + name);
            continue;
        }

        if (bytes.find(name) != bytes.end())
        {
            const std::string& s = prop.second->as_str();
            for (auto c: s)
            {
                if (std::isdigit(c)) continue;
                auto ch = std::tolower(c);
                if (ch != 'b' && ch != 'k' && ch != 'm' && ch != 'g' && ch != 't')
                    throw std::runtime_error("Invalid byte config: " + name);
            }
            continue;
        }

        if (ints.find(name) != ints.end())
        {
            const std::string& s = prop.second->as_str();
            for (auto c: s)
            {
                if (!std::isdigit(c) && (c != '.') && (c != '-') && (c != '+'))
                    throw std::runtime_error("Invalid number config: " + name);
            }
            continue;
        }

        if (times.find(name) != times.end())
        {
            Timestamp ts = prop.second->as_time(TimeUnit::SEC);
            if (ts == TT_INVALID_TIMESTAMP)
                throw std::runtime_error("Invalid time config: " + name);
            continue;
        }

        // special configs
        if (name.compare(CFG_TSDB_TIMESTAMP_RESOLUTION) == 0)
        {
            const std::string& s = prop.second->as_str();
            if (s.empty())
                throw std::runtime_error("Invalid " + name + " value");
            auto ch = tolower(s[0]);
            if (ch != 's' && ch != 'm')
                throw std::runtime_error("Invalid " + name + " value");
            continue;
        }
    }
}

void
Config::persist()
{
    bool version_exists = exists("ticktockdb.version");
    std::lock_guard<std::mutex> guard(m_lock);
    std::fstream file(m_file_name, std::ios::out | std::ios::trunc);

    if (! version_exists)
        file << "ticktockdb.version = " << TT_MAJOR_VERSION << "." << TT_MINOR_VERSION << "." << TT_PATCH_VERSION << std::endl;

    for (const auto& prop: m_properties)
        file << prop.second->get_name() << " = " << prop.second->as_str() << std::endl;
}

void
Config::append(const std::string& name, const std::string& value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::fstream file(m_file_name, std::ios::out | std::ios::app);
    file << name << " = " << value << std::endl;
}

void
Config::set_value(const std::string& name, const std::string& value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    set_value_no_lock(name, value);
}

void
Config::set_value_no_lock(const std::string& name, const std::string& value)
{
    std::shared_ptr<Property> property = get_property(name);

    if (property == nullptr)
    {
        property = std::shared_ptr<Property>(new Property(name, value));
        m_properties[name] = property;
    }
    else
    {
        property->set_value(value);
    }
}

/* Those configs specified on command-line are considered "overrides".
 * They take precedence over those specified in the config file, which
 * in turn take precedence over defaults.
 */
void
Config::add_override(const char *name, const char *value)
{
    auto search = m_overrides.find(name);
    if (search == m_overrides.end())
        m_overrides[name] = std::shared_ptr<Property>(new Property(name, value));
    else
        search->second->set_value(value);
}

/* Return true if the given config is specified either on the command line,
 * or in the config file.
 */
bool
Config::exists(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return ((get_property(name) != nullptr) || (get_override(name) != nullptr));
}

bool
Config::get_bool(const std::string& name, bool def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    return property->as_bool();
}

int
Config::get_int(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    return property->as_int();
}

int
Config::get_int(const std::string& name, int def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    return property->as_int();
}

float
Config::get_float(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    return property->as_float();
}

float
Config::get_float(const std::string& name, float def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    return property->as_float();
}

const std::string &
Config::get_str(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return EMPTY_STD_STRING;   //throw std::exception();
    return property->as_str();
}

const std::string &
Config::get_str(const std::string& name, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    return property->as_str();
}

uint64_t
Config::get_bytes(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    return property->as_bytes();
}

uint64_t
Config::get_bytes(const std::string& name, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr)
        return Property::as_bytes(def_value);
    else
        return property->as_bytes();
}

Timestamp
Config::get_time(const std::string& name, TimeUnit unit)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return TT_INVALID_TIMESTAMP;
    return property->as_time(unit);
}

Timestamp
Config::get_time(const std::string& name, TimeUnit unit, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    Timestamp tstamp;
    if (property == nullptr)
        tstamp = Property::as_time(def_value, unit);
    else
    {
        tstamp = property->as_time(unit);
        if (tstamp == TT_INVALID_TIMESTAMP)
        {
            tstamp = Property::as_time(def_value, unit);
            Logger::warn("Invalid time config %s ignored, using default %s",
                name.c_str(), def_value.c_str());
        }
    }
    ASSERT(tstamp != TT_INVALID_TIMESTAMP);
    return tstamp;
}

std::shared_ptr<Property>
Config::get_property(const std::string& name)
{
    auto search = m_properties.find(name);
    if (search == m_properties.end()) return nullptr;
    return search->second;
}

std::shared_ptr<Property>
Config::get_override(const std::string& name)
{
    auto search = m_overrides.find(name);
    if (search == m_overrides.end()) return nullptr;
    return search->second;
}

std::string
Config::get_data_dir()
{
    // if 'tsdb.data.dir' is specified, use it
    if (m_instance->exists(CFG_TSDB_DATA_DIR))
        return m_instance->get_str(CFG_TSDB_DATA_DIR);

    // if 'ticktock.home' is specified, use <ticktock.home>/data
    if (m_instance->exists(CFG_TICKTOCK_HOME))
        return m_instance->get_str(CFG_TICKTOCK_HOME) + "/data";

    // use 'cwd'/data
    return g_working_dir + "/data";
}

std::string
Config::get_wal_dir()
{
    return get_data_dir() + "/WAL";
}

std::string
Config::get_log_dir()
{
    // if 'log.file' is specified, use it to find the log dir
    if (m_instance->exists(CFG_LOG_FILE))
    {
        std::string log_file = m_instance->get_str(CFG_LOG_FILE);
        auto const pos = log_file.find_last_of('/');
        if (pos == std::string::npos)
            return g_working_dir;
        return log_file.substr(0, pos);
    }

    // if 'ticktock.home' is specified, use <ticktock.home>/log
    if (m_instance->exists(CFG_TICKTOCK_HOME))
        return m_instance->get_str(CFG_TICKTOCK_HOME) + "/log";

    // use 'cwd'/log
    return g_working_dir + "/log";
}

std::string
Config::get_log_file()
{
    // if 'log.file' is specified, use it
    if (m_instance->exists(CFG_LOG_FILE))
        return m_instance->get_str(CFG_LOG_FILE);

    return get_log_dir() + "/ticktock.log";
}

/* The config could be '6181,6162', '6181,', ',6182', '6182',
 * or nothing at all (not specified).
 */
int
Config::get_count_internal(const char *name, int def_value, int which)
{
    ASSERT(name != nullptr);
    ASSERT((which == 0) || (which == 1));

    int count;

    if (Config::exists(name))
    {
        const std::string& str_count = Config::get_str(name);
        std::tuple<std::string,std::string> kv;

        if (tokenize(str_count, kv, ','))
        {
            // 2 counts present
            std::string& str = (0 == which) ? std::get<0>(kv) : std::get<1>(kv);
            count = str.empty() ? 0 : std::stoi(str);
        }
        else
            count = Config::get_int(name);
    }
    else
    {
        count = def_value;
    }

    return count;
}

int
Config::get_http_listener_count(int which)
{
    return get_count_internal(CFG_HTTP_LISTENER_COUNT, CFG_HTTP_LISTENER_COUNT_DEF, which);
}

int
Config::get_http_responders_per_listener(int which)
{
    return get_count_internal(CFG_HTTP_RESPONDERS_PER_LISTENER, CFG_HTTP_RESPONDERS_PER_LISTENER_DEF, which);
}

int
Config::get_tcp_listener_count(int which)
{
    return get_count_internal(CFG_TCP_LISTENER_COUNT, CFG_TCP_LISTENER_COUNT_DEF, which);
}

int
Config::get_tcp_responders_per_listener(int which)
{
    return get_count_internal(CFG_TCP_RESPONDERS_PER_LISTENER, CFG_TCP_RESPONDERS_PER_LISTENER_DEF, which);
}

/* Return all the configs that are different than their default values.
 * in a human readable, null-terminated string format.
 */
const char *
Config::c_str(char *buff, size_t size)
{
    std::lock_guard<std::mutex> guard(m_lock);
    ASSERT(size > 0);
    ASSERT(buff != nullptr);

    int idx = 0;

    idx += std::snprintf(&buff[idx], size, "{\n");

    for (auto it = m_properties.begin(); (it != m_properties.end()) && (size > idx); it++)
    {
        std::shared_ptr<Property> property = it->second;

        int n = std::snprintf(&buff[idx], size-idx, "  \"%s\": \"%s\",\n",
            property->get_name().c_str(), property->as_str().c_str());

        if (n <= 0)
        {
            buff[0] = 0;
            break;
        }
        else
        {
            idx += n;
        }
    }

    std::snprintf(&buff[idx], size-idx, "}");
    return buff;
}


}
