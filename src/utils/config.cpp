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


std::mutex Config::m_lock;
std::map<std::string, std::shared_ptr<Property>> Config::m_properties;


void
Config::init()
{
    TaskData data;  // not used by reload()
    reload(data);

    g_tstamp_resolution_ms = ts_resolution_ms();
    g_cluster_enabled = Config::exists(CFG_CLUSTER_SERVERS);
    g_self_meter_enabled = Config::get_bool(CFG_TSDB_SELF_METER_ENABLED, CFG_TSDB_SELF_METER_ENABLED_DEF);

    // schedule task to reload() periodically
    if (Config::get_bool(CFG_CONFIG_RELOAD_ENABLED, CFG_CONFIG_RELOAD_ENABLED_DEF))
    {
        Task task;
        task.doit = &Config::reload;
        int freq_sec =
            Config::get_time(CFG_CONFIG_RELOAD_FREQUENCY, TimeUnit::SEC, CFG_CONFIG_RELOAD_FREQUENCY_DEF);
        Timer::inst()->add_task(task, freq_sec, "config_reload");
    }
}

bool
Config::reload(TaskData& data)
{
    std::lock_guard<std::mutex> guard(m_lock);

    try
    {
        m_properties.clear();

        std::fstream file(g_config_file);
        std::string line;

        while (std::getline(file, line))
        {
            if (starts_with(line, ';')) continue;   // comments
            if (starts_with(line, '#')) continue;   // comments

            std::tuple<std::string,std::string> kv;
            if (! tokenize(line, kv, '=')) continue;

            std::string key = std::get<0>(kv);
            std::string value = std::get<1>(kv);

            std::shared_ptr<Property> property = get_property(key);

            if (property == nullptr)
            {
                property = std::shared_ptr<Property>(new Property());
                property->name = key;
                property->value = value;
                m_properties[key] = property;
            }
            else
            {
                property->value = value;
            }
        }
    }
    catch (std::exception& ex)
    {
        fprintf(stderr, "failed to read config file %s: %s", g_config_file.c_str(), ex.what());
    }

    return false;
}

void
Config::set_value(const std::string& name, const std::string& value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);

    if (property == nullptr)
    {
        property = std::shared_ptr<Property>(new Property());
        property->name = name;
        property->value = value;
        m_properties[name] = property;
    }
    else
    {
        property->value = value;
    }
}

bool
Config::exists(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    return (get_property(name) != nullptr);
}

bool
Config::get_bool(const std::string& name, bool def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    std::string value = property->value;
    if (value.empty()) value = def_value;
    return starts_with(value, 't') || starts_with(value, 'T');
}

int
Config::get_int(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    std::string value = property->value;
    if (value.empty()) value = property->def_value;
    return std::stoi(value);
}

int
Config::get_int(const std::string& name, int def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    std::string value = property->value;
    if (value.empty()) value = property->def_value;
    return std::stoi(value);
}

const std::string &
Config::get_str(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return EMPTY_STD_STRING;   //throw std::exception();
    if (property->value.empty())
    {
        return property->def_value;
    }
    else
    {
        return property->value;
    }
}

const std::string &
Config::get_str(const std::string& name, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) return def_value;
    if (property->value.empty())
    {
        return property->def_value;
    }
    else
    {
        return property->value;
    }
}

int
Config::get_bytes(const std::string& name)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    std::string value = property->value;
    if (value.empty()) value = property->def_value;
    int bytes = std::stoi(value);
    bytes *= get_bytes_factor(value);
    return bytes;
}

int
Config::get_bytes(const std::string& name, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    std::string value = (property == nullptr) ? def_value : property->value;
    if (value.empty()) throw std::exception();
    int bytes = std::stoi(value);
    bytes *= get_bytes_factor(value);
    return bytes;
}

long
Config::get_time(const std::string& name, TimeUnit unit)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    if (property == nullptr) throw std::exception();
    std::string value = property->value;
    if (value.empty()) value = property->def_value;
    long time = std::stol(value);
    TimeUnit u = to_time_unit(value);
    if (u == TimeUnit::UNKNOWN) throw std::exception();
    return convert_time(time, u, unit);
}

long
Config::get_time(const std::string& name, TimeUnit unit, const std::string& def_value)
{
    std::lock_guard<std::mutex> guard(m_lock);
    std::shared_ptr<Property> property = get_property(name);
    std::string value = (property == nullptr) ? def_value : property->value;
    if (value.empty()) throw std::exception();
    long time = std::stol(value);
    TimeUnit u = to_time_unit(value);
    if (u == TimeUnit::UNKNOWN) throw std::exception();
    return convert_time(time, u, unit);
}

std::shared_ptr<Property>
Config::get_property(const std::string& name)
{
    auto search = m_properties.find(name);
    if (search == m_properties.end()) return nullptr;
    return search->second;
}

const char *
Config::c_str(char *buff, size_t size)
{
    std::lock_guard<std::mutex> guard(m_lock);
    ASSERT(size > 0);
    ASSERT(buff != nullptr);

    int idx = 0;

    for (auto it = m_properties.begin(); (it != m_properties.end()) && (size > 0); it++)
    {
        std::shared_ptr<Property> property = it->second;

        int n = std::snprintf(&buff[idx], size, "%s = %s\n", property->name.c_str(), property->get_effective_value().c_str());

        if (n <= 0)
        {
            buff[0] = 0;
            break;
        }
        else
        {
            idx += n;
            size -= n;
        }
    }

    buff[std::strlen(buff)-1] = 0;  // get rid of last \n
    return buff;
}


}
