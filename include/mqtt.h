/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <mosquitto.h>
#include "type.h"


namespace tt
{


class MQTTTopic
{
public:
    MQTTTopic(const char *name, const char *format) : m_name(name) { m_format = to_input_format(format); }
    bool name_equals(MQTTTopic& other) { return m_name == other.m_name; }

    std::string m_name;
    InputFormat m_format;
};


class MQTTClient
{
public:
    static void start();
    static void stop();
    static bool restart();  // return true if clients were updated; false if nothing changed

    void init();
    bool has_topic() { return ! m_topics.empty(); }
    void add_topic(const char *topic, const char *format);

private:
    MQTTClient(const char *broker, int port);
    ~MQTTClient();

    // return true if different
    bool topic_diff(MQTTClient& other, std::vector<MQTTTopic>& only_here, std::vector<MQTTTopic>& only_there);

    static bool parse_config(bool restart); // return true if clients were updated; false if nothing changed
    static void on_connect(struct mosquitto *mosq, void *obj, int rc);
    static void on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg);
    static void on_message_line(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg);

    int m_port;
    std::string m_broker;   // hostname
    std::vector<MQTTTopic> m_topics;
    struct mosquitto *m_mosquitto;

    // all the MQTTClients, indexed by "broker:port"
    static std::unordered_map<std::string, MQTTClient*> m_clients;
    static std::mutex m_lock;   // to protect m_clients
};


}
