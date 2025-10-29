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

#include <cstring>
#include "config.h"
#include "json.h"
#include "http.h"
#include "logger.h"
#include "strbuf.h"
#include "tsdb.h"
#ifdef ENABLE_MQTT
#include "mqtt.h"
#endif


namespace tt
{


#ifdef ENABLE_MQTT

// key used in mqtt.settings
static const char *KEY_BROKER = "broker";
static const char *KEY_FORMAT = "format";
static const char *KEY_PORT = "port";
static const char *KEY_TOPIC = "topic";
static const char *KEY_TOPICS = "topics";

static int my_qos = 1;

// static members
std::mutex MQTTClient::m_lock;  // to protect m_clients
std::unordered_map<std::string, MQTTClient*> MQTTClient::m_clients;


MQTTClient::MQTTClient(const char *broker, int port) :
    m_port(port),
    m_broker(broker),
    m_mosquitto(nullptr)
{
}

MQTTClient::~MQTTClient()
{
    if (m_mosquitto != nullptr)
    {
        mosquitto_loop_stop(m_mosquitto, true);
        mosquitto_disconnect(m_mosquitto);
        mosquitto_destroy(m_mosquitto);
        mosquitto_lib_cleanup();
        m_mosquitto = nullptr;
    }
}

void
MQTTClient::init()
{
    m_mosquitto = mosquitto_new("ticktockdb", false, this);

    if (m_mosquitto == nullptr)
    {
        Logger::error("[mqtt] mosquitto_new() failed, errno=%d", errno);
        return;
    }

    // setup callbacks
    mosquitto_connect_callback_set(m_mosquitto, MQTTClient::on_connect);
    mosquitto_message_callback_set(m_mosquitto, MQTTClient::on_message);

    int rc = mosquitto_connect(m_mosquitto, m_broker.c_str(), m_port, 10);  // last # is keepalive (sec)

    if (rc != 0)
        Logger::error("[mqtt] Failed to connect to mosquitto-mqtt-broker %s:%d, rc=%d", m_broker, m_port, rc);
    else
        mosquitto_loop_start(m_mosquitto);
}

void
MQTTClient::add_topic(const char *topic, const char *format)
{
    ASSERT(IF_UNKNOWN != to_input_format(format));
    m_topics.emplace_back(topic, format);
}

bool
MQTTClient::parse_config(bool restart)
{
    bool changed = false;

    if (! Config::inst()->exists(CFG_MQTT_SETTINGS))
    {
        Logger::info("[mqtt] %s config not set; MQTTClient will not run.", CFG_MQTT_SETTINGS);
        return changed;
    }

    // read config
    JsonArray arr;
    StringBuffer strbuf;
    char *settings;

    settings = strbuf.strdup(Config::inst()->get_str(CFG_MQTT_SETTINGS).c_str());
    Logger::info("[mqtt] mqtt.settings = %s", settings);

    // settings: [{"broker":"dock", "port":1883, "topics": [ {"topic":"telegraf/test", "format":"line"}, ...]}, {}]
    JsonParser::parse_array(settings, arr);

    for (auto elem: arr)
    {
        JsonMap& map = elem->to_map();

        // make sure all the required keys exist
        if (map.find(KEY_BROKER) == map.end() || map.find(KEY_PORT) == map.end() || map.find(KEY_TOPICS) == map.end())
        {
            Logger::error("[mqtt] mqtt.settings missing required info; ignored.");
            continue;
        }

        const char *broker = map[KEY_BROKER]->to_string();
        int port = (int)map[KEY_PORT]->to_double();

        MQTTClient *client = new MQTTClient(broker, port);
        JsonArray& topics = map[KEY_TOPICS]->to_array();

        for (auto elem: topics)
        {
            JsonMap& topic = elem->to_map();

            // make sure all the required keys exist
            if (topic.find(KEY_TOPIC) == topic.end() || topic.find(KEY_FORMAT) == topic.end())
            {
                Logger::error("[mqtt] mqtt.settings mal-formatted topic ignored.");
                continue;
            }

            client->add_topic(topic[KEY_TOPIC]->to_string(), topic[KEY_FORMAT]->to_string());
        }

        if (client->has_topic())
        {
            // save client to m_clients
            std::string key(broker);
            key += ':';
            key += std::to_string(port);

            std::lock_guard<std::mutex> guard(m_lock);
            auto search = m_clients.find(key);

            // make sure the key does not already exist before insert
            if (search == m_clients.end())
            {
                m_clients[key] = client;
            }
            else if (restart)
            {
                // override the existing
                MQTTClient *existing = search->second;
                std::vector<MQTTTopic> only_here, only_there;

                if (existing->topic_diff(*client, only_here, only_there))
                {
                    // unsubscribe those in only_here
                    for (auto topic: only_here)
                    {
                        mosquitto_unsubscribe(existing->m_mosquitto, nullptr, topic.m_name.c_str());
                        Logger::info("[mqtt] unsubscribing %s", topic.m_name.c_str());
                    }

                    // subscribe those in only_there
                    for (auto topic: only_there)
                    {
                        mosquitto_subscribe(client->m_mosquitto, nullptr, topic.m_name.c_str(), my_qos);
                        Logger::info("[mqtt] subscribing %s", topic.m_name.c_str());
                    }

                    m_clients[key] = client;
                    delete existing;
                    changed = true;
                }
                else
                    delete client;
            }
            else
            {
                Logger::error("[mqtt] mqtt.settings duplicate broker '%s' ignored.", key.c_str());
                delete client;
            }
        }
        else
        {
            Logger::error("[mqtt] mqtt.settings broker missing valid topic ignored.");
            delete client;
        }
    }

    JsonParser::free_array(arr);
    return changed;
}

bool
MQTTClient::topic_diff(MQTTClient& other, std::vector<MQTTTopic>& only_here, std::vector<MQTTTopic>& only_there)
{
    for (auto& topic_here: m_topics)
    {
        bool found = false;

        for (auto& topic_there: other.m_topics)
        {
            if (topic_here.name_equals(topic_there))
            {
                found = true;
                break;
            }
        }

        if (! found)
            only_here.push_back(topic_here);
    }

    for (auto& topic_there: other.m_topics)
    {
        bool found = false;

        for (auto& topic_here: m_topics)
        {
            if (topic_here.name_equals(topic_there))
            {
                found = true;
                break;
            }
        }

        if (! found)
            only_there.push_back(topic_there);
    }

    return !only_here.empty() || !only_there.empty();
}

void
MQTTClient::on_connect(struct mosquitto *mosq, void *obj, int rc)
{
    MQTTClient *client = (MQTTClient*)obj;

    if (rc != 0)
    {
        Logger::error("[mqtt] on_connect() failed: mosquitto-mqtt-broker %s:%d, rc=%d",
            client->m_broker.c_str(), client->m_port, rc);
        return;
    }

    for (int i = 0; i < client->m_topics.size(); i++)
    {
        MQTTTopic& topic = client->m_topics[i];
        mosquitto_subscribe(mosq, nullptr, topic.m_name.c_str(), my_qos);
        Logger::info("[mqtt] subscribing %s", topic.m_name.c_str());
    }
}

void
MQTTClient::on_message(struct mosquitto *mosq, void *obj, const struct mosquitto_message *msg)
{
    MQTTClient *client = (MQTTClient*)obj;
    std::string msg_topic(msg->topic);
    InputFormat format = IF_UNKNOWN;

    // skip string values
    if ((std::strchr((char*)msg->payload, '"') != nullptr) || (msg->payloadlen >= MemoryManager::get_network_buffer_size()))
        return;

    // use topic to determine msg's format
    for (auto& t: client->m_topics)
    {
        if (msg_topic == t.m_name)
            format = t.m_format;
    }

    if (format == IF_LINE)
    {
        HttpRequest request;
        HttpResponse response;
        char *buff = MemoryManager::alloc_network_buffer();

        strcpy(buff, (char*)msg->payload);
        request.content = buff;
        request.length = msg->payloadlen;

        Tsdb::http_api_write_handler(request, response);

        MemoryManager::free_network_buffer(buff);
    }
    else if (format != IF_UNKNOWN)
    {
        HttpRequest request;
        HttpResponse response;
        char *buff = MemoryManager::alloc_network_buffer();

        strcpy(buff, (char*)msg->payload);
        request.content = buff;
        request.length = msg->payloadlen;
        request.forward = false;

        Tsdb::http_api_put_handler(request, response);

        MemoryManager::free_network_buffer(buff);
    }
}

void
MQTTClient::start()
{
    parse_config(false);

    // no need to lock/protect m_clients during startup
    if (m_clients.empty())
        return;

    // initialize mosquitto library
    mosquitto_lib_init();

    // for each MQTT broker...
    for (const auto& pair: m_clients)
    {
        MQTTClient *client = pair.second;

        client->m_mosquitto = mosquitto_new("ticktockdb", false, client);

        if (client->m_mosquitto == nullptr)
        {
            Logger::error("[mqtt] mosquitto_new() failed, errno=%d", errno);
            continue;
        }

        // setup callbacks
        mosquitto_connect_callback_set(client->m_mosquitto, MQTTClient::on_connect);
        mosquitto_message_callback_set(client->m_mosquitto, MQTTClient::on_message);

        int rc = mosquitto_connect(client->m_mosquitto, client->m_broker.c_str(), client->m_port, 10);  // last # is keepalive (sec)

        if (rc != 0)
            Logger::error("[mqtt] Failed to connect to mosquitto-mqtt-broker %s:%d, rc=%d", client->m_broker, client->m_port, rc);
        else
            mosquitto_loop_start(client->m_mosquitto);
    }
}

// Handle config changes (e.g. subscribe to additional topics)
bool
MQTTClient::restart()
{
    return parse_config(true);
}

void
MQTTClient::stop()
{
    std::lock_guard<std::mutex> guard(m_lock);

    for (const auto& pair: m_clients)
        delete pair.second;
}

#endif


}
