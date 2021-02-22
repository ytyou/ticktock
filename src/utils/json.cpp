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
#include <cctype>
#include "json.h"
#include "memmgr.h"
#include "utils.h"


namespace tt
{


void *
JsonParser::from_json(char *json)
{
    while (std::isspace(*json)) json++;

    if (*json == '{')
    {
        JsonMap *map = new JsonMap;
        parse_map(json, *map);
        return static_cast<void*>(map);
    }
    else
    {
        ASSERT(*json == '[');
        JsonArray *array = new JsonArray;
        parse_array(json, *array);
        return static_cast<void*>(array);
    }
}

char *
JsonParser::parse_map(char *json, JsonMap& map, char delim)
{
    ASSERT(json != nullptr);

    while (std::isspace(*json)) json++;
    ASSERT(*json == '{');
    if (*json == '{') json++;

    while (*json != 0)
    {
        while (std::isspace(*json)) json++;
        if (*json == '}') { json++; break; }
        if (*json == ',') json++;
        std::pair<const char*,JsonValue*> pair;
        json = parse_key_value(json, pair, delim);
        ASSERT(map.find(pair.first) == map.end());
        map[pair.first] = pair.second;
    }

    return json;
}

char *
JsonParser::parse_array(char *json, JsonArray& array)
{
    ASSERT(json != nullptr);

    while (std::isspace(*json)) json++;
    ASSERT(*json == '[');
    if (*json == '[') json++;

    while (*json != 0)
    {
        while (std::isspace(*json)) json++;
        if (*json == ']') { json++; break; }
        if (*json == ',') json++;
        while (std::isspace(*json)) json++;

        JsonValue *value =
            (JsonValue*)MemoryManager::alloc_recyclable(RecyclableType::RT_JSON_VALUE);
        ASSERT(value != nullptr);

        if (*json == '"')
        {
            char *str = ++json;
            while (*json != '"') json++;
            *json = 0;
            json++;
            value->set_value(str);
        }
        else if (*json == '[')
        {
            json = parse_array(json, value->arr);
            value->set_type(JsonValueType::JVT_ARRAY);
        }
        else if (*json == '{')
        {
            json = parse_map(json, value->map);
            value->set_type(JsonValueType::JVT_MAP);
        }
        else if (std::strncmp(json, "true", 4) == 0)
        {
            value->set_value(true);
            json += 4;
        }
        else if (std::strncmp(json, "false", 5) == 0)
        {
            value->set_value(false);
            json += 5;
        }
        else
        {
            double dbl = atof(json);
            value->set_value(dbl);
            do { json++; } while ((*json != ',') && (*json != ']') && (*json != '}'));
        }

        array.push_back(value);
    }

    return json;
}

char *
JsonParser::parse_key_value(char *json, std::pair<const char*,JsonValue*>& pair, char delim)
{
    ASSERT(json != nullptr);

    while (std::isspace(*json)) json++;
    ASSERT(*json == '"');
    char *key = ++json;
    while (*json != '"') json++;
    *json = 0;
    do { json++; } while (std::isspace(*json));
    ASSERT(*json == delim);
    do { json++; } while (std::isspace(*json));

    JsonValue *value =
        (JsonValue*)MemoryManager::alloc_recyclable(RecyclableType::RT_JSON_VALUE);
    ASSERT(value != nullptr);

    if (*json == '"')
    {
        value->set_value(++json);
        while (*json != '"') json++;
        *json = 0;
        json++;
    }
    else if (*json == '{')
    {
        json = parse_map(json, value->map);
        value->set_type(JsonValueType::JVT_MAP);
    }
    else if (*json == '[')
    {
        json = parse_array(json, value->arr);
        value->set_type(JsonValueType::JVT_ARRAY);
    }
    else if (std::strncmp(json, "true", 4) == 0)
    {
        value->set_value(true);
        json += 4;
    }
    else if (std::strncmp(json, "false", 5) == 0)
    {
        value->set_value(false);
        json += 5;
    }
    else
    {
        double dbl = atof(json);
        value->set_value(dbl);
        do { json++; } while ((*json != ',') && (*json != ']') && (*json != '}'));
    }

    pair.first = key;
    pair.second = value;

    return json;
}

char *
JsonParser::parse_map_unquoted(char *json, JsonMap& map, char delim)
{
    ASSERT(json != nullptr);

    while (std::isspace(*json)) json++;
    ASSERT(*json == '{');
    if (*json == '{') json++;

    while (*json != 0)
    {
        while (std::isspace(*json)) json++;
        if (*json == '}') { *json = 0; json++; break; }
        if (*json == ',') { *json = 0; json++; }
        std::pair<const char*,JsonValue*> pair;
        json = parse_key_value_unquoted(json, pair, delim);
        map[pair.first] = pair.second;
    }

    return json;
}

char *
JsonParser::parse_key_value_unquoted(char *json, std::pair<const char*,JsonValue*>& pair, char delim)
{
    ASSERT(json != nullptr);

    while (std::isspace(*json)) json++;
    char *key = json++;
    while ((*json != delim) && !std::isspace(*json)) json++;
    *json = 0;
    do { json++; } while (std::isspace(*json) || (*json == delim));

    JsonValue *value =
        (JsonValue*)MemoryManager::alloc_recyclable(RecyclableType::RT_JSON_VALUE);
    ASSERT(value != nullptr);

    if (*json == '{')
    {
        json = parse_map_unquoted(json, value->map);
        value->set_type(JsonValueType::JVT_MAP);
    }
    else if (*json == '[')
    {
        // TODO: parse_array_unquoted()!
        json = parse_array(json, value->arr);
        value->set_type(JsonValueType::JVT_ARRAY);
    }
    else if (std::strncmp(json, "true", 4) == 0)
    {
        value->set_value(true);
        json += 4;
    }
    else if (std::strncmp(json, "false", 5) == 0)
    {
        value->set_value(false);
        json += 5;
    }
    else if (std::isdigit(*json))
    {
        double dbl = atof(json);
        value->set_value(dbl);
        do { json++; } while ((*json != ',') && (*json != ']') && (*json != '}'));
    }
    else
    {
        value->set_value(json);
        while ((*json != delim) && !std::isspace(*json) && (*json != ',') && (*json != ']') && (*json != '}')) json++;
        if ((*json != ',') && (*json != ']') && (*json != '}'))
        {
            *json = 0;
            json++;
        }
    }

    pair.first = key;
    pair.second = value;

    return json;
}

int
JsonParser::to_json(std::set<std::string>& strs, char *buff, size_t size)
{
    int n = 1;

    ASSERT(buff != nullptr);
    ASSERT(size >= 3);
    buff[0] = '[';

    for (const std::string& str: strs)
    {
        // remove any double-quote, if any
        std::string s(str);
        s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
        n += snprintf(buff+n, size-n, "\"%s\",", s.c_str());
    }

    if (n == 1) n++;
    if (n < size)
    {
        buff[n-1] = ']';
        buff[n] = 0;
    }
    else
    {
        // Log error?
        n = size;
        buff[n-1] = 0;
    }

    return n;
}

int
JsonParser::to_json(JsonMap& map, char *buff, size_t size)
{
    int n = 1;

    ASSERT(buff != nullptr);
    ASSERT(size >= 3);
    buff[0] = '{';

    for (std::pair<const char*,JsonValue*> kv : map)
    {
        n += snprintf(buff+n, size-n, "\"%s\":", kv.first);

        switch (kv.second->type)
        {
            case JsonValueType::JVT_ARRAY:
                n += JsonParser::to_json(kv.second->arr, buff+n, size-n);
                break;

            case JsonValueType::JVT_MAP:
                n += JsonParser::to_json(kv.second->map, buff+n, size-n);
                break;

            case JsonValueType::JVT_BOOL:
                n += snprintf(buff+n, size-n, "%s", kv.second->to_bool() ? "true" : "false");
                break;

            case JsonValueType::JVT_DOUBLE:
                n += snprintf(buff+n, size-n, "%f", kv.second->to_double());
                break;

            case JsonValueType::JVT_STRING:
                n += snprintf(buff+n, size-n, "\"%s\"", kv.second->to_string());
                break;

            default:
                break;
        }

        n += snprintf(buff+n, size-n, ",");
    }

    if (n == 1) n++;
    if (n < size)
    {
        buff[n-1] = '}';
        buff[n] = 0;
    }
    else
    {
        // Log error?
        n = size;
        buff[n-1] = 0;
    }

    return n;
}

int
JsonParser::to_json(JsonArray& arr, char *buff, size_t size)
{
    int n = 1;

    ASSERT(buff != nullptr);
    ASSERT(size >= 3);
    buff[0] = '[';

    for (JsonValue *val : arr)
    {
        switch (val->type)
        {
            case JsonValueType::JVT_ARRAY:
                n += JsonParser::to_json(val->arr, buff+n, size-n);
                break;

            case JsonValueType::JVT_MAP:
                n += JsonParser::to_json(val->map, buff+n, size-n);
                break;

            case JsonValueType::JVT_BOOL:
                n += snprintf(buff+n, size-n, "%s", val->to_bool() ? "true" : "false");
                break;

            case JsonValueType::JVT_DOUBLE:
                n += snprintf(buff+n, size-n, "%f", val->to_double());
                break;

            case JsonValueType::JVT_STRING:
                n += snprintf(buff+n, size-n, "\"%s\"", val->to_string());
                break;

            default:
                break;
        }

        n += snprintf(buff+n, size-n, ",");
    }

    if (n == 1) n++;
    if (n < size)
    {
        buff[n-1] = ']';
        buff[n] = 0;
    }
    else
    {
        // Log error?
        n = size;
        buff[n-1] = 0;
    }

    return n;
}

void
JsonParser::free_value(JsonValue *value)
{
    ASSERT(value != nullptr);

    switch (value->type)
    {
        case JsonValueType::JVT_ARRAY:
            JsonParser::free_array(value->arr);
            break;

        case JsonValueType::JVT_MAP:
            JsonParser::free_map(value->map);
            break;

        case JsonValueType::JVT_BOOL:
        case JsonValueType::JVT_DOUBLE:
        case JsonValueType::JVT_STRING:
        default:
            break;
    }

    ASSERT(value->recyclable_type() == RecyclableType::RT_JSON_VALUE);
    MemoryManager::free_recyclable(value);
}

void
JsonParser::free_map(JsonMap& map)
{
    for (auto it = map.begin(); it != map.end(); it++)
    {
        free_value(it->second);
    }

    map.clear();
}

void
JsonParser::free_array(JsonArray& arr)
{
    for (JsonValue *value: arr)
    {
        free_value(value);
    }

    arr.clear();
    arr.shrink_to_fit();
}


}
