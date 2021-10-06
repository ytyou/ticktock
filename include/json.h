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

#include <map>
#include <memory>
#include <set>
#include <vector>
#include "recycle.h"
#include "utils.h"


namespace tt
{


class JsonValue;

typedef std::map<const char*,JsonValue*,cstr_less> JsonMap;
typedef std::vector<JsonValue*> JsonArray;


class JsonParser
{
public:
    static void *from_json(char *json); // deprecated???

    static int to_json(std::set<std::string>& strs, char *buff, size_t size);
    static int to_json(JsonArray& arr, char *buff, size_t size);
    static int to_json(JsonMap& map, char *buff, size_t size);

    static char* parse_map(char *json, JsonMap& map, char delim = ':');
    static char* parse_array(char *json, JsonArray& array);

    static char* parse_map_unquoted(char *json, JsonMap& map, char delim = ':');

    static void free_map(JsonMap& map);
    static void free_array(JsonArray& arr);
    static void free_value(JsonValue* value);

private:
    JsonParser() = delete;
    static char* parse_key_value(char *json, std::pair<const char*,JsonValue*>& kv, char delim = ':');
    static char* parse_key_value_unquoted(char *json, std::pair<const char*,JsonValue*>& kv, char delim = ':');
};


enum JsonValueType : unsigned char
{
    JVT_ARRAY,
    JVT_BOOL,
    JVT_DOUBLE,
    JVT_MAP,
    JVT_STRING
};


class JsonValue : public Recyclable
{
public:
    inline JsonValueType get_type() const
    {
        return type;
    }

    inline void set_type(JsonValueType type)
    {
        this->type = type;
    }

    inline void set_value(bool b)
    {
        this->boolean = b;
        this->type = JsonValueType::JVT_BOOL;
    }

    inline void set_value(double dbl)
    {
        this->dbl = dbl;
        this->type = JsonValueType::JVT_DOUBLE;
    }

    inline void set_value(char *str)
    {
        ASSERT(str != nullptr);
        this->str = str;
        this->type = JsonValueType::JVT_STRING;
    }

    inline JsonArray& to_array()
    {
        ASSERT(type == JsonValueType::JVT_ARRAY);
        return arr;
    }

    bool to_bool() const
    {
        if (type == JsonValueType::JVT_BOOL)
        {
            return boolean;
        }
        else
        {
            ASSERT(type == JsonValueType::JVT_STRING);
            ASSERT(str != nullptr);
            return ((*str == 't') || (*str == 'T'));
        }
    }

    inline double to_double() const
    {
        ASSERT(type == JsonValueType::JVT_DOUBLE);
        return dbl;
    }

    inline JsonMap& to_map()
    {
        ASSERT(type == JsonValueType::JVT_MAP);
        return map;
    }

    inline const char *to_string() const
    {
        ASSERT(type == JsonValueType::JVT_STRING);
        ASSERT(str != nullptr);
        return str;
    }

private:
    friend class JsonParser;

    JsonValueType type; // this determines which one of the following is used
    bool boolean;
    double dbl;
    char *str;      // we don't own the memory
    JsonMap map;
    JsonArray arr;
};


}
