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

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "dp.h"
#include "global.h"
#include "memmgr.h"
#include "logger.h"


namespace tt
{


DataPoint::DataPoint() :
    m_timestamp(0L),
    m_value(0.0),
    m_metric(nullptr),
    m_raw_tags(nullptr),
    TagOwner(false)
{
}

DataPoint::DataPoint(Timestamp ts, double value) :
    m_timestamp(ts),
    m_value(value),
    m_metric(nullptr),
    m_raw_tags(nullptr),
    TagOwner(false)
{
}

void
DataPoint::init(Timestamp ts, double value)
{
    m_timestamp = ts;
    m_value = value;
    m_metric = nullptr;
    m_raw_tags = nullptr;
    TagOwner::init(false);
}

char *
DataPoint::from_http(char *http)
{
    if (http == nullptr) return nullptr;

    char *curr1 = http, *curr2, *curr3;

    // timestamp
    for (curr2 = curr1+10; *curr2 != ' '; curr2++) /* do nothing */;
    m_timestamp = std::atol(curr1);
    ASSERT(g_tstamp_resolution_ms ? is_ms(m_timestamp) : is_sec(m_timestamp));
    curr1 = curr2 + 1;
    if (*curr1 == 0) return nullptr;

    // value
    for (curr2 = curr1; *curr2 != ' '; curr2++) /* do nothing */;
    m_value = std::atof(curr1);
    curr1 = curr2 + 1;
    if (*curr1 == 0) return nullptr;

    // tags
    while ((*curr1 != ';') && (*curr1 != 0))
    {
        for (curr2 = curr1, curr3 = nullptr; *curr2 != ' '; curr2++)
        {
            if (*curr2 == '=')
            {
                *curr2 = 0;
                curr3 = curr2 + 1;
            }
        }
        *curr2 = 0;
        if (curr3 == nullptr)
        {
            // TODO: this is an attribute
        }
        else
        {
            add_tag(curr1, curr3);
        }

        curr1 = curr2 + 1;
    }

    return curr1;
}

char *
DataPoint::from_json(char* json)
{
    if (json == nullptr) return nullptr;
    while (*json != '{') json++;

    for (json++; *json != '}' && *json != 0; )
    {
        char *key;
        json = next_word(json, key);
        if (json == nullptr) return nullptr;

        if (strcmp(key, "metric") == 0)
        {
            char *value;
            json = next_word(json, value);
            if (json == nullptr) return nullptr;
            add_tag(key, value);
            set_metric(value);
        }
        else if (strcmp(key, "tags") == 0)
        {
            while ((*json != '{') && (*json != 0)) json++;
            json = next_tags(json);
            if (json == nullptr) return nullptr;
        }
        else if (strcmp(key, "timestamp") == 0)
        {
            json = next_long(json, m_timestamp);
            ASSERT(g_tstamp_resolution_ms ? is_ms(m_timestamp) : is_sec(m_timestamp));
        }
        else if (strcmp(key, "value") == 0)
        {
            json = next_double(json, m_value);
        }
        else
        {
            return nullptr;
        }

        while (isspace(*json)) json++;
    }

    return json + 1;
}

// Input format:
//   metric timestamp value tag1=val1 tag2=val2 ...\n
// Example:
//   proc.stat.processes 1606091337 73914 host=centos0\n
// Output:
//   return true if parses ok
bool
DataPoint::from_plain(char* &text)
{
    m_metric = text;
    if (*text == '"')
    {
        m_metric++;
        do
        {
            if (*++text == ' ') *text = '_';
        } while ((*text != '"') && (*text != '\n'));
        *text++ = 0;
    }
    else
        text = strchr(text, ' ');
    if (text == nullptr) { m_metric = nullptr; return false; }
    *text++ = 0;
    m_timestamp = std::atol(text);
    ASSERT(g_tstamp_resolution_ms ? is_ms(m_timestamp) : is_sec(m_timestamp));
    text = strchr(text, ' ');
    if (text == nullptr) { m_metric = nullptr; return false; }
    m_value = std::atof(++text);
    while ((*text != ' ') && (*text != '\n')) text++;
    if (*text == '\n') { text++; return true; }
    m_raw_tags = ++text;
    text = strchr(text, '\n');
    if (text == nullptr) { m_raw_tags = nullptr; return false; }
    *text++ = 0;
    if (*(text-2) == '\r') *(text-2) = 0;
    return true;
}

char *
DataPoint::next_word(char* json, char* &word)
{
    char *curr = strchr(json, '"');
    if (curr == nullptr) return nullptr;
    curr++;
    word = curr;
    curr = strchr(curr, '"');
    if (curr == nullptr) return nullptr;
    *curr = 0;
    for (curr++; std::isspace(*curr); curr++) /* do nothing */;
    return curr;
}

char *
DataPoint::next_long(char* json, Timestamp &number)
{
    while (! std::isdigit(*json) && (*json != 0) && (*json != '\n')) json++;
    number = std::atol(json);
    //number = (g_tstamp_resolution_ms ? to_ms(number) : to_sec(number));
    while (std::isdigit(*json)) json++;
    if (*json == '"') json++;   // in case the number is quoted
    return json;
}

char *
DataPoint::next_double(char* json, double &number)
{
    while (! std::isdigit(*json) && (*json != '.') && (*json != '+') && (*json != '-') && (*json != '\n')) json++;
    number = std::atof(json);
    while (std::isdigit(*json) || (*json == '.') || (*json == '+') || (*json == '-') || (*json == 'e')) json++;
    if (*json == '"') json++;   // in case the number is quoted
    return json;
}

char *
DataPoint::next_tags(char* json)
{
    for (json++; (*json != '}') && (*json != 0); )
    {
        char *name, *value;
        json = next_word(json, name);
        if (json == nullptr) return nullptr;
        ASSERT(name != nullptr);
        ASSERT(*name != ',');
        json = next_word(json, value);
        if (json == nullptr) return nullptr;
        ASSERT(value != nullptr);
        ASSERT(*value != ':');
        add_tag(name, value);
        while (std::isspace(*json)) json++;
    }

    if (*json == '}') json++;

    return json;
}

// TODO: This is not safe. If input data is mal-formatted, e.g. tag without '=',
//       this code will overrun buffer...
// Return true if there are more tags; false if this is the last tag;
bool
DataPoint::next_tag(char* &text)
{
    while (*text == ' ') text++;    // skip leading spaces
    if (*text == '\n') { text++; return false; }
    if (*text == 0) return false;
    char *key = text;
    char *val = key;
    while ((*val != '=') && (*val != '\n')) val++;
    if (*val == '\n') { text = val+1; return false; }
    *val++ = 0;
    if (*val == '\n') return false;
    for (text = val; (*text != ' ') && (*text != '\n'); text++) /* do nothing */;
    char tmp = *text;
    *text = 0;
    add_tag(key, val);
    text++;
    return (tmp == ' ');
}

void
DataPoint::parse_raw_tags()
{
    if (m_raw_tags == nullptr) return;

    char *key, *val, *space, *eq;

    for (key = m_raw_tags; key != nullptr; key = space)
    {
        while (*key == ' ') key++;
        eq = strchr(key, '=');
        if (eq == nullptr) return;
        *eq = 0;
        val = eq + 1;
        space = strchr(val, ' ');
        if (space != nullptr) *space++ = 0;
        add_tag(key, val);
    }
}

const char *
DataPoint::c_str(char* buff) const
{
    char *curr = buff;
    size_t size = c_size();
    int n = std::snprintf(buff, size, "%s %ld %lf", m_metric, m_timestamp, m_value);

    Tag *tag = m_tags;

    while ((tag != nullptr) && (size > 0))
    {
        size -= n;
        curr += n;

        n = std::snprintf(curr, size, " %s=%s", tag->m_key, tag->m_value);
        tag = tag->next();
    }

    return buff;
}


DataPointSet::DataPointSet(int max_size) :
    m_max_size(max_size),
    m_count(0),
    TagOwner(false)
{
    m_dps = new DataPointPair[max_size];
    ASSERT(m_dps != nullptr);
    ASSERT(m_tags == nullptr);
}

DataPointSet::~DataPointSet()
{
    clear();

    if (m_dps != nullptr)
    {
        delete m_dps;
        m_dps = nullptr;
    }
}

void
DataPointSet::clear()
{
    if (m_tags != nullptr)
    {
        remove_all_tags();
        m_tags = nullptr;
    }

    m_count = 0;
}

void
DataPointSet::add(Timestamp tstamp, double value)
{
    ASSERT(! is_full());
    m_dps[m_count].first = tstamp;
    m_dps[m_count].second = value;
    m_count++;
}

const char *
DataPointSet::c_str(char* buff) const
{
    char *curr = buff;
    size_t size = c_size();
    int n;

    for (int i = 0; (i < m_count) && (size > 0); i++)
    {
        n = std::snprintf(buff, size, "%ld %lf", get_timestamp(i), get_value(i));

        Tag *tag = m_tags;

        while ((tag != nullptr) && (size > 0))
        {
            size -= n;
            curr += n;

            n = std::snprintf(curr, size, " %s=%s", tag->m_key, tag->m_value);
            tag = tag->next();
        }
    }

    return buff;
}


}
