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

#include <cassert>
#include <cstddef>
#include <map>
#include "tag.h"
#include "type.h"
#include "recycle.h"


namespace tt
{


class DataPoint : public TagOwner, public Recyclable
{
public:
    DataPoint();
    DataPoint(Timestamp ts, double value);
    void init(Timestamp ts, double value);

    inline bool recycle()
    {
        TagOwner::recycle();
        return true;    // return true to tell memmgr to reuse it
    }

    inline Timestamp get_timestamp() const
    {
        return m_timestamp;
    }

    inline void set_timestamp(Timestamp ts)
    {
        m_timestamp = ts;
    }

    inline double get_value() const
    {
        return m_value;
    }

    inline void set_value(double value)
    {
        m_value = value;
    }

    char* from_http(char* http);
    char* from_json(char* json);
    bool from_plain(char* &text);
    void parse_raw_tags();

    inline const char *get_metric() { return m_metric; }
    inline char *get_raw_tags() { return m_raw_tags; }
    inline void set_raw_tags(char *tags) { m_raw_tags = tags; }
    inline void set_metric(const char *metric) { m_metric = metric; }

    const char* c_str(char* buff, size_t size) const;

private:
    char* next_word(char* json, char* &word);
    char* next_long(char* json, Timestamp &number);
    char* next_double(char* json, double &number);
    char* next_tags(char* json);
    bool next_tag(char* &text);

    Timestamp m_timestamp;
    double m_value;

    const char *m_metric;
    char *m_raw_tags;
};


// This is used for batch inserts.
class DataPointSet : public TagOwner
{
public:
    DataPointSet(int max_size);
    virtual ~DataPointSet();

    void clear();
    void add(Timestamp tstamp, double value);

    inline Timestamp get_timestamp(int i) const
    {
        ASSERT(i < m_max_size);
        return m_dps[i].first;
    }

    inline double get_value(int i) const
    {
        ASSERT(i < m_max_size);
        return m_dps[i].second;
    }

    inline int get_dp_count() const
    {
        return m_count;
    }

    inline bool is_full() const
    {
        return (m_count >= m_max_size);
    }

    inline bool is_empty() const
    {
        return (m_count <= 0);
    }

    const char* c_str(char* buff, size_t size) const;

private:
    int m_count;
    int m_max_size;
    DataPointPair *m_dps;
};


}
