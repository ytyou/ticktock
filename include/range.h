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

#include <inttypes.h>
#include "serial.h"
#include "type.h"
#include "utils.h"


namespace tt
{


// define a range [from, to)
class TimeRange : public Serializable
{
public:
    TimeRange() : TimeRange(0L, 0L)
    {
    }

    TimeRange(Timestamp from, Timestamp to) :
        m_from(from),
        m_to(to)
    {
        //ASSERT(m_from <= m_to);
        //ASSERT((is_ms(m_from) && is_ms(m_to)) || (is_sec(m_from) && is_sec(m_to)) || (m_from == 0L));
    }

    TimeRange(const TimeRange& range) :
        m_from(range.m_from),
        m_to(range.m_to)
    {
    }

    void init(const TimeRange& range)
    {
        m_from = range.m_from;
        m_to = range.m_to;
    }

    void init(Timestamp from, Timestamp to)
    {
        m_from = from;
        m_to = to;

        //ASSERT(m_from <= m_to);
        ASSERT((is_ms(m_from) && is_ms(m_to)) || (is_sec(m_from) && is_sec(m_to)) || (m_from == 0L));
    }

    void merge(const TimeRange& other);       // union
    void intersect(const TimeRange& other);

    inline void add_time(Timestamp tstamp)
    {
        if (tstamp < m_from) m_from = tstamp;
        tstamp++;
        if (m_to < tstamp) m_to = tstamp;
    }

    // return 0 if in range; -1 if on the left; +1 if on the right;
    inline int in_range(Timestamp tstamp) const
    {
        return (tstamp < m_from) ? -1 : ((m_to <= tstamp) ? 1 : 0);
    }

    inline bool has_intersection(const TimeRange& range) const
    {
        return (m_from < range.m_to) && (range.m_from < m_to);
    }

    inline bool contains(const TimeRange& range) const
    {
        return (m_from <= range.m_from) && (range.m_to <= m_to);
    }

    inline bool equals(const TimeRange& range) const
    {
        return (m_from == range.m_from) && (range.m_to == m_to);
    }

    inline Timestamp get_from() const
    {
        return m_from;
    }

    inline Timestamp get_to() const
    {
        return m_to;
    }

    inline Timestamp get_middle() const
    {
        return m_from + ((m_to - m_from) / 2);
    }

    inline Timestamp get_duration() const
    {
        return m_to - m_from;
    }

    inline Timestamp get_duration_sec() const
    {
        return to_sec(m_to) - to_sec(m_from);
    }

    inline Timestamp get_from_sec() const
    {
        return to_sec(m_from);
    }

    inline Timestamp get_to_sec() const
    {
        return to_sec(m_to);
    }

    inline bool older_than_sec(Timestamp tstamp) const
    {
        ASSERT(is_sec(tstamp));
        return (to_sec(m_to) <= tstamp);
    }

    inline void set_from(Timestamp from)
    {
        m_from = from;
    }

    inline void set_to(Timestamp to)
    {
        m_to = to;
    }

    inline size_t c_size() const override { return 44; }

    inline const char *c_str(char* buff) const override
    {
        std::snprintf(buff, c_size(), "[%" PRIu64 ",%" PRIu64 ")", m_from, m_to);
        return buff;
    }

    static const TimeRange MAX;
    static const TimeRange MIN;

private:
    Timestamp m_from;
    Timestamp m_to;
};


}
