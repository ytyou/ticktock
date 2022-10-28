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


namespace tt
{


enum RecyclableType : unsigned char
{
    RT_AGGREGATOR_AVG    = 0,
    RT_AGGREGATOR_COUNT  = RT_AGGREGATOR_AVG + 1,       // 1
    RT_AGGREGATOR_DEV    = RT_AGGREGATOR_COUNT + 1,     // 2
    RT_AGGREGATOR_MAX    = RT_AGGREGATOR_DEV + 1,       // 3
    RT_AGGREGATOR_MIN    = RT_AGGREGATOR_MAX + 1,       // 4
    RT_AGGREGATOR_NONE   = RT_AGGREGATOR_MIN + 1,       // 5
    RT_AGGREGATOR_PT     = RT_AGGREGATOR_NONE + 1,      // 6
    RT_AGGREGATOR_SUM    = RT_AGGREGATOR_PT + 1,        // 7
    RT_BITSET_CURSOR     = RT_AGGREGATOR_SUM + 1,       // 8
    RT_COMPRESSOR_V0     = RT_BITSET_CURSOR + 1,        // 9
    RT_COMPRESSOR_V1     = RT_COMPRESSOR_V0 + 1,        // 10
    RT_COMPRESSOR_V2     = RT_COMPRESSOR_V1 + 1,        // 11
    RT_COMPRESSOR_V3     = RT_COMPRESSOR_V2 + 1,        // 12
    RT_DATA_POINT        = RT_COMPRESSOR_V3 + 1,        // 13
    RT_DOWNSAMPLER_AVG   = RT_DATA_POINT + 1,           // 14
    RT_DOWNSAMPLER_COUNT = RT_DOWNSAMPLER_AVG + 1,      // 15
    RT_DOWNSAMPLER_DEV   = RT_DOWNSAMPLER_COUNT + 1,    // 16
    RT_DOWNSAMPLER_FIRST = RT_DOWNSAMPLER_DEV + 1,      // 17
    RT_DOWNSAMPLER_LAST  = RT_DOWNSAMPLER_FIRST + 1,    // 18
    RT_DOWNSAMPLER_MAX   = RT_DOWNSAMPLER_LAST + 1,     // 19
    RT_DOWNSAMPLER_MIN   = RT_DOWNSAMPLER_MAX + 1,      // 20
    RT_DOWNSAMPLER_PT    = RT_DOWNSAMPLER_MIN + 1,      // 21
    RT_DOWNSAMPLER_SUM   = RT_DOWNSAMPLER_PT + 1,       // 22
    RT_HTTP_CONNECTION   = RT_DOWNSAMPLER_SUM + 1,      // 23
    RT_JSON_VALUE        = RT_HTTP_CONNECTION + 1,      // 24
    RT_KEY_VALUE_PAIR    = RT_JSON_VALUE + 1,           // 25
    RT_PAGE_ON_DISK      = RT_KEY_VALUE_PAIR + 1,       // 26
    RT_QUERY_RESULTS     = RT_PAGE_ON_DISK + 1,         // 27
    RT_QUERY_TASK        = RT_QUERY_RESULTS + 1,        // 28
    RT_RATE_CALCULATOR   = RT_QUERY_TASK + 1,           // 29
    RT_TCP_CONNECTION    = RT_RATE_CALCULATOR + 1,      // 30
    RT_COUNT             = RT_TCP_CONNECTION + 1        // 31
};


class Recyclable
{
public:
    Recyclable() :
        m_recyclable_next(nullptr)
    {
    }

    virtual ~Recyclable() { }

    inline Recyclable*& next()
    {
        return m_recyclable_next;
    }

    inline Recyclable *next_const() const
    {
        return m_recyclable_next;
    }

    inline RecyclableType& recyclable_type()
    {
        return m_recyclable_type;
    }

    // This method is called before going back into service.
    // This is your chance to do any initialization of your Recyclable.
    inline virtual void init() { };

    // This method is called before going onto free-list.
    // This is your chance to free up any resources, like you do in a
    // destructor.
    // If this method returns false, memory will be freed instead of
    // going back to free-list to be re-used.
    inline virtual bool recycle() { return true; };

private:
    RecyclableType m_recyclable_type;
    Recyclable *m_recyclable_next;
};


}
