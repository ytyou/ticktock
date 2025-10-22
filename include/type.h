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

#include <cstdint>
#include <vector>


namespace tt
{


#define LIKELY(X)       __builtin_expect((X), 1)
#define UNLIKELY(X)     __builtin_expect((X), 0)

typedef uint16_t PageCount;
typedef uint16_t PageSize;
typedef uint64_t TsdbSize;
typedef uint32_t MetricId;
typedef uint32_t BucketId;
typedef uint32_t TimeSeriesId;
typedef uint16_t FileIndex;
typedef uint16_t PageIndex;
typedef uint16_t HeaderIndex;
typedef uint32_t TagId;
typedef uint16_t TagCount;
typedef uint32_t RollupEntry;   // the n-th entry of rollup point in a Tsdb
typedef uint32_t RollupIndex;

#define TT_INVALID_FILE_INDEX   UINT16_MAX
#define TT_INVALID_HEADER_INDEX UINT16_MAX
#define TT_INVALID_ROLLUP_ENTRY UINT32_MAX
#define TT_INVALID_ROLLUP_INDEX UINT32_MAX
#define TT_INVALID_PAGE_INDEX   UINT16_MAX
#define TT_INVALID_TIMESTAMP    UINT64_MAX
#define TT_INVALID_TAG_ID       UINT32_MAX
#define TT_INVALID_TIME_SERIES_ID   UINT32_MAX
#define TT_INVALID_METRIC_ID    UINT32_MAX
#define TT_INVALID_BUCKET_ID    UINT32_MAX

typedef uint64_t Timestamp;     // milliseconds since epoch

typedef std::pair<Timestamp,double> DataPointPair;
typedef std::vector<DataPointPair> DataPointVector;

template <typename T>
struct __attribute__((__packed__)) aligned_type
{
    T value;
};

enum TimeUnit : unsigned char
{                   // string representation
    MS = 0,         // ms
    SEC = 1,        // s
    MIN = 2,        // m[in]
    HOUR = 3,       // h
    DAY = 4,        // d (24 hours)
    WEEK = 5,       // w (7 days)
    MONTH = 6,      // n (30 days)
    YEAR = 7,       // y (365 days)
    UNKNOWN = 99
};


#define TT_TYPE_TIME_SERIES     1
#define TT_TYPE_MEASUREMENT     2

class __attribute__ ((__packed__)) BaseType
{
public:
    virtual bool is_type(int type) const = 0;
    virtual ~BaseType() {};
};

/* We support 2 levels of rollup.
 * 1. One data point per hour; We call this 'level-1' rollup;
 * 2. One data point per day; We call this 'level-2' rollup;
 */
enum RollupLevel : unsigned char
{
    RL_LEVEL_NONE = 0,
    RL_LEVEL1 = 1,
    RL_LEVEL2 = 2
};

enum RollupType : unsigned char
{
    RU_NONE = 0x00,
    RU_AVG = 0x01,
    RU_CNT = 0x02,
    RU_MAX = 0x03,
    RU_MIN = 0x04,
    RU_SUM = 0x05,
    RU_RAW = 0x06,
    RU_AVG2 = 0x81,     // level2 rollup
    RU_CNT2 = 0x82,     // level2 rollup
    RU_MAX2 = 0x83,     // level2 rollup
    RU_MIN2 = 0x84,     // level2 rollup
    RU_SUM2 = 0x85,     // level2 rollup
    RU_LEVEL2 = 0x80    // level2 mask
};

enum InputFormat : unsigned char
{
    IF_JSON,
    IF_LINE,
    IF_PLAIN,
    IF_UNKNOWN
};


}
