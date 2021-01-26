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

#include <cstdint>
#include <vector>


namespace tt
{


typedef uint32_t PageCount;
typedef uint16_t PageSize;
typedef uint64_t TsdbSize;

typedef uint64_t Timestamp;     // milliseconds since epoch

typedef std::pair<Timestamp,double> DataPointPair;
typedef std::vector<DataPointPair> DataPointVector;

enum TimeUnit
{                   // string representation
    MS = 0,         // ms
    SEC = 1,        // s
    MIN = 2,        // min
    HOUR = 3,       // h
    DAY = 4,        // d
    WEEK = 5,       // w
    UNKNOWN = 99
};


}
