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

#include <mutex>


namespace tt
{


class PageInfo;
class TimeSeries;
class Tsdb;


/* This file is used to store serialized version of various maps
 * that we use to locate TimeSeries, by looking up metric names
 * and tags.
 */
class MetaFile
{
public:
    MetaFile(const std::string& file_name);
    virtual ~MetaFile();

    void open();    // for append
    void close();
    void flush();
    void reset();   // truncate the file to zero length

    void append(TimeSeries *ts, PageInfo *info);
    void append(TimeSeries *ts, unsigned int file_id, unsigned int from_id, unsigned int to_id);
    void load(Tsdb *tsdb);

    inline bool is_open() const { return (m_file != nullptr); }

private:
    std::mutex m_lock;
    std::string m_name;
    std::FILE *m_file;
};


}
