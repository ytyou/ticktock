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
#include <unordered_map>
#include "utils.h"


namespace tt
{


#define MAX_CHECK_POINT     32

class CheckPoint;

typedef std::unordered_map<const char*,CheckPoint*,hash_func,eq_func> cp_map;
typedef std::unordered_map<const char*,cp_map,hash_func,eq_func> cps_map;


class CheckPoint
{
public:
    CheckPoint(const char *cp)
    {
        set(cp);
        m_received = ts_now_sec();
    }

    inline const char *get()
    {
        return &m_check_point[0];
    }

    void set(const char *cp)
    {
        std::strncpy(m_check_point, cp, MAX_CHECK_POINT);
        m_check_point[MAX_CHECK_POINT-1] = 0;
    }

private:
    Timestamp m_received;
    char m_check_point[MAX_CHECK_POINT];
};


class CheckPointManager
{
public:
    // load snapshots from disk
    static void init();

    // format of cp: <leader>:<channel>:<check-point>
    // no spaces allowed; max length of <check-point> is 30

    // add check-point received from client/leader
    static bool add(char *cp);

    // take a snapshot of all check-points before flushing database
    static void take_snapshot();

    // persist the snapshot (from last take_snapshot() call) to disk
    static void persist();

    // return last persisted snapshot, in json format
    static int get_persisted(const char *leader, char *buff, int size);

    // last chance to persist anything that's not yet persisted
    static void close();

private:
    CheckPointManager() = default;
    static int get_persisted_of(const char *leader, cp_map& map, char *buff, int size);

    static std::mutex m_lock;
    static cps_map m_cps;
    static cps_map m_snapshot;
    static cps_map m_persisted;
};


}
