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

#include "cp.h"


namespace tt
{


std::mutex CheckPointManager::m_lock;
std::unordered_map<const char*,std::unordered_map<const char*,CheckPoint*,hash_func,eq_func>,hash_func,eq_func> CheckPointManager::m_cps;
std::unordered_map<const char*,std::unordered_map<const char*,CheckPoint*,hash_func,eq_func>,hash_func,eq_func> CheckPointManager::m_snapshot;
std::unordered_map<const char*,std::unordered_map<const char*,CheckPoint*,hash_func,eq_func>,hash_func,eq_func> CheckPointManager::m_persisted;


void
CheckPointManager::init()
{
    // load from disk
}

bool
CheckPointManager::add(char *cp)
{
    return true;
}

void
CheckPointManager::take_snapshot()
{
}

int
CheckPointManager::get_persisted(const char *leader, char *buff, size_t size)
{
    ASSERT(size > 0);
    ASSERT(buff != nullptr);

    return 0;
}

void
CheckPointManager::persist()
{
}

void
CheckPointManager::close()
{
    take_snapshot();
    persist();
}


}
