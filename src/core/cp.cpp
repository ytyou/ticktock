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
cps_map CheckPointManager::m_cps;
cps_map CheckPointManager::m_snapshot;
cps_map CheckPointManager::m_persisted;


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

/* Return format:
 *  [{"leader":"1","channels":[{"channel":"ch1","checkpoint":"cp1"},{"channel":"ch2","cp":"cp2"}]},{...}]
 */
int
CheckPointManager::get_persisted(const char *leader, char *buff, size_t size)
{
    ASSERT(size > 3);
    ASSERT(buff != nullptr);

    int idx = 1;
    buff[0] = '[';
    size -= 3;

    std::lock_guard<std::mutex> guard(m_lock);

    if (leader == nullptr)
    {
        for (auto it = m_persisted.begin(); it != m_persisted.end(); it++)
        {
            if (idx > 1)
            {
                buff[idx++] = ',';
                size--;
            }

            int len;
            len = get_persisted_of(it->second, &buff[idx], size);
            ASSERT(len < size);
            idx += len;
            size -= len;
            if (size <= 1) break;
        }
    }
    else
    {
        auto search = m_persisted.find(leader);
        if (search != m_persisted.end())
            idx += get_persisted_of(search->second, &buff[idx], size);
    }

    buff[idx++] = ']';
    buff[idx] = 0;

    return idx;
}

int
CheckPointManager::get_persisted_of(cp_map& map, char *buff, size_t size)
{
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
