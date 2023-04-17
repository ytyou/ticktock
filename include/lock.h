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

#include <pthread.h>
#include "utils.h"


namespace tt
{


class PThread_ReadLock
{
public:
    PThread_ReadLock(pthread_rwlock_t *lock) :
        m_lock(lock)
    {
        ASSERT(m_lock != nullptr);
        pthread_rwlock_rdlock(m_lock);
    }

    ~PThread_ReadLock()
    {
        pthread_rwlock_unlock(m_lock);
    }

private:
    pthread_rwlock_t *m_lock;
};


class PThread_WriteLock
{
public:
    PThread_WriteLock(pthread_rwlock_t *lock) :
        m_lock(lock)
    {
        ASSERT(m_lock != nullptr);
        pthread_rwlock_wrlock(m_lock);
    }

    ~PThread_WriteLock()
    {
        pthread_rwlock_unlock(m_lock);
    }

private:
    pthread_rwlock_t *m_lock;
};


}
