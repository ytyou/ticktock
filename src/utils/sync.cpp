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

#include <cassert>
#include "sync.h"
#include "utils.h"


namespace tt
{


CountingSignal::CountingSignal(int count) :
    m_count(count),
    m_count_up_ok(true),
    m_lock(nullptr)
{
}

CountingSignal::~CountingSignal()
{
    if (m_lock != nullptr)
        delete m_lock;
}

bool
CountingSignal::count_up(unsigned int count)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (! m_count_up_ok) return false;
    m_count += count;
    return true;
}

void
CountingSignal::count_down(unsigned int count)
{
    bool notify = false;

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_count -= count;
        if (m_count <= 0)
        {
            notify = true;
            m_count_up_ok = false;
        }
    }

    if (notify)
        m_cv.notify_one();
}

void
CountingSignal::wait(bool keep_lock)
{
    if (keep_lock)
    {
        ASSERT(m_lock == nullptr);
        m_lock = new std::unique_lock<std::mutex>(m_mutex);
        m_count_up_ok = false;
        while (m_count > 0)
            m_cv.wait(*m_lock, [this]{return (m_count <= 0);});
    }
    else
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_count_up_ok = false;
        while (m_count > 0)
            m_cv.wait(lock, [this]{return (m_count <= 0);});
        m_count_up_ok = true;
    }
}

void
CountingSignal::unlock()
{
    if (m_lock != nullptr)
    {
        std::unique_lock<std::mutex> *lock = m_lock;
        m_count_up_ok = true;
        m_lock = nullptr;
        delete lock;
    }
}


}
