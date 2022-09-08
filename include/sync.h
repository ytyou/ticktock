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

#include <atomic>
#include <mutex>
#include <condition_variable>
#include "utils.h"


namespace tt
{


class CountingSignal
{
public:
    CountingSignal(int count = 0);
    virtual ~CountingSignal();

    void wait(bool keep_lock);
    bool count_up(unsigned int count = 1);  // return true if successful
    void count_down(unsigned int count = 1);
    void unlock();

private:
    int m_count;
    bool m_count_up_ok;
    std::mutex m_mutex;
    std::unique_lock<std::mutex> *m_lock;   // we own this
    std::condition_variable m_cv;
};


class Counter
{
public:
    Counter() : m_count(0) {}
    void dec_count() { m_count--; }
    void inc_count() { m_count++; }
    inline bool count_is_zero() { return ((int)m_count.load() <= 0); }

private:
    std::atomic<int32_t> m_count;
};


/*
class CountKeeper
{
public:
    CountKeeper(Counter& counter) :
        m_count(counter.m_count)
    {
        ASSERT(m_count >= 0);
        m_count++;
        ASSERT(m_count >= 1);
    }

    ~CountKeeper()
    {
        ASSERT(m_count >= 1);
        m_count--;
        ASSERT(m_count >= 0);
    }

private:
    std::atomic<int32_t>& m_count;
};
*/


}
