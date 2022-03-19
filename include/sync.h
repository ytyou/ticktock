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
#include <condition_variable>


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


// When a new TCP connection is being 'accept()'ed, all
// responders need to stop and wait.
class NewConnectionSignal
{
public:
    NewConnectionSignal(int count = 0);

    bool count_up(unsigned int count = 1);  // return true if successful
    void count_down(unsigned int count = 1);
    void unlock();
    void wait();

private:
    int m_count;
    std::mutex m_mutex;
    std::condition_variable m_cv;
};


}
