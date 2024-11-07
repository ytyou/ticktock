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
#include <random>
#include <thread>
#include "stop.h"
#include "task.h"


namespace tt
{


class TimedTask
{
public:
    TimedTask(Task& task, int freq_sec, const char *name) :
        m_freq_sec(freq_sec),
        m_task(task),
        m_name(name),
        m_next_run(ts_now_sec() + random(0,freq_sec))
    {
    }

    TimedTask(const TimedTask& copy);               // copy constructor
    TimedTask& operator=(const TimedTask& copy);    // assignment constructor

friend class Timer;
private:
    int m_freq_sec;
    Task m_task;
    long m_next_run;
    const char *m_name;
};


class Timer : public Stoppable
{
public:
    static Timer *inst();

    void start();
    void stop();

    void add_task(Task& task, int freq_sec, const char *name);

private:
    friend class Stats;

    Timer();
    void run();

    int m_granularity_sec;
    TaskScheduler m_scheduler;
    std::vector<TimedTask> m_tasks;
    std::thread m_thread;

    bool m_has_new;     // true if m_new_tasks not empty
    std::mutex m_lock;  // lock for m_new_tasks
    std::vector<TimedTask> m_new_tasks; // new tasks are added here

    static Timer *m_instance;
};


}
