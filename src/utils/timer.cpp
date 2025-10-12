/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include "config.h"
#include "global.h"
#include "logger.h"
#include "timer.h"
#include "utils.h"


namespace tt
{


Timer *Timer::m_instance = nullptr;


Timer::Timer() :
    m_has_new(false),
    m_granularity_sec(Config::inst()->get_time(CFG_TIMER_GRANULARITY, TimeUnit::SEC, CFG_TIMER_GRANULARITY_DEF)),
    m_scheduler("timer", Config::inst()->get_int(CFG_TIMER_THREAD_COUNT,CFG_TIMER_THREAD_COUNT_DEF), Config::inst()->get_int(CFG_TIMER_QUEUE_SIZE,CFG_TIMER_QUEUE_SIZE_DEF))
{
}

// The very first call of this method is not thread-safe!
Timer *
Timer::inst()
{
    return (m_instance == nullptr) ? (m_instance = new Timer) : m_instance;
}

void
Timer::start()
{
    m_thread = std::thread(&Timer::run, this);
}

void
Timer::stop()
{
    shutdown();
    m_scheduler.shutdown();
    m_scheduler.wait(0);
    if (m_thread.joinable()) m_thread.join();
}

void
Timer::run()
{
    g_thread_id = "timer";
    Logger::info("Timer started");

    while (! is_shutdown_requested())
    {
        long now = ts_now_sec();

        // check for new tasks
        if (m_has_new)
        {
            std::lock_guard<std::mutex> guard(m_lock);

            if (! m_new_tasks.empty())
            {
                m_tasks.insert(m_tasks.end(), m_new_tasks.begin(), m_new_tasks.end());
                m_new_tasks.clear();
            }

            m_has_new = false;
        }

        for (TimedTask& task: m_tasks)
        {
            if (task.m_next_run <= now)
            {
                Logger::debug("Timer submitting task %s", task.m_name);
                m_scheduler.submit_task(task.m_task);
                task.m_next_run = now + task.m_freq_sec;
            }

            if (is_shutdown_requested()) break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(m_granularity_sec));
    }

    Logger::info("Timer stopped");
}

void
Timer::add_task(Task& task, int freq_sec, const char *name)
{
    TimedTask timed(task, freq_sec, name);
    std::lock_guard<std::mutex> guard(m_lock);
    m_new_tasks.push_back(timed);
    m_has_new = true;
}


TimedTask::TimedTask(const TimedTask& copy) :
    m_freq_sec(copy.m_freq_sec),
    m_task(copy.m_task),
    m_next_run(copy.m_next_run),
    m_name(copy.m_name)
{
}

TimedTask&
TimedTask::operator=(const TimedTask& copy)
{
    m_freq_sec = copy.m_freq_sec;
    m_task = copy.m_task;
    m_next_run = copy.m_next_run;
    m_name = copy.m_name;
    return *this;
}


}
