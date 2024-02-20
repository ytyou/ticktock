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

#include "config.h"
#include "global.h"
#include "logger.h"
#include "timer.h"
#include "utils.h"


namespace tt
{


Timer *Timer::m_instance = nullptr;


Timer::Timer() :
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
    m_tasks.push_back(timed);
}


}
