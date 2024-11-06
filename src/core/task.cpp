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

#include <cassert>
#include "global.h"
#include "task.h"
#include "config.h"
#include "limit.h"
#include "logger.h"


namespace tt
{


TaskData::TaskData() :
    integer(0),
    pointer(nullptr)
{
}

TaskData::TaskData(const TaskData& copy) :
    integer(copy.integer),
    pointer(copy.pointer)
{
}

TaskData &
TaskData::operator=(const TaskData& data)
{
    integer = data.integer;
    pointer = data.pointer;
    return *this;
}


Task::Task() :
    doit(nullptr)
{
}

Task::Task(const Task& copy) :
    doit(copy.doit),
    data(copy.data)
{
}

Task &
Task::operator=(const Task& task)
{
    doit = task.doit;
    data = task.data;
    return *this;
}


TaskScheduler::TaskScheduler() :
    m_id(EMPTY_STRING),
    m_thread_count(0),
    m_next_worker(0),
    m_threads(nullptr),
    m_workers(nullptr)
{
}

TaskScheduler::TaskScheduler(std::string id, size_t thread_count, size_t queue_size)
{
    init(id, thread_count, queue_size);
}

void
TaskScheduler::init(std::string id, size_t thread_count, size_t queue_size)
{
    m_id = id;
    m_thread_count = std::min((size_t)MAX_THREAD_COUNT, thread_count);
    m_next_worker = 0;

    // create threads
    m_threads = new std::thread[m_thread_count];
    m_workers = static_cast<Worker**>(malloc(sizeof(Worker*) * m_thread_count));
    ASSERT(m_workers != nullptr);

    for (size_t i = 0; i < thread_count; i++)
    {
        Worker *worker = new Worker(i, queue_size);
        m_workers[i] = worker;
        m_threads[i] = std::thread(&TaskScheduler::Worker::work, worker, this);
    }
}

TaskScheduler::~TaskScheduler()
{
    if (m_threads != nullptr)
    {
        delete[] m_threads;
        m_threads = nullptr;
    }

    if (m_workers != nullptr)
    {
        for (size_t i = 0; i < m_thread_count; i++)
            delete m_workers[i];
        free(m_workers);
    }
}

int
TaskScheduler::submit_task(Task& task, int id)
{
    if (is_shutdown_requested())
    {
        return -1;
    }

    int assignee = -1;

    for (unsigned int k = 0; assignee < 0; k++)
    {
        if ((id >= 0) && (id < m_thread_count))
        {
            if (m_workers[id]->m_tasks.try_enqueue(task))
            {
                assignee = id;
                break;
            }
        }
        else
        {
            for (int i = 0; i < m_thread_count; i++)
            {
                if (m_workers[m_next_worker]->m_tasks.try_enqueue(task))
                {
                    assignee = m_next_worker;
                    break;
                }

                m_next_worker = (m_next_worker + 1) % m_thread_count;
            }
        }

        if ((assignee >= 0) || is_shutdown_requested()) break;

        spin_yield(k);
    }

    m_next_worker = (m_next_worker + 1) % m_thread_count;

    return assignee;
}

void
TaskScheduler::submit_task_to_all(Task& task)
{
    for (int i = 0; i < m_thread_count; i++)
    {
        submit_task(task, i);
    }
}

void
TaskScheduler::shutdown(ShutdownRequest request)
{
    Stoppable::shutdown(request);

    for (size_t i = 0; i < m_thread_count; i++)
    {
        m_workers[i]->shutdown(request);
    }
}

void
TaskScheduler::wait(size_t timeout_secs)
{
    for (size_t i = 0; i < m_thread_count; i++)
    {
        /* The thread with id 'g_handler_thread_id' is in trouble.
         * Something happened in that thread, and now it's inside
         * intr_handler(), so it simply won't exit cleanly.
         * Don't wait for it.
         */
        if (m_threads[i].get_id() == g_handler_thread_id)
            m_threads[i].detach();
        else
            m_threads[i].join();
    }
}

size_t
TaskScheduler::get_pending_task_count(std::vector<size_t> &counts) const
{
    ASSERT(counts.empty());
    size_t total = 0;

    for (int i = 0; i < m_thread_count; i++)
    {
        size_t count = m_workers[i]->m_tasks.size();
        counts.push_back(count);
        total += count;
    }

    return total;
}

int
TaskScheduler::get_total_task_count(size_t counts[], int size) const
{
    if (size > m_thread_count) size = m_thread_count;

    for (int i = 0; i < size; i++)
    {
        counts[i] = m_workers[i]->m_total_tasks;
    }

    return size;
}

bool
TaskScheduler::is_stopped() const
{
    for (size_t i = 0; i < m_thread_count; i++)
    {
        if (! m_workers[i]->is_stopped())
        {
            return false;
        }
    }

    return true;
}


TaskScheduler::Worker::Worker(int id, size_t queue_size) :
    m_id(id),
    m_tasks(queue_size),
    m_total_tasks(0)
{
    ASSERT(m_tasks.is_empty());
}

void
TaskScheduler::Worker::work(TaskScheduler* scheduler)
{
    ASSERT(scheduler != nullptr);

    g_thread_id = scheduler->m_id + "_task_" + std::to_string(m_id);

    while (! is_shutdown_requested())
    {
        Task task;

        task.doit = nullptr;

        for (unsigned int k = 0; ! m_tasks.try_dequeue(task) && ! is_shutdown_requested(); k++)
        {
            spin_yield(k);
        }

        if (task.doit == nullptr) continue;
        if (is_shutdown_requested()) break;

        bool status = false;

        try
        {
            status = (task.doit)(task.data);
        }
        catch (const std::exception& ex)
        {
            Logger::error("Task failed with an exception: %s", ex.what());
        }
        catch (...)
        {
            Logger::error("Task failed with unknown exception");
        }

        m_total_tasks++;

        // re-schedule?
        if (status)
        {
            ASSERT(false);
            //scheduler->submit_task(task);
        }
    }

    set_stopped();
}

void
TaskScheduler::Worker::shutdown(ShutdownRequest request)
{
    Stoppable::shutdown(request);
    m_tasks.shutdown(request);
}


}
