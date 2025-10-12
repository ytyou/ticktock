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

#pragma once

#include <atomic>
#include <thread>
#include "queue.h"


namespace tt
{


class TaskData
{
public:
    TaskData();
    TaskData(const TaskData& copy);

    TaskData& operator=(const TaskData& data);

    int integer;
    void *pointer;
};


typedef bool (*TaskFunc)(TaskData& data);


class Task
{
public:
    Task();
    Task(const Task& copy);

    Task& operator=(const Task& task);

    TaskFunc doit;
    TaskData data;
};


// Tasks can't be submitted from multiple threads simultaniously.
// To submit a task, do the following from a single thread:
//   1. Fill in Task::doit, and optionally Task::data
//   2. Call submit_task() with the Task
class TaskScheduler : public Stoppable
{
public:
    TaskScheduler();
    TaskScheduler(std::string id, size_t thread_count, size_t queue_size);
    ~TaskScheduler();

    // these are blocking calls, from a single thread
    int submit_task(Task& task, int id = -1);
    void submit_task_to_all(Task& task);

    void shutdown(ShutdownRequest request = ShutdownRequest::ASAP);
    void wait(size_t timeout_secs); // BLOCKING CALL!

    size_t get_pending_task_count(int id) const;
    size_t get_pending_task_count(std::vector<size_t> &counts) const;
    int get_total_task_count(size_t counts[], int size) const;
    bool is_stopped() const;        // return true if all workers have exited

private:

    void init(std::string id, size_t thread_count, size_t queue_size);

    class Worker : public Stoppable
    {
    public:
        Worker(int id, size_t queue_size);

        void work(TaskScheduler* scheduler);
        void shutdown(ShutdownRequest request = ShutdownRequest::ASAP);

        int m_id;
        Queue11<Task> m_tasks;
        uint64_t m_total_tasks;
    };

    std::string m_id;

    std::thread *m_threads;
    Worker **m_workers;

    int m_next_worker;
    size_t m_thread_count;
};


}
