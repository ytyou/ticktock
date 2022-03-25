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

#include <atomic>
#include <chrono>
#include <thread>
#include <signal.h>
#include <assert.h>
#include <iostream>
#include <inttypes.h>
#include "task_test.h"
#include "utils.h"

#define NUM_TASKS   (60*1024*1024)


using namespace tt;

namespace tt_test
{


bool one_time_task(TaskData& data)
{
    float sum = 0.0;
    for (int i = 0; i < 512; i++)
    {
        sum += (float)i;
    }
    return (sum < 0.1);
}

void
TaskTests::task_creator(TaskScheduler *scheduler, size_t num_tasks)
{
    auto start = std::chrono::steady_clock::now();
    Task task;

    task.doit = one_time_task;
    task.data.pointer = nullptr;

    for (int i = 0; i < num_tasks; i++)
    {
        //auto t1 = std::chrono::steady_clock::now();
        //auto t2 = std::chrono::steady_clock::now();
        //assert(task != nullptr);
        //assert(task->doit != nullptr);
        scheduler->submit_task(task);
        //auto t3 = std::chrono::steady_clock::now();
        //new_task_time += (std::chrono::duration<double>(t2 - t1)).count();
        //submit_task_time += (std::chrono::duration<double>(t3 - t2)).count();
    }

    // measure runtime
    auto finish = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = finish - start;

    log("Took %.2f secs to create %d tasks", elapsed.count(), num_tasks);
    //Logger::info("Took %f secs to create %d tasks (new: %.2f, submit: %.2f, pause: %d)", elapsed.count(), num_tasks, new_task_time, submit_task_time, submit_task_pause);
}

// return amount of time, in secs, we spend running test
double
TaskTests::run_once(size_t scheduler_count, size_t thread_count)
{
    log("Running test with %d schedulers, of %d threads each...", scheduler_count, thread_count);

    auto start = std::chrono::steady_clock::now();
    char buff[128]; // used for logging

    // submit tasks as fast as we can
    TaskScheduler *schedulers[scheduler_count];
    std::thread task_creators[scheduler_count];
    size_t num_tasks = NUM_TASKS / scheduler_count;

    for (int i = 0; i < scheduler_count; i++)
    {
        schedulers[i] = new TaskScheduler(EMPTY_STRING, thread_count, 1024);
        task_creators[i] = std::thread(&TaskTests::task_creator, this, schedulers[i], num_tasks);
    }

    //Logger::info("All task_creators are running...");

    for (int i = 0; i < scheduler_count; i++)
    {
        task_creators[i].join();
    }

    //Logger::info("All task_creators are done.");

    std::vector<size_t> counts;

    for (int i = 0; i < scheduler_count; i++)
    {
        while (schedulers[i]->get_pending_task_count(counts) > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        schedulers[i]->shutdown();
        schedulers[i]->wait(0);
        delete schedulers[i];
    }

    // measure runtime
    auto finish = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = finish - start;

    return elapsed.count();
}

double
reference()
{
    auto start = std::chrono::steady_clock::now();
    TaskData data;  // no need to init it, since one_time_task() doesn't use it

    for (int i = 0; i < NUM_TASKS; i++)
    {
        if (one_time_task(data))
        {
            assert(false);
        }
    }

    // measure runtime
    auto finish = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = finish - start;

    return elapsed.count();
}

void
TaskTests::run()
{
    log("Running %s...", m_name);
    log("Reference time: %.2f secs", reference());

    int best_thread_count = 0;
    int best_scheduler_count = 0;
    double best_runtime = std::numeric_limits<double>::max();

    for (size_t scheduler_cnt = 1; scheduler_cnt <= 3; scheduler_cnt++)
    {
        for (size_t thread_cnt = 1; thread_cnt <= 4; thread_cnt++)
        {
            double runtime = run_once(scheduler_cnt, thread_cnt);

            if (runtime < best_runtime)
            {
                best_thread_count = thread_cnt;
                best_scheduler_count = scheduler_cnt;
                best_runtime = runtime;
                log("NEW BEST: runtime=%.2f (scheduler=%d, threads=%d)", best_runtime, best_scheduler_count, best_thread_count);
            }
            else
            {
                log("runtime=%.2f (scheduler=%d, threads=%d)", runtime, scheduler_cnt, thread_cnt);
            }
        }
    }

    log("BEST runtime of %.2f secs was achieved with %d schedulers and %d threads", best_runtime, best_scheduler_count, best_thread_count);
    log("Finished %s", m_name);
}


}
