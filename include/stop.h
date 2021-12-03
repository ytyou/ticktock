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
#include <regex>


namespace tt
{


enum class ShutdownRequest : unsigned char
{
    NONE = 0,       // no shutdown, continue with normal program
    ASAP = 1,       // finish the remaining tasks and then shutdown
    NOW = 2         // shutdown immediately, discarding all remaining tasks
};


class Stoppable
{
public:
    Stoppable() :
        m_stopped(false),
        m_shutdown_request(ShutdownRequest::NONE)
    {
    }

    inline virtual void shutdown(ShutdownRequest request = ShutdownRequest::ASAP)
    {
        m_shutdown_request = request;
    }

    inline virtual bool is_shutdown_requested() const
    {
        return (m_shutdown_request.load(std::memory_order_relaxed) != ShutdownRequest::NONE);
    }

    inline virtual bool is_stopped() const
    {
        return m_stopped.load(std::memory_order_relaxed);
    }

    inline virtual void set_stopped()
    {
        m_stopped.store(true, std::memory_order_relaxed);
    }

    inline virtual void wait(size_t timeout_secs) { };

private:
    volatile std::atomic<bool> m_stopped;
    volatile std::atomic<ShutdownRequest> m_shutdown_request;
};


}
