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

#include "cal.h"


namespace tt
{


std::mutex Calendar::m_lock;
std::vector<Timestamp> Calendar::m_months;


Timestamp
Calendar::begin_month_of(Timestamp ts)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int n = m_months.size();

    if (n > 1)
    {
        for (int i = n-1; i >= 0; i--)
        {
            Timestamp b = m_months[i];

            if (b <= ts)
            {
                if (i < (n-1))
                    return b;
                break;
            }
        }
    }

    n = add_month(ts, n);
    ASSERT(m_months[n] == begin_month(ts));
    return m_months[n];
}

Timestamp
Calendar::end_month_of(Timestamp ts)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int n = m_months.size();

    if (n > 1)
    {
        for (int i = n-1; i >= 0; i--)
        {
            if (m_months[i] <= ts)
            {
                if (i < (n-1))
                    return m_months[i+1];
                break;
            }
        }
    }

    n = add_month(ts, n);
    ASSERT(m_months[n+1] == end_month(ts));
    return m_months[n+1];
}

// return index of begin
std::size_t
Calendar::add_month(Timestamp ts, int n)
{
    ASSERT(n >= 0);

    Timestamp begin = begin_month(ts);
    Timestamp end = end_month(ts);

    if (n == 0)
    {
        m_months.push_back(begin);
        m_months.push_back(end);
    }
    else if (m_months[n-1] == begin)
    {
        m_months.push_back(end);
        n--;
    }
    else if (m_months[n-1] < begin)
    {
        Timestamp back = m_months.back();

        ASSERT(is_sec(back));

        while (back < begin)
        {
            back = end_month(back);
            m_months.push_back(back);
        }

        ASSERT(back == begin);
        m_months.push_back(end);
        ASSERT(m_months.size() >= 2);
        n = m_months.size() - 2;
    }
    else if (m_months[0] == end)
    {
        m_months.insert(m_months.begin(), begin);
        n = 0;
    }
    else
    {
        ASSERT(end < m_months[0]);
        Timestamp front = m_months.front();

        while (end < front)
        {
            Timestamp front = begin_month(front - 1);
            m_months.insert(m_months.begin(), front);
        }

        ASSERT(end == front);
        m_months.insert(m_months.begin(), begin);
        n = 0;
    }

    return n;
}


}
