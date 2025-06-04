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


int
Calendar::binary_search(Timestamp ts, int n)
{
    if (n == 0) return -1;  // m_months[] is empty
    ASSERT(n > 1);  // n can never be 1
    ASSERT(m_months.size() == n);

    if (ts < m_months[0]) return -1;
    if (m_months[n-1] <= ts) return -1;

    // special case (last month)
    if (m_months[n-2] <= ts && ts < m_months[n-1])
        return n-2;

    // perform binary search
    int left = 0, right = n-1;

    while (left < right)
    {
        int mid = left + (right - left) / 2;

        if (m_months[mid] < ts)
        {
            if (left == mid)
                break;
            left = mid;
        }
        else if (ts < m_months[mid])
        {
            if (right == mid)
                break;
            right = mid;
        }
        else // ts == m_months[mid]
        {
            left = mid;
            break;
        }
        //{
            //left++;
            //break;
        //}
    }

    return left;
}

Timestamp
Calendar::begin_month_of(Timestamp ts)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int n = m_months.size();
    int i = binary_search(ts, n);

    if (i < 0)
        i = add_month(ts, n);

    ASSERT(i >= 0);
    ASSERT(m_months[i] == begin_month(ts));
    return m_months[i];
}

Timestamp
Calendar::end_month_of(Timestamp ts)
{
    std::lock_guard<std::mutex> guard(m_lock);
    int n = m_months.size();
    int i = binary_search(ts, n);

    if (i < 0)
        i = add_month(ts, n);

    ASSERT(0 <= i && i < (n-1));
    ASSERT(m_months[i+1] == end_month(ts));
    return m_months[i+1];
}

// add begin-of-month(ts) to cache (m_months), and return index of it;
// 'n' should be the size of m_months[];
std::size_t
Calendar::add_month(Timestamp ts, int n)
{
    ASSERT(n >= 0);

    Timestamp begin = begin_month(ts);
    Timestamp end = end_month(ts);

    if (n == 0)
    {
        // m_months[] is empty
        m_months.push_back(begin);
        m_months.push_back(end);
    }
    else if (m_months[n-1] == begin)
    {
        // last of m_months[] is the 'begin'
        m_months.push_back(end);
        n--;
    }
    else if (m_months[n-1] < begin)
    {
        // 'begin' is after the last of m_months[]
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
        // 'begin' is right before the first of m_months[]
        m_months.insert(m_months.begin(), begin);
        n = 0;
    }
    else
    {
        // 'begin' is before the first of m_months[]
        ASSERT(end < m_months[0]);
        Timestamp front = m_months.front();
        std::vector<Timestamp> tmp = std::move(m_months);

        ASSERT(m_months.empty());
        m_months.push_back(begin);

        while (end < front)
        {
            m_months.push_back(end);
            end = begin_month(end + 45 * 24 * 3600);
        }

        // merge tmp and m_months
        m_months.insert(m_months.end(), tmp.begin(), tmp.end());

        ASSERT(end == front);
        n = 0;
    }

    return n;
}


}
