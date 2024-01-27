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

#include "range.h"


namespace tt
{

const TimeRange TimeRange::MAX(0L, std::numeric_limits<uint64_t>::max());
const TimeRange TimeRange::MIN(std::numeric_limits<uint64_t>::max(), 0L);


void
TimeRange::merge(const TimeRange& other)
{
    m_from = std::min(m_from, other.m_from);
    m_to = std::max(m_to, other.m_to);
}

void
TimeRange::intersect(const TimeRange& other)
{
    m_from = std::max(m_from, other.m_from);
    m_to = std::min(m_to, other.m_to);
}


}
