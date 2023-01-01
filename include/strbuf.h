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

#include <vector>


namespace tt
{


/* Recyclable buffer for strings. NOT thread-safe.
 * Can't handle string longer than g_page_size-1.
 */
class StringBuffer
{
public:
    StringBuffer();
    ~StringBuffer();

    char *strdup(const char *str);

private:
    int m_cursor;
    std::vector<char*> m_buffs;
};


class HashBuffer
{
public:
    HashBuffer(std::size_t size);
    ~HashBuffer();

    char *strdup(const char *str);

private:
    std::size_t m_cursor;
    std::size_t m_buff_size;
    std::vector<char*> m_buffs;
};


}
