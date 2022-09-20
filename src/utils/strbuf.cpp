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

#include <cstring>
#include "logger.h"
#include "memmgr.h"
#include "strbuf.h"
#include "utils.h"


namespace tt
{


StringBuffer::StringBuffer() :
    m_cursor(0)
{
    m_buffs.push_back((char*)MemoryManager::alloc_memory_page());
}

StringBuffer::~StringBuffer()
{
    for (char *buff: m_buffs)
    {
        MemoryManager::free_memory_page((void*)buff);
    }
}

char *
StringBuffer::strdup(const char *str)
{
    size_t buff_size = g_page_size - 1;

    ASSERT(str != nullptr);
    ASSERT(std::strlen(str) < buff_size);

    size_t len = std::strlen(str);

    if (UNLIKELY(len > buff_size))
    {
        Logger::error("Can't fit str into StringBuffer: '%s'", str);
        throw std::out_of_range("string too long to fit into StringBuffer");
    }

    if ((m_cursor + len) >= buff_size)
    {
        m_cursor = 0;
        m_buffs.push_back((char*)MemoryManager::alloc_memory_page());
    }

    char *buff = m_buffs.back() + m_cursor;
    std::strncpy(buff, str, len+1);
    buff[len] = 0;
    m_cursor += len + 1;

    return buff;
}


}
