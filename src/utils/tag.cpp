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

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "global.h"
#include "logger.h"
#include "tag.h"


namespace tt
{


TagOwner::TagOwner(bool own_mem) :
    m_own_mem(own_mem),
    m_tags(nullptr)
{
}

TagOwner::TagOwner(TagOwner&& src) :
    m_own_mem(src.m_own_mem),
    m_tags(src.m_tags)
{
    src.m_tags = nullptr;
}

TagOwner::~TagOwner()
{
    recycle();
}

void
TagOwner::init(bool own_mem)
{
    m_own_mem = own_mem;
    m_tags = nullptr;
}

void
TagOwner::recycle()
{
    if (m_tags != nullptr)
    {
        Tag::free_list(m_tags, m_own_mem);
        m_tags = nullptr;
    }
}

void
TagOwner::remove_tag(const char *key)
{
    ASSERT(key != nullptr);
    Tag *removed = KeyValuePair::remove_first(&m_tags, key);
    Tag::free_list(removed, m_own_mem);
}

Tag *
TagOwner::find_by_key(const char *key)
{
    ASSERT(key != nullptr);
    if (m_tags == nullptr) return nullptr;
    return KeyValuePair::get_key_value_pair(m_tags, key);
}

char *
TagOwner::get_ordered_tags(char *buff, size_t size) const
{
    int n;
    char *curr = buff;

    *curr = 0;

    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        //if (strcmp(tag->m_key, METRIC_TAG_NAME) == 0) continue;
        ASSERT(strcmp(tag->m_key, METRIC_TAG_NAME) != 0);
        n = std::snprintf(curr, size, "%s=%s;", tag->m_key, tag->m_value);
        if (size <= n) break;
        size -= n;
        curr += n;
    }

    // empty?
    if (buff[0] == 0)
    {
        buff[0] = ';';
        buff[1] = 0;
    }

    return buff;
}

void
TagOwner::get_keys(std::set<std::string>& keys) const
{
    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        if ((tag->m_key != nullptr) && (tag->m_key[0] != 0))
            keys.insert(tag->m_key);
    }
}

void
TagOwner::get_values(std::set<std::string>& values) const
{
    for (Tag *tag = m_tags; tag != nullptr; tag = tag->next())
    {
        if ((tag->m_value != nullptr) && (tag->m_value[0] != 0))
            values.insert(tag->m_value);
    }
}


}
