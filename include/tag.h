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

#include <cassert>
#include <cstddef>
#include <map>
#include <set>
#include "kv.h"
#include "strbuf.h"
#include "type.h"
#include "utils.h"


namespace tt
{


typedef KeyValuePair Tag;


class TagOwner
{
public:
    TagOwner(bool own_mem);
    TagOwner(TagOwner&& src);   // move constructor
    TagOwner(const TagOwner&) = delete; // copy constructor
    virtual ~TagOwner();

    void init(bool own_mem);
    void recycle();

    Tag *find_by_key(const char *key);

    inline const char* get_tag_value(const char *tag_name) const
    {
        return KeyValuePair::get_value(m_tags, tag_name);
    }

    inline Tag *get_tags()
    {
        return m_tags;
    }

    inline Tag *get_cloned_tags() const // return list of tags that can be kept by caller
    {
        return KeyValuePair::clone(m_tags);
    }

    inline Tag *get_cloned_tags(StringBuffer& strbuf) const // return list of tags that can be kept by caller
    {
        return KeyValuePair::clone(m_tags, strbuf);
    }

    char *get_ordered_tags(char* buff, size_t size) const;

    void get_keys(std::set<std::string>& keys) const;
    void get_values(std::set<std::string>& values) const;

/*
    inline void add_tag(Tag *tag)
    {
        ASSERT(tag != nullptr);
        KeyValuePair::insert_in_order(&m_tags, tag->m_key, tag->m_value);
    }
*/

    inline void add_tag(const char *name, const char *value)
    {
        ASSERT(std::strchr(name, ' ') == nullptr);
        ASSERT(std::strchr(value, ' ') == nullptr);
        ASSERT(std::strlen(name) > 0);
        ASSERT(std::strlen(value) > 0);
        ASSERT(std::strchr(name, '"') == nullptr);
        ASSERT(std::strchr(value, '"') == nullptr);

        KeyValuePair::insert_in_order(&m_tags, name, value);
    }

    void remove_tag(const char *key);

    inline void remove_all_tags()
    {
        Tag::free_list(m_tags, m_own_mem);
    }

    inline void set_tags(Tag *tags)
    {
        ASSERT(m_tags == nullptr);
        m_tags = tags;
    }

    inline int get_tag_count() const { return get_tag_count(m_tags); }
    static int get_tag_count(Tag *tags);

protected:
    bool m_own_mem; // should we free m_key and m_value?
    Tag *m_tags;
};


}
