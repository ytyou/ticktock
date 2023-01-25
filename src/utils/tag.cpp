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
#include "leak.h"
#include "logger.h"
#include "memmgr.h"
#include "rw.h"
#include "tag.h"


namespace tt
{


TagId Tag_v2::m_next_id = TT_FIELD_TAG_ID+1;
default_contention_free_shared_mutex Tag_v2::m_lock;
std::unordered_map<const char*,TagId,hash_func,eq_func> Tag_v2::m_map =
    {{TT_FIELD_TAG_NAME, TT_FIELD_TAG_ID}};
const char **Tag_v2::m_names = nullptr;
uint32_t Tag_v2::m_names_capacity = 0;


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

/* Format: tag1=value1,tag2=value2,...
 *   Comma, Equals Sign, Space are allowed, as long as
 *   they are escaped with a '\'.
 * We assume there's at least 1 tag present.
 */
bool
TagOwner::parse(char *tags)
{
    ASSERT(tags != nullptr);

    do
    {
        const char *key = tags;

        while (*tags != '=' || *(tags-1) == '\\')
            tags++;
        if (UNLIKELY(*tags != '=')) return false;
        *tags++ = 0;

        const char *value = tags;

        while ((*tags != ',' || *(tags-1) == '\\') && (*tags != 0))
            tags++;
        if (*tags != 0) *tags++ = 0;

        add_tag(key, value);
    } while (*tags != 0);

    return true;
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

int
TagOwner::get_tag_count(Tag *tags)
{
    int count = 0;

    for (Tag *tag = tags; tag != nullptr; tag = tag->next(), count++)
        /* do nothing */;

    return count;
}


Tag_v2::Tag_v2(Tag *tags)
{
    if (tags == nullptr)
    {
        m_count = 0;
        m_tags = nullptr;
    }
    else
    {
        ASSERT(TagOwner::get_tag_count(tags) <= UINT16_MAX);
        m_count = (TagCount)TagOwner::get_tag_count(tags);
        if (m_count > 0)
        {
            m_tags = (TagId*) calloc(2 * m_count, sizeof(TagId));
            int i = 0;

            for (Tag *tag = tags; tag != nullptr; tag = tag->next())
            {
                m_tags[i++] = get_or_set_id(tag->m_key);
                m_tags[i++] = get_or_set_id(tag->m_value);
            }

            ASSERT(i == (2 * m_count));
        }
        else
            m_tags = nullptr;
    }
}

Tag_v2::Tag_v2(TagBuilder& builder)
{
    m_count = builder.get_count();

    if (m_count == 0)
        m_tags = nullptr;
    else
    {
        m_tags = (TagId*) calloc(2 * m_count, sizeof(TagId));
        std::memcpy(m_tags, builder.get_ids(), 2*m_count*sizeof(TagId));
    }
}

Tag_v2::Tag_v2(Tag_v2& tags) :
    m_count(tags.m_count),
    m_tags(tags.m_tags)
{
    if (m_count == 0)
        m_tags = nullptr;
    else
    {
        ASSERT(tags.m_tags != nullptr);
        m_tags = (TagId*) calloc(2 * m_count, sizeof(TagId));
        std::memcpy(m_tags, tags.m_tags, 2*m_count*sizeof(TagId));
    }
}

Tag_v2::~Tag_v2()
{
    if (m_tags != nullptr)
        std::free(m_tags);
}

TagId
Tag_v2::get_or_set_id(const char *name)
{
    {
        ReadLock guard(m_lock);
        auto search = m_map.find(name);
        if (search != m_map.end())
            return search->second;
    }

    WriteLock guard(m_lock);

    auto search = m_map.find(name);
    if (search != m_map.end())
        return search->second;

    m_map[set_name(m_next_id, name)] = m_next_id;
    return m_next_id++;
}

const char *
Tag_v2::get_name(TagId id)
{
    ReadLock guard(m_lock);

    if (UNLIKELY(m_names_capacity <= id))
        return nullptr;
    else
        return m_names[id];
}

const char *
Tag_v2::set_name(TagId id, const char *name)
{
    if (m_names_capacity <= id)
    {
        uint32_t new_capacity = id + 256;
        const char **tmp = (const char **) calloc(new_capacity, sizeof(const char*));
        std::memcpy(tmp, m_names, m_names_capacity * sizeof(const char *));
        std::memset(&tmp[m_names_capacity], 0, (new_capacity-m_names_capacity) * sizeof(const char *));
        if (m_names != nullptr) std::free(m_names);
        m_names = tmp;
        if (m_names_capacity == 0) m_names[0] = TT_FIELD_TAG_NAME;
        m_names_capacity = new_capacity;
    }

    m_names[id] = STRDUP(name);
    return m_names[id];
}

TagId
Tag_v2::get_id(const char *name)
{
    ReadLock guard(m_lock);
    auto search = m_map.find(name);
    if (search != m_map.end())
        return search->second;
    else
        return TT_INVALID_TAG_ID;
}

TagId
Tag_v2::get_value_id(TagId key_id)
{
    for (int i = 0; i < 2*m_count; i += 2)
    {
        if (m_tags[i] == key_id)
            return m_tags[i+1];
    }
    return TT_INVALID_TAG_ID;
}

bool
Tag_v2::match(TagId key_id)
{
    for (int i = 0; i < 2*m_count; i += 2)
    {
        if (m_tags[i] == key_id)
            return true;
    }
    return false;
}

// 'value' ends with '*'
bool
Tag_v2::match(TagId key_id, const char *value)
{
    TagId vid = get_value_id(key_id);
    if (TT_INVALID_TAG_ID == vid) return false;

    std::size_t len = std::strlen(value) - 1;
    return (std::strncmp(get_name(vid), value, len) == 0);
}

bool
Tag_v2::match(TagId key_id, std::vector<TagId> value_ids)
{
    for (int i = 0; i < 2*m_count; i += 2)
    {
        if (m_tags[i] == key_id)
        {
            TagId target = m_tags[i+1];
            for (auto vid : value_ids)
            {
                if (vid == target)
                    return true;
            }
            return false;
        }
    }
    return false;
}

bool
Tag_v2::match(const char *key, const char *value)
{
    ASSERT(key != nullptr);
    ASSERT(value != nullptr);
    //ASSERT(std::strchr(value, '|') == nullptr);

    TagId kid = get_id(key);
    if (TT_INVALID_TAG_ID == kid) return false;

    TagId vid = get_value_id(kid);
    if (TT_INVALID_TAG_ID == vid) return false;

    const char *vname = get_name(vid);
    ASSERT(vname != nullptr);

    if (std::strchr(value, '|') != nullptr)
    {
        char buff[std::strlen(value)+1];
        std::strcpy(buff, value);
        std::vector<char*> tokens;
        tokenize(buff, '|', tokens);
        for (char *v: tokens)
        {
            if (std::strcmp(vname, v) == 0)
                return true;
        }
        return false;
    }
    else if (ends_with(value, '*'))
    {
        size_t len = std::strlen(value) - 1;
        return (std::strncmp(vname, value, len) == 0);
    }
    else
    {
        return (std::strcmp(vname, value) == 0);
    }
}

bool
Tag_v2::match_last(TagId key_id, TagId value_id)
{
    if (m_count == 0) return false;
    TagCount cnt = 2 * m_count;
    return (m_tags[cnt-2] == key_id) && (m_tags[cnt-1] == value_id);
}

Tag *
Tag_v2::get_v1_tags() const
{
    Tag *head = nullptr;

    for (int i = 2*m_count-1; i >= 0; i -= 2)
    {
        Tag *tag = (Tag *)
            MemoryManager::alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);
        tag->m_key = get_name(m_tags[i-1]);
        tag->m_value = get_name(m_tags[i]);
        tag->next() = head;
        head = tag;
    }

    return head;
}

Tag *
Tag_v2::get_cloned_v1_tags(StringBuffer& strbuf) const
{
    Tag *head = nullptr;

    for (int i = 2*m_count-1; i >= 0; i -= 2)
    {
        Tag *tag = (Tag *)
            MemoryManager::alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);
        tag->m_key = strbuf.strdup(get_name(m_tags[i-1]));
        tag->m_value = strbuf.strdup(get_name(m_tags[i]));
        tag->next() = head;
        head = tag;
    }

    return head;
}

void
Tag_v2::get_keys(std::set<std::string>& keys) const
{
    for (int i = 0; i < 2*m_count; i += 2)
        keys.insert(get_name(m_tags[i]));
}

void
Tag_v2::get_values(std::set<std::string>& values) const
{
    for (int i = 1; i < 2*m_count; i += 2)
        values.insert(get_name(m_tags[i]));
}


TagBuilder::TagBuilder(TagCount capacity, TagId *tags) :
    m_count(0),
    m_capacity(capacity),
    m_tags(tags)
{
}

void
TagBuilder::init(Tag *tags)
{
    if (tags == nullptr)
        m_count = 0;
    else
    {
        int i = 0;

        for (Tag *tag = tags; tag != nullptr; tag = tag->next())
        {
            ASSERT((i + 1) < (2 * m_capacity));
            m_tags[i++] = Tag_v2::get_or_set_id(tag->m_key);
            m_tags[i++] = Tag_v2::get_or_set_id(tag->m_value);
        }

        m_count = i / 2;
        ASSERT(i == (2 * (m_capacity - 1)));
    }
}

void
TagBuilder::update_last(TagId kid, const char *value)
{
    ASSERT(value != nullptr);

    m_count = m_capacity;
    m_tags[2*m_capacity-2] = kid;
    m_tags[2*m_capacity-1] = Tag_v2::get_or_set_id(value);
}


/* Examples of 'tags':
 *  1. key=value;
 *  2. key=val*;
 *  3. key=*
 *  4. key=value1|value2|value3
 */
TagMatcher::TagMatcher() :
    m_key_id(TT_INVALID_TAG_ID),
    m_value(nullptr)
{
}

void
TagMatcher::init(Tag *tags)
{
    ASSERT(tags != nullptr);
    ASSERT(next() == nullptr);

    m_key_id = Tag_v2::get_id(tags->m_key);

    if (TT_INVALID_TAG_ID == m_key_id)
        return;     // NOT going to match ANYTHING

    if (std::strchr(tags->m_value, '|') != nullptr)
    {
        char buff[std::strlen(tags->m_value)+1];
        std::strcpy(buff, tags->m_value);
        std::vector<char*> tokens;
        tokenize(buff, '|', tokens);
        for (char *v: tokens)
        {
            TagId id = Tag_v2::get_id(v);
            if (id != TT_INVALID_TAG_ID)
                m_value_ids.push_back(id);
        }
    }
    else if (ends_with(tags->m_value, '*'))
    {
        if (tags->m_value[1] != 0)  // key=val*
            m_value = tags->m_value;
    }
    else
    {
        m_value_ids.push_back(Tag_v2::get_id(tags->m_value));
    }

    if (tags->next() == nullptr)
        next() = nullptr;
    else
    {
        TagMatcher *matcher = (TagMatcher*)
            MemoryManager::alloc_recyclable(RecyclableType::RT_TAG_MATCHER);
        matcher->init(tags->next());
        next() = matcher;
    }
}

bool
TagMatcher::match(Tag_v2& tags)
{
    if (TT_INVALID_TAG_ID == m_key_id)
        return false;

    TagMatcher *next = this->next();

    if ((next != nullptr) && ! next->match(tags))
        return false;

    if (! m_value_ids.empty())
        return tags.match(m_key_id, m_value_ids);

    if (m_value != nullptr)
        return tags.match(m_key_id, m_value);

    return tags.match(m_key_id);
}

bool
TagMatcher::recycle()
{
    TagMatcher *next = this->next();
    if (next != nullptr)
        MemoryManager::free_recyclable(next);
    m_key_id = TT_INVALID_TAG_ID;
    m_value = nullptr;
    m_value_ids.clear();
    return true;
}


}
