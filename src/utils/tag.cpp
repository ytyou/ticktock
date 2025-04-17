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
#include "tag.h"


namespace tt
{


TagId Tag_v2::m_next_id = TT_FIELD_VALUE_ID+1;
//default_contention_free_shared_mutex Tag_v2::m_lock;
pthread_rwlock_t Tag_v2::m_lock;
std::unordered_map<const char*,TagId,hash_func,eq_func> Tag_v2::m_map =
    {{TT_FIELD_TAG_NAME, TT_FIELD_TAG_ID},{TT_FIELD_VALUE,TT_FIELD_VALUE_ID}};
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

        while ((*tags != '=' || *(tags-1) == '\\') && (*tags != '\n') && (*tags != 0))
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
TagOwner::remove_tag(Tag *tag)
{
    Tag::free_list(tag, m_own_mem);
}

Tag *
TagOwner::remove_tag(const char *key, bool free)
{
    ASSERT(key != nullptr);
    Tag *removed = KeyValuePair::remove_first(&m_tags, key);
    if (free && (removed != nullptr))
    {
        Tag::free_list(removed, m_own_mem);
        removed = nullptr;
    }
    return removed;
}

Tag *
TagOwner::find_by_key(Tag *tags, const char *key)
{
    ASSERT(key != nullptr);
    if (tags == nullptr) return nullptr;
    return KeyValuePair::get_key_value_pair(tags, key);
}

// return true if this is less than the 'other', in alphabetical order
// of tag names, values
bool
TagOwner::less_than(const TagOwner& other) const
{
    Tag *tag1, *tag2;
    int i;

    for (tag1 = m_tags, tag2 = other.m_tags; tag1 != nullptr && tag2 != nullptr; tag1 = tag1->next(), tag2 = tag2->next())
    {
        i = strcmp(tag1->m_key, tag2->m_key);
        if (i != 0) return i < 0;

        i = strcmp(tag1->m_value, tag2->m_value);
        if (i != 0) return i < 0;
    }

    if ((tag1 == nullptr) && (tag2 != nullptr))
        return true;

    // make sure this is irreflexive by returning false
    return false;
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
        if ((tag->m_key[0] == '_') && (strcmp(tag->m_key, TT_FIELD_TAG_NAME) == 0))
            continue;
        ASSERT(strcmp(tag->m_key, METRIC_TAG_NAME) != 0);
        n = std::snprintf(curr, size, "%s=%s,", tag->m_key, tag->m_value);
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
    else
    {
        ASSERT(*curr == 0);
        curr--;
        ASSERT(*curr == ',');
        *curr = 0;  // remove last comma
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
TagOwner::get_tag_count(Tag *tags, bool excludeField)
{
    int count = 0;

    if (excludeField)
    {
        for (Tag *tag = tags; tag != nullptr; tag = tag->next())
        {
            if ((tag->m_key[0] != '_') || (std::strcmp(tag->m_key, TT_FIELD_TAG_NAME) != 0))
                count++;
        }
    }
    else
    {
        for (Tag *tag = tags; tag != nullptr; tag = tag->next(), count++)
            /* do nothing */;
    }

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
        ASSERT(TagOwner::get_tag_count(tags, false) <= UINT16_MAX);
        m_count = (TagCount)TagOwner::get_tag_count(tags, false);
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

void
Tag_v2::init()
{
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&m_lock, &attr);
}

void
Tag_v2::append(TagId key_id, TagId value_id)
{
    TagId *tags = (TagId*) calloc(2 * (m_count+2), sizeof(TagId));
    if (m_tags != nullptr)
    {
        std::memcpy(tags, m_tags, 2*m_count*sizeof(TagId));
        std::free(m_tags);
    }
    tags[2*m_count] = key_id;
    tags[2*m_count+1] = value_id;
    m_tags = tags;
    m_count++;
}

TagId
Tag_v2::get_or_set_id(const char *name)
{
    {
        PThread_ReadLock guard(&m_lock);
        auto search = m_map.find(name);
        if (search != m_map.end())
            return search->second;
    }

    PThread_WriteLock guard(&m_lock);

    auto search = m_map.find(name);
    if (search != m_map.end())
        return search->second;

    m_map[set_name(m_next_id, name)] = m_next_id;
    return m_next_id++;
}

const char *
Tag_v2::get_name(TagId id)
{
    PThread_ReadLock guard(&m_lock);

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
        if (m_names != nullptr)
            std::memcpy(tmp, m_names, m_names_capacity * sizeof(const char *));
        std::memset(&tmp[m_names_capacity], 0, (new_capacity-m_names_capacity) * sizeof(const char *));
        if (m_names != nullptr) std::free(m_names);
        m_names = tmp;
        if (m_names_capacity == 0)
        {
            m_names[0] = TT_FIELD_TAG_NAME;
            m_names[1] = TT_FIELD_VALUE;
        }
        m_names_capacity = new_capacity;
    }

    m_names[id] = STRDUP(name);
    return m_names[id];
}

TagId
Tag_v2::get_id(const char *name)
{
    PThread_ReadLock guard(&m_lock);
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
Tag_v2::match(TagId key_id, TagId value_id)
{
    if (TT_INVALID_TAG_ID == value_id)
        return false;

    for (int i = 0; i < 2*m_count; i += 2)
    {
        if (m_tags[i] == key_id)
            return value_id == m_tags[i+1];
    }

    return false;
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
Tag_v2::match_case_insensitive(const char *key, const char *value)
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
            if (::strcasecmp(vname, v) == 0)
                return true;
        }
        return false;
    }
    else if (ends_with(value, '*'))
    {
        size_t len = std::strlen(value) - 1;
        return (::strncasecmp(vname, value, len) == 0);
    }
    else
    {
        return (::strcasecmp(vname, value) == 0);
    }
}

bool
Tag_v2::match_last(TagId key_id, TagId value_id) const
{
    if (m_count == 0) return false;
    TagCount cnt = 2 * m_count;
    return (m_tags[cnt-2] == key_id) && (m_tags[cnt-1] == value_id);
}

const char *
Tag_v2::get_value(TagId key_id)
{
    for (int i = 0; i < 2*m_count; i += 2)
        if (m_tags[i] == key_id)
            return get_name(m_tags[i+1]);
    return nullptr;
}

bool
Tag_v2::exists(const char *key) const
{
    ASSERT(key != nullptr);
    TagId kid = get_id(key);

    for (int i = 0; i < 2*m_count; i += 2)
        if (m_tags[i] == kid)
            return true;

    return false;
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
        ASSERT(tag->m_key != nullptr);
        ASSERT(tag->m_value != nullptr);
        tag->next() = head;
        head = tag;
    }

    return head;
}

Tag *
Tag_v2::get_ordered_v1_tags() const
{
    Tag *head = nullptr;

    for (int i = 2*m_count-1; i >= 0; i -= 2)
        KeyValuePair::insert_in_order(&head, get_name(m_tags[i-1]), get_name(m_tags[i]));

    return head;
}

Tag *
Tag_v2::get_cloned_v1_tags(StringBuffer& strbuf) const
{
    Tag *head = nullptr;

    for (int i = 2*m_count-1; i >= 0; i -= 2)
        KeyValuePair::insert_in_order(&head, strbuf.strdup(get_name(m_tags[i-1])), strbuf.strdup(get_name(m_tags[i])));

    return head;
}

TagCount
Tag_v2::clone(TagId *tags, TagCount capacity)
{
    ASSERT(tags != nullptr);
    ASSERT(m_count <= capacity);
    std::memcpy(tags, m_tags, 2*m_count*sizeof(TagId));
    return m_count;
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
TagBuilder::init(Tag_v2& tags)
{
    m_count = tags.clone(m_tags, m_capacity);
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
    m_value_id(TT_INVALID_TAG_ID),
    m_regex(nullptr)
{
}

void
TagMatcher::init(Tag *tags)
{
    ASSERT(tags != nullptr);
    ASSERT(tags->m_key != nullptr);
    ASSERT(tags->m_value != nullptr);
    ASSERT(tags->m_value[0] != 0);
    ASSERT(m_regex == nullptr);
    ASSERT(next() == nullptr);

    char buff[MAX_TOTAL_TAG_LENGTH];
    const char *value = tags->m_value;
    size_t last = std::strlen(value);

    if (UNLIKELY(last == 0 || MAX_TOTAL_TAG_LENGTH/2 <= last))
    {
        Logger::error("Tag value invalid: %s=%s", tags->m_key, value);
        return;
    }

    last--;
    m_key_id = Tag_v2::get_id(tags->m_key);
    m_value_id = TT_INVALID_TAG_ID;

    if (TT_INVALID_TAG_ID == m_key_id)
        return;     // NOT going to match ANYTHING

    // handle 'literal_or(...)'
    if (starts_with(value, "literal_or(") && (value[last] == ')'))
    {
        m_regex = new std::regex(value+10, std::regex_constants::ECMAScript);
    }
    else if (starts_with(value, "iliteral_or(") && (value[last] == ')'))
    {
        m_regex = new std::regex(value+11, std::regex_constants::ECMAScript | std::regex_constants::icase);
    }
    else if (starts_with(value, "not_literal_or(") && (value[last] == ')'))
    {
        // use pattern (?!...|...) for negative match
        //buff[0] = '('; buff[1] = '?'; buff[2] = '!';
        //std::strncpy(&buff[3], value+15, sizeof(buff)-3);
        //std::strcat(buff, "(.*)");
        replace_literal_or(buff, value+14);
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript);
    }
    else if (starts_with(value, "not_iliteral_or(") && (value[last] == ')'))
    {
        // use pattern (?!...|...)(.*) for negative match
        //buff[0] = '('; buff[1] = '?'; buff[2] = '!';
        //std::strncpy(&buff[3], value+16, sizeof(buff)-7);
        //std::strcat(buff, "(.*)");
        replace_literal_or(buff, value+15);
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript | std::regex_constants::icase);
    }
    else if (starts_with(value, "wildcard(") && (value[last] == ')'))
    {
        // copy whatever is in (...) into buff[]
        // escape special characters '.' and '*'
        int len = replace_stars(buff, value+9);
        buff[len-1] = 0;    // remove ')'
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript);
    }
    else if (starts_with(value, "iwildcard(") && (value[last] == ')'))
    {
        // copy whatever is in (...) into buff[]
        // escape special characters '.' and '*'
        int len = replace_stars(buff, value+10);
        buff[len-1] = 0;    // remove ')'
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript | std::regex_constants::icase);
    }
    else if (starts_with(value, "regexp(") && (value[last] == ')'))
    {
        std::strncpy(buff, value+7, sizeof(buff));
        buff[std::strlen(buff)-1] = 0;  // remove trailing ')'
        m_regex = new std::regex(buff, std::regex_constants::basic);
    }
    else if (ends_with(value, '*'))
    {
        // OpenTSDB 1.x - 2.1 filter
        int len = replace_stars(buff, value);
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript);
    }
    else if (std::strchr(value, '|') != nullptr)
    {
        // OpenTSDB 1.x - 2.1 filter
        buff[0] = '(';
        std::strncpy(buff, value, sizeof(buff)-2);
        buff[last+2] = ')'; buff[last+3] = 0;
        m_regex = new std::regex(buff, std::regex_constants::ECMAScript);
    }
    else
    {
        m_value_id = Tag_v2::get_id(value);
        m_regex = nullptr;
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

int
TagMatcher::replace_stars(char *dst, const char *src)
{
    int i, j;

    for (i = 0, j = 0; src[j] != 0; i++, j++)
    {
        if (src[j] == '.')      // escape '.'
        {
            dst[i++] = '\\';
            dst[i] = '.';
        }
        else if (src[j] == '*') // replace '*' with '.*'
        {
            dst[i++] = '.';
            dst[i] = '*';
        }
        else
            dst[i] = src[j];
    }

    dst[i] = 0;     // null-terminate
    return i;
}

/* @param src example: '(web01|web02)'
 * @param dst example: '(?!web01$|web02$)(.*)'
 */
std::size_t
TagMatcher::replace_literal_or(char *dst, const char *src)
{
    ASSERT(dst != nullptr);
    ASSERT(src != nullptr);

    dst[0] = '('; dst[1] = '?'; dst[2] = '!'; dst[3] = 0;

    char buff[std::strlen(src)+1];
    std::vector<char*> tokens;

    std::strcpy(buff, src+1);       // skip leading '('
    buff[std::strlen(buff)-1] = 0;  // remove trailing ')'
    tokenize(buff, '|', tokens);

    for (char *token: tokens)
    {
        strcat(dst, token);
        strcat(dst, "$|");
    }

    std::size_t len = std::strlen(dst);
    dst[len-1] = 0; // remove last '|'
    strcat(dst+len-2, ")(.*)");
    return len+3;
}

bool
TagMatcher::match(Tag_v2& tags)
{
    if (TT_INVALID_TAG_ID == m_key_id)
        return false;

    TagMatcher *next = this->next();

    if ((next != nullptr) && ! next->match(tags))
        return false;

    if (m_regex == nullptr)
        return tags.match(m_key_id, m_value_id);
    else
    {
        const char *value = tags.get_value(m_key_id);
        if (value == nullptr) return false;
        std::cmatch matches;
        return std::regex_match(value, matches, *m_regex);
    }
}

bool
TagMatcher::recycle()
{
    TagMatcher *next = this->next();
    if (next != nullptr)
        MemoryManager::free_recyclable(next);
    m_key_id = TT_INVALID_TAG_ID;
    m_value_id = TT_INVALID_TAG_ID;
    if (m_regex != nullptr)
    {
        delete m_regex;
        m_regex = nullptr;
    }
    return true;
}


}
