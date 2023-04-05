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
#include <unordered_map>
#include "kv.h"
#include "lock.h"
#include "strbuf.h"
#include "type.h"
#include "utils.h"


namespace tt
{


// TT_FIELD_VALUE is used when there's NO field
#define TT_FIELD_TAG_ID     0
#define TT_FIELD_TAG_NAME   "_field"
#define TT_FIELD_VALUE      "_"

class TagBuilder;
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

    bool parse(char *tags);

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


/* Tag names and values are stored in a hash table, and their
 * corresponding IDs (integers) are stored here. This is to
 * avoid storing the same name/value, in string format, over
 * and over, in order to save space (and speed up search).
 */
class __attribute__ ((__packed__)) Tag_v2
{
public:
    Tag_v2(Tag *tags);
    Tag_v2(Tag_v2& tags);   // copy constructor
    Tag_v2(TagBuilder& builder);
    ~Tag_v2();

    bool match(TagId key_id);
    bool match(TagId key_id, const char *value);
    bool match(TagId key_id, std::vector<TagId> value_ids);
    bool match(const char *key, const char *value);
    bool match_last(TagId key_id, TagId value_id);

    Tag *get_v1_tags() const;
    Tag *get_cloned_v1_tags(StringBuffer& strbuf) const;

    void get_keys(std::set<std::string>& keys) const;
    void get_values(std::set<std::string>& values) const;

    inline TagCount get_count() const { return m_count; }

    static void init();
    static TagId get_id(const char *name);
    static TagId get_or_set_id(const char *name);

private:
    static const char *get_value(TagId value_id);
    TagId get_value_id(TagId key_id);

    static const char *get_name(TagId id);
    static const char *set_name(TagId id, const char *name);

    TagId *m_tags;
    TagCount m_count;

    static TagId m_next_id;
    //static default_contention_free_shared_mutex m_lock;
    static pthread_rwlock_t m_lock;
    static std::unordered_map<const char*,TagId,hash_func,eq_func> m_map;
    static const char **m_names;    // indexed by id
    static uint32_t m_names_capacity;
};


class TagBuilder
{
public:
    TagBuilder(TagCount capacity, TagId *tags);
    void init(Tag *tags);
    void update_last(TagId kid, const char *value);

    inline TagCount get_count() const { return m_count; }
    inline TagId *get_ids() const { return m_tags; }

private:
    TagCount m_count;
    TagCount m_capacity;
    TagId *m_tags;
};


class TagMatcher : public Recyclable
{
public:
    TagMatcher();

    void init(Tag *tags);
    bool match(Tag_v2& tags);

    bool recycle() override;

private:
    inline TagMatcher*& next()
    {
        return (TagMatcher*&)Recyclable::next();
    }

    TagId m_key_id;
    const char *m_value;
    std::vector<TagId> m_value_ids;
};


}
