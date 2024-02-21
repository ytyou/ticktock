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

#include <string>
#include "recycle.h"
#include "strbuf.h"


namespace tt
{


class KeyValuePair : public Recyclable
{
public:
    KeyValuePair();
    KeyValuePair(const char *key, const char *value);

    inline KeyValuePair*& next()
    {
        return (KeyValuePair*&)Recyclable::next();
    }

    static const char* get_value(KeyValuePair *list, const char *key);
    static KeyValuePair* get_key_value_pair(KeyValuePair *list, const char *key);

    static bool has_key(KeyValuePair *list, const char *key);
    static bool has_key_value(KeyValuePair *list, const char *key, const char *value);
    static bool match_value(KeyValuePair *list, const char *key, const char *value);

    static void prepend(KeyValuePair **list, KeyValuePair *kv);
    static void prepend(KeyValuePair **list, const char *key, const char *value);
    static void insert_in_order(KeyValuePair **list, const char *key, const char *value);
    static KeyValuePair *remove_first(KeyValuePair **list, const char *key);

    static KeyValuePair* clone(KeyValuePair *list);
    static KeyValuePair* clone(KeyValuePair *list, StringBuffer& strbuf);
    static void free_list(KeyValuePair *list, bool deep = false);

    static char* to_json(KeyValuePair *list, char *buff, int size);

    // WARNING: The input 'buff' will be modified!
    static KeyValuePair* parse_in_place(char *buff, char delim);
    static KeyValuePair* parse_multiple(std::string& buff);

    const char *m_key;
    const char *m_value;
};


}
