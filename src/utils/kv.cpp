/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),
    Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).

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

#include "memmgr.h"
#include "kv.h"
#include "logger.h"
#include "utils.h"
#include "leak.h"


namespace tt
{


KeyValuePair::KeyValuePair() :
    KeyValuePair(nullptr, nullptr)
{
}

KeyValuePair::KeyValuePair(const char *key, const char *value) :
    m_key(key),
    m_value(value)
{
}

bool
KeyValuePair::has_key(KeyValuePair *list, const char *key)
{
    if (key == nullptr)
    {
        Logger::warn("nullptr passed into KeyValuePair::has_key()");
        return false;
    }

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        if (strcmp(key, kv->m_key) == 0) return true;
    }

    return false;
}

bool
KeyValuePair::has_key_value(KeyValuePair *list, const char *key, const char *value)
{
    if ((key == nullptr) || (value == nullptr))
    {
        Logger::warn("nullptr passed into KeyValuePair::has_key_value()");
        return false;
    }

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        if ((strcmp(key, kv->m_key) == 0) && (strcmp(value, kv->m_value) == 0))
        {
            return true;
        }
    }

    return false;
}

bool
KeyValuePair::match_value(KeyValuePair *list, const char *key, const char *value)
{
    ASSERT(value != nullptr);

    KeyValuePair *kv = get_key_value_pair(list, key);

    if (kv == nullptr) return false;

    if (std::strchr(value, '|') != nullptr)
    {
        char buff[std::strlen(value)+1];
        std::strcpy(buff, value);
        std::vector<char*> tokens;
        tokenize(buff, '|', tokens);
        for (char *v: tokens)
        {
            if (std::strcmp(kv->m_value, v) == 0)
            {
                return true;
            }
        }
        return false;
    }
    else if (ends_with(value, "*"))
    {
        size_t len = std::strlen(value) - 1;
        return (std::strncmp(kv->m_value, value, len) == 0);
    }
    else
    {
        return (std::strcmp(kv->m_value, value) == 0);
    }
}

void
KeyValuePair::prepend(KeyValuePair **list, KeyValuePair *kv)
{
    if ((list == nullptr) || (kv == nullptr))
    {
        Logger::warn("nullptr passed into KeyValuePair::prepend()");
        return;
    }

    kv->next() = *list;
    *list = kv;
}

void
KeyValuePair::prepend(KeyValuePair **list, const char *key, const char *value)
{
    if ((list == nullptr) || (key == nullptr) || (value == nullptr))
    {
        Logger::warn("nullptr passed into KeyValuePair::prepend()");
        return;
    }

    MemoryManager *mm = MemoryManager::inst();
    KeyValuePair *kv = (KeyValuePair*)mm->alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);
    kv->m_key = key;
    kv->m_value = value;
    kv->next() = *list;
    *list = kv;
}

void
KeyValuePair::insert_in_order(KeyValuePair **list, const char *key, const char *value)
{
    if ((list == nullptr) || (key == nullptr) || (value == nullptr))
    {
        Logger::warn("nullptr passed into KeyValuePair::insert_in_order()");
        return;
    }

    KeyValuePair *kv = (KeyValuePair*)MemoryManager::alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);

    kv->m_key = key;
    kv->m_value = value;

    // insert it in sorted order
    if ((*list == nullptr) || (strcmp(kv->m_key, (*list)->m_key) <= 0))
    {
        kv->next() = *list;
        *list = kv;
    }
    else
    {
        KeyValuePair *next;

        for (next = *list;
             (next->next() != nullptr) && (strcmp(next->next()->m_key, kv->m_key) < 0);
             next = next->next())
        {
            /* do nothing */
        }

        kv->next() = (KeyValuePair*)next->next();
        next->next() = kv;
    }
}

KeyValuePair *
KeyValuePair::remove_first(KeyValuePair **list, const char *key)
{
    ASSERT(key != nullptr);
    ASSERT(list != nullptr);

    if (*list == nullptr) return nullptr;

    // first?
    if (std::strcmp((*list)->m_key, key) == 0)
    {
        KeyValuePair *removed = *list;
        *list = removed->next();
        removed->next() = nullptr;
        return removed;
    }

    for (KeyValuePair *next = *list; next->next() != nullptr; next = next->next())
    {
        if (std::strcmp(next->next()->m_key, key) == 0)
        {
            KeyValuePair *removed = next->next();
            next->next() = removed->next();
            removed->next() = nullptr;
            return removed;
        }
    }

    return nullptr;
}

KeyValuePair *
KeyValuePair::get_key_value_pair(KeyValuePair *list, const char *key)
{
    if ((list == nullptr) || (key == nullptr))
    {
        Logger::debug("nullptr passed into KeyValuePair::get_key_value_pair()");
        return nullptr;
    }

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        if (strcmp(kv->m_key, key) == 0) return kv;
    }

    return nullptr;
}

const char *
KeyValuePair::get_value(KeyValuePair *list, const char *key)
{
    KeyValuePair *kv = get_key_value_pair(list, key);
    return (kv == nullptr) ? nullptr : kv->m_value;
}

KeyValuePair *
KeyValuePair::clone(KeyValuePair *list)
{
    MemoryManager *mm = MemoryManager::inst();
    KeyValuePair *dup = nullptr;
    KeyValuePair *last = nullptr;

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        KeyValuePair *tmp =
            (KeyValuePair*)mm->alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);

        tmp->m_key = STRDUP(kv->m_key);
        tmp->m_value = STRDUP(kv->m_value);
        tmp->next() = nullptr;

        if (last == nullptr)
        {
            dup = last = tmp;
        }
        else
        {
            last->next() = tmp;
            last = tmp;
        }
    }

    return dup;
}

KeyValuePair *
KeyValuePair::clone(KeyValuePair *list, StringBuffer& strbuf)
{
    MemoryManager *mm = MemoryManager::inst();
    KeyValuePair *dup = nullptr;
    KeyValuePair *last = nullptr;

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        KeyValuePair *tmp =
            (KeyValuePair*)mm->alloc_recyclable(RecyclableType::RT_KEY_VALUE_PAIR);

        tmp->m_key = strbuf.strdup(kv->m_key);
        tmp->m_value = strbuf.strdup(kv->m_value);
        tmp->next() = nullptr;

        if (last == nullptr)
        {
            dup = last = tmp;
        }
        else
        {
            last->next() = tmp;
            last = tmp;
        }
    }

    return dup;
}

void
KeyValuePair::free_list(KeyValuePair *list, bool deep)
{
    MemoryManager *mm = MemoryManager::inst();

    while (list != nullptr)
    {
        KeyValuePair *kv = list;
        list = kv->next();

        if (deep)
        {
            FREE((char*)kv->m_key);
            FREE((char*)kv->m_value);
        }

        mm->free_recyclable(kv);
    }
}

char *
KeyValuePair::to_json(KeyValuePair *list, char *buff, int size)
{
    int n = snprintf(buff, size, "{");

    for (KeyValuePair *kv = list; kv != nullptr; kv = kv->next())
    {
        if (buff[n-1] != '{')
        {
            n += snprintf(buff+n, size-n, ",");
        }
        n += snprintf(buff+n, size-n, "\"%s\":\"%s\"", kv->m_key, kv->m_value);
    }

    n += snprintf(buff+n, size-n, "}");
    return buff;
}

KeyValuePair *
KeyValuePair::parse_in_place(char *buff, char delim)
{
    KeyValuePair *list = nullptr;

    std::vector<char*> tokens;
    tokenize(buff, delim, tokens);

    for (char *token: tokens)
    {
        char *key, *value;
        tokenize(token, key, value, '=');
        KeyValuePair::prepend(&list, key, value);
    }

    return list;
}


}
