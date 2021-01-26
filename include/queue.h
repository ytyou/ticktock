/*
 * MIT License
 *
 * Copyright (c) 2018 Joe Best-Rotheray
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/* Slightly modified by Yongtao You
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <stdlib.h>
#include <inttypes.h>
#include <thread>
#include <mutex>
#include <cassert>
#include "global.h"
#include "stop.h"
#include "utils.h"


namespace tt
{


// As long as no more than 1 thread is inserting,
// and no more than 1 thread is deleting, it should
// be thread-safe. That is, the inserting thread
// and the deleting thread can be different threads.
template <typename T, size_t cache_line_size = 64> class Queue11 : public Stoppable
{
public:
    explicit Queue11(size_t capacity) :
        m_capacity(capacity), m_head(0), m_tail(0),
        m_items(static_cast<T*>(aligned_alloc(cache_line_size, sizeof(T)*capacity)))
    {
        ASSERT(m_items != NULL);
        ASSERT((((uint64_t)m_items) % cache_line_size) == 0);
    }

    virtual ~Queue11()
    {
        if (m_items != nullptr) free(m_items);
    }

    // make this class non-copyable
    Queue11(const Queue11<T>&) = delete;
    Queue11(const Queue11<T>&&) = delete;
    Queue11<T>& operator=(const Queue11<T>&) = delete;
    Queue11<T>& operator=(const Queue11<T>&&) = delete;

    inline bool is_full() { return (m_head == ((m_tail + 1) % m_capacity)); }
    inline bool is_empty() { return (m_head == m_tail); }

    inline size_t size()
    {
        int s = m_tail - m_head;
        if (s < 0) s += m_capacity;
        return s;
    }

    inline size_t capacity() { return m_capacity; }

    // this is non-blocking
    bool try_enqueue(const T& item)
    {
        uint64_t old_tail = m_tail.load(std::memory_order_relaxed);
        uint64_t new_tail = (old_tail + 1) % m_capacity;
        if (m_head.load(std::memory_order_acquire) == new_tail) return false;
        //if (is_full()) return false;
        m_items[old_tail] = item;
        m_tail.store(new_tail, std::memory_order_release);
        return true;
    }

    // this is non-blocking
    bool try_dequeue(T& item)
    {
        uint64_t old_head = m_head.load(std::memory_order_relaxed);
        if (m_tail.load(std::memory_order_acquire) == old_head) return false;
        item = m_items[old_head];
        m_head.store((old_head + 1) % m_capacity, std::memory_order_release);
        return true;
    }

    const char *c_str(char *buff, size_t size) const
    {
        if ((buff == nullptr) || (size < 32)) return EMPTY_STRING;

        sprintf(buff, "[head: %d, tail: %d]", (int)m_head, (int)m_tail);
        return buff;
    }

private:
    std::atomic<size_t> m_head __attribute__ ((aligned(cache_line_size)));
    std::atomic<size_t> m_tail __attribute__ ((aligned(cache_line_size)));
    size_t m_capacity;
    T* m_items;
} __attribute__ ((aligned(cache_line_size)));


// Implement a thread-safe, lock-free, multi-producer, multi-consumer queue.
template <typename T, size_t cache_line_size = 64> class Queue : public Stoppable
{
public:

    // allocation of memory block for m_items should be aligned to cache_line_size
    explicit Queue(size_t capacity) :
        m_items(static_cast<Item*>(aligned_alloc(cache_line_size, sizeof(Item)*capacity))),
        m_capacity(capacity), m_head(0), m_tail(0)
    {
        ASSERT(m_items != NULL);
        ASSERT((((uint64_t)m_items) % cache_line_size) == 0);

        for (size_t i = 0; i < capacity; ++i)
        {
            m_items[i].version = i;
        }
    }

    virtual ~Queue()
    {
        if (m_items != nullptr) free(m_items);
    }

    // make this class non-copyable
    Queue(const Queue<T>&) = delete;
    Queue(const Queue<T>&&) = delete;
    Queue<T>& operator=(const Queue<T>&) = delete;
    Queue<T>& operator=(const Queue<T>&&) = delete;


    // returns true if enqueue was successful; return false otherwise
    bool try_enqueue(const T& value)
    {
        uint64_t tail = m_tail.load(std::memory_order_relaxed);
        uint64_t tail_idx = tail % m_capacity;

        if (m_items[tail_idx].version.load(std::memory_order_acquire) != tail)
        {
            return false;
        }

        if (! m_tail.compare_exchange_strong(tail, tail + 1, std::memory_order_relaxed))
        {
            return false;
        }

        m_items[tail_idx].value = value;

        // Release operation, all reads/writes before this store cannot be reordered past it
        // Writing version to tail + 1 signals reader threads when to read payload
        m_items[tail_idx].version.store(tail + 1, std::memory_order_release);

        return true;
    }

    // returns true if dequeue was successful; return false otherwise
    bool try_dequeue(T& out)
    {
        uint64_t head = m_head.load(std::memory_order_relaxed);
        uint64_t head_idx = head % m_capacity;

        // Acquire here makes sure read of m_data[head].value is not reordered before this
        // Also makes sure side effects in try_enqueue are visible here
        if (m_items[head_idx].version.load(std::memory_order_acquire) != (head + 1))
        {
            return false;
        }

        if (! m_head.compare_exchange_strong(head, head + 1, std::memory_order_relaxed))
        {
            return false;
        }

        out = m_items[head_idx].value;

        // This signals to writer threads that they can now write something to this index
        m_items[head_idx].version.store(head + m_capacity, std::memory_order_release);

        return true;
    }

    // keep trying until success
    bool enqueue(const T& value)
    {
        bool success;

        while (! (success = this->try_enqueue(value)) && ! is_shutdown_requested())
        {
            std::this_thread::yield();
        }

        return success;
    }

    // keep trying until success
    bool dequeue(T& value)
    {
        bool success;

        while (! (success = this->try_dequeue(value)) && ! is_shutdown_requested())
        {
            std::this_thread::yield();
        }

        return success;
    }

    size_t capacity() const
    {
        return m_capacity;
    }

    bool is_empty() const
    {
        //uint64_t head = m_head.load(std::memory_order_relaxed);
        //return (m_items[head % m_capacity].version.load(std::memory_order_acquire) != (head + 1));
        return (size() == 0);
    }

    size_t size() const
    {
        uint64_t head = m_head.load(std::memory_order_relaxed);
        uint64_t tail = m_tail.load(std::memory_order_relaxed);
        return static_cast<size_t>(tail - head);
    }

    const char *c_str(char *buff, size_t size) const
    {
        if ((buff == nullptr) || (size < 100)) return EMPTY_STRING;

        uint64_t head = m_head.load(std::memory_order_relaxed);
        uint64_t head_idx = head % m_capacity;

        uint64_t tail = m_tail.load(std::memory_order_relaxed);
        uint64_t tail_idx = tail % m_capacity;

        uint64_t head_ver = m_items[head_idx].version.load(std::memory_order_relaxed);
        uint64_t tail_ver = m_items[tail_idx].version.load(std::memory_order_relaxed);

        sprintf(buff, "[head: %" PRIu64 ", head_ver: %" PRIu64 ", tail: %" PRIu64 ", tail_ver: %" PRIu64 "]", head, head_ver, tail, tail_ver);
        return buff;
    }


private:

    struct alignas(cache_line_size) Item
    {
        std::atomic<uint64_t> version;
        T value;
    };

    struct alignas(cache_line_size) AlignedAtomicU64 : public std::atomic<uint64_t>
    {
        using std::atomic<uint64_t>::atomic;
    };

    Item* m_items;
    size_t m_capacity;

    // Make sure each index is on a different cache line
    AlignedAtomicU64 m_head;
    AlignedAtomicU64 m_tail;
};


template <typename T> class QueueMutex
{
public:
    QueueMutex(size_t capacity) :
        m_capacity(capacity), m_head(0), m_tail(0),
        m_items(static_cast<T*>(malloc(sizeof(T)*capacity)))
    {
    }

    virtual ~QueueMutex()
    {
        if (m_items != nullptr) free(m_items);
    }

    bool is_full() { return (m_head == ((m_tail + 1) % m_capacity)); }
    bool is_empty() { return (m_head == m_tail); }
    size_t size() { return std::abs((int)m_tail - (int)m_head); }
    size_t capacity() { return m_capacity; }

    bool enqueue(const T& item)
    {
        std::lock_guard<std::mutex> guard(m_lock);
        if (is_full()) return false;
        m_items[m_tail] = item;
        m_tail = (m_tail + 1) % m_capacity;
        return true;
    }

    bool dequeue(T& item)
    {
        std::lock_guard<std::mutex> guard(m_lock);
        if (is_empty()) return false;
        item = m_items[m_head];
        m_head = (m_head + 1) % m_capacity;
        return true;
    }

    const char *c_str(char *buff, size_t size) const
    {
        if ((buff == nullptr) || (size < 32)) return EMPTY_STRING;

        sprintf(buff, "[head: %d, tail: %d]", (int)m_head, (int)m_tail);
        return buff;
    }

private:
    size_t m_head;
    size_t m_tail;
    size_t m_capacity;
    T* m_items;
    std::mutex m_lock;
};


}
