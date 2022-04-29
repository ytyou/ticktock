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

#include <atomic>
#include <cassert>
#include <cstdio>
#include <queue>
#include <regex>
#include <functional>
#include <cstring>
#include "global.h"
#include "type.h"


namespace tt
{


#ifdef _DEBUG
#define ASSERT(_X)   assert(_X)
#else
#define ASSERT(_X)
#endif

#define NONE_NULL_STR(_X)       (((_X) == nullptr) ? EMPTY_STRING : (_X))

#define MAX_SEC_SINCE_EPOCH     (100000000000L)

// 1024 x 1024
#define ONE_MEGABYTES           (1048576L)


extern const int SPIN_YIELD_THRESHOLD;

extern void segv_handler(int sig);

class JsonValue;
enum JsonValueType : unsigned char;

extern int random(int from, int to);
extern double random(double from, double to);
extern Timestamp ts_now();  // return ts in either ms or sec, depending on g_tstamp_resolution_ms
extern Timestamp ts_now_ms();
extern Timestamp ts_now_sec();
extern void ts_now(char *buff, const size_t size);
extern void ts_now(time_t& sec, unsigned int& msec);
extern bool is_ms(Timestamp tstamp);
extern bool is_sec(Timestamp tstamp);
extern bool is_off_hour();
extern bool ts_resolution_ms();     // timestamp resolution is millisecond?
extern bool is_my_ip(std::string& ip);
extern Timestamp validate_resolution(Timestamp ts); // return timestamp with correct unit
extern Timestamp to_ms(Timestamp tstamp);
extern Timestamp to_sec(Timestamp tstamp);
extern Timestamp parse_ts(const JsonValue *value, Timestamp now);
extern TimeUnit to_time_unit(const char *str, size_t len);
extern Timestamp convert_time(Timestamp time, TimeUnit from_unit, TimeUnit to_unit);
extern uint64_t get_bytes_factor(const std::string& str);
extern void tokenize(const std::string& str, std::vector<std::string>& tokens, std::regex& delim);
extern bool tokenize(const std::string& str, std::tuple<std::string,std::string>& kv, char delim);
extern bool tokenize(const std::string& str, std::vector<std::string>& tokens, char delim);
extern bool tokenize(char* str, char* &key, char* &value, char delim);
extern bool tokenize(char* str, char delim, std::vector<char*>& tokens);
extern bool tokenize(char* str, char *delim, std::vector<char*>& tokens);
extern std::string trim(const std::string& str);
extern std::string ltrim(const std::string& str);
extern std::string rtrim(const std::string& str);
extern bool starts_with(const std::string& str, char ch);
extern bool starts_with(const char *str, const char *prefix);
extern bool ends_with(const char *str, const char tail);
extern bool ends_with(const std::string& str, const std::string& tail);
extern int replace_all(std::string& str, const std::string& from, const std::string& to);
extern void spin_yield(unsigned int k);
extern bool operator<(const DataPointPair& lhs, const DataPointPair& rhs);
extern bool dp_pair_less(const DataPointPair& lhs, const DataPointPair& rhs);
extern int max_subset_4k(int16_t set[], size_t size, std::vector<int>& subset); // 'subset' is the output

extern bool is_aligned(uintptr_t ptr, unsigned long align);
extern bool is_power_of_2(uint64_t n);
extern uint64_t next_power_of_2(uint64_t n);

extern bool file_exists(const std::string& full_path);
extern int rm_file(const std::string& full_path);
extern int rm_all_files(const std::string& pattern);
extern int rotate_files(const std::string& pattern, int retain_count);
extern uint64_t get_disk_block_size(const std::string& full_path);
extern uint64_t get_disk_available_blocks(const std::string& full_path);

extern bool url_unescape(const char *url, char *buff, size_t len);

extern void print_double_in_hex(double n);
extern void print_uint16_t_in_hex(uint16_t n);
extern void print_uint32_t_in_hex(uint32_t n);
extern void print_uint64_t_in_hex(uint64_t n);

struct cstr_less : public std::binary_function<const char*, const char*, bool>
{
    inline bool operator() (const char* str1, const char* str2) const
    {
        return std::strcmp(str1, str2) < 0;
    }
};

struct hash_func
{
    std::size_t operator()(const char* str) const noexcept
    {
        std::size_t hash = 5381;
        int c;

        while (c = *str++)
        {
            hash = ((hash << 5) + hash) ^ c;    // hash * 33 ^ c
        }

        return hash;
    }
};

struct eq_func
{
    bool operator()(const char* str1, const char* str2) const
    {
        return (strcmp(str1, str2) == 0);
    }
};

// merge N sorted std::vector<class Data>'s,
// // as std::vector<std::vector<class Data>>,
// // into one sorted std::vector<class Data>;
// // class Data must implement operator<
template<class Data>
void merge(std::vector<std::vector<Data>>& ins, std::vector<Data>& outs)
{   
    using it_t = typename std::vector<Data>::iterator;
    using it_pair_t = std::pair<it_t,it_t>;
    auto it_pair_cmp = [](const it_pair_t &lhs, const it_pair_t &rhs) { return !(*lhs.first < *rhs.first); };
    std::priority_queue<it_pair_t, std::vector<it_pair_t>, decltype(it_pair_cmp)> pq(it_pair_cmp);

    for (std::vector<Data>& v: ins)
    {
        it_pair_t p = std::make_pair(v.begin(), v.end());
        if (p.first != p.second) pq.push(p); 
    }

    while (! pq.empty())
    {       
        auto top = pq.top();
        outs.push_back(*top.first);
        pq.pop();

        if (++top.first != top.second) pq.push(top);
    }   
}


template<class Elem>
class DynamicArray2D
{
public:
    DynamicArray2D(size_t rows, size_t cols) :
        m_rows(rows),
        m_cols(cols)
    {
        m_array = new Elem[m_rows * m_cols];
    }

    ~DynamicArray2D()
    {
        delete[] m_array;
    }

    Elem& elem(size_t i, size_t j)
    {
        ASSERT((i < m_rows) && (j < m_cols));
        return m_array[i * m_cols + j];
    }

private:
    size_t m_rows, m_cols;
    Elem *m_array;
};


}
