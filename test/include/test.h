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

#include <fstream>
#include <cstdarg>
#include <cstdlib>
#include "global.h"
#include "config.h"
#include "memmgr.h"
#include "query.h"
#include "tsdb.h"
#include "utils.h"


namespace tt_test
{


#define TEST_ROOT   "/tmp/tt_u/"
#define CONFIRM(X)  confirm((X), __FILE__, __LINE__)


class TestStats
{
public:
    TestStats() : m_passed(0), m_failed(0), m_total(0)
    {
    }

    void add(TestStats& stats)
    {
        m_passed += stats.m_passed;
        m_failed += stats.m_failed;
        m_total += stats.m_total;
        ASSERT((m_passed + m_failed) == m_total);
    }

    void add_passed(int passed)
    {
        ASSERT(passed > 0);
        m_passed += passed;
        m_total += passed;
    }

    void add_failed(int failed)
    {
        ASSERT(failed > 0);
        m_failed += failed;
        m_total += failed;
    }

    int get_passed() const { return m_passed; }
    int get_failed() const { return m_failed; }
    int get_total() const { return m_total; }

private:
    int m_passed;
    int m_failed;
    int m_total;
};


class TestCase
{
public:
    virtual void run() = 0;

    void confirm(bool exp, const char *file, int line)
    {
        if (! exp)
        {
            m_stats.add_failed(1);
            log("confirm() FAILED at line %d, file %s", line, file);
        }
    }

    void parse_data_points(char *json, tt::DataPointVector& dps)
    {
        if (json == nullptr) return;
        if (*json == '[')
        {
            tt::JsonArray array;
            tt::JsonParser::parse_array(json, array);
            //assert(array.size() == 1);
            for (int i = 0; i < array.size(); i++)
            {
                tt::JsonMap& map = array[i]->to_map();
                tt::JsonMap& dpsMap = map["dps"]->to_map();
                for (auto& it: dpsMap)
                {
                    dps.emplace_back(std::stoull(it.first), it.second->to_double());
                    //printf("%s: %f\n", it.first, it.second->to_double());
                }
            }
            tt::JsonParser::free_array(array);
        }
    }

    bool contains(tt::DataPointVector& dps, tt::DataPointPair& target)
    {
        for (auto& dp: dps)
        {
            if ((dp.first == target.first) && (std::abs(dp.second - target.second) <= 0.00000001))
                return true;
        }
        return false;
    }

    void flush_tsdb()
    {
        std::vector<tt::Tsdb*> tsdbs;
        tt::Tsdb::insts(tt::TimeRange::MAX, tsdbs);
        for (auto tsdb: tsdbs) tsdb->flush_for_test();
    }

    void query_raw(const char *metric, tt::Timestamp start, tt::DataPointVector& results)
    {
        query_with_downsample(metric, nullptr, start, results);
    }

    void query_with_downsample(
        const char *metric,
        const char *downsample,
        tt::Timestamp start,
        tt::DataPointVector& results)
    {
        tt::HttpRequest request;
        tt::HttpResponse response;
        char content[4096];
        if (downsample == nullptr)
            sprintf(content, "{\"start\":%" PRIu64 ",\"queries\":[{\"metric\":\"%s\"}]}", start, metric);
        else
            sprintf(content, "{\"start\":%" PRIu64 ",\"queries\":[{\"metric\":\"%s\",\"downsample\":\"%s\"}]}",
                start, metric, downsample);
        request.init();
        request.content = content;
        log("query request: %s", request.content);
        CONFIRM(tt::QueryExecutor::http_post_api_query_handler(request, response));
        char *body = std::strstr(response.response, "[");
        log("query response status: %d, size: %d", (int)response.status_code, response.response_size);
        parse_data_points(body, results);
    }

    void query_with_relative_ts(
        const char *metric,
        const char *start,
        tt::DataPointVector& results)
    {
        tt::HttpRequest request;
        tt::HttpResponse response;
        char content[4096];
        sprintf(content, "{\"start\":\"%s\",\"queries\":[{\"metric\":\"%s\"}]}", start, metric);
        request.init();
        request.content = content;
        log("query request: %s", request.content);
        CONFIRM(tt::QueryExecutor::http_post_api_query_handler(request, response));
        char *body = std::strstr(response.response, "[");
        log("query response status: %d, size: %d", (int)response.status_code, response.response_size);
        parse_data_points(body, results);
    }

    TestStats& get_stats() { return m_stats; }

    static void create_config(const char *key, const char *value)
    {
        std::ofstream stream(tt::g_config_file);
        stream << "ticktock.home = /tmp/tt_u" << std::endl;
        stream << "tsdb.page.size = 4096b" << std::endl;
        stream << key << " = " << value << std::endl;
        stream.close();
    }

    static void create_config(std::vector<std::pair<const char*, const char*> >& configs)
    {
        std::ofstream stream(tt::g_config_file);
        stream << "ticktock.home = /tmp/tt_u" << std::endl;
        stream << "tsdb.page.size = 4096b" << std::endl;
        for (auto& config : configs)
            stream << config.first << " = " << config.second << std::endl;
        stream.close();
    }

    static int rand_plus_minus(int from, int to)
    {
        int r = tt::random(from, to);
        if (tt::random(0,1) == 1) r = -r;
        return r;
    }

    static void generate_data_points(tt::DataPointVector& dps, int cnt, tt::Timestamp ts)
    {
        if (tt::g_tstamp_resolution_ms)
        {
            dps.emplace_back(ts+tt::random(0,5000), tt::random(0,100));
            for (int i = 1; i < cnt; i++)
                dps.emplace_back(dps[i-1].first+rand_plus_minus(0,5000)+30000, dps[i-1].second+rand_plus_minus(0,50));
        }
        else
        {
            dps.emplace_back(ts+tt::random(0,10), tt::random(0,100));
            for (int i = 1; i < cnt; i++)
                dps.emplace_back(dps[i-1].first+rand_plus_minus(0,10)+30, dps[i-1].second+rand_plus_minus(0,50));
        }
    }

    static void generate_data_points_float(tt::DataPointVector& dps, int cnt, tt::Timestamp ts)
    {
        if (tt::g_tstamp_resolution_ms)
        {
            dps.emplace_back(ts+tt::random(0,5000), (double)tt::random(0,100)/101.0);
            for (int i = 1; i < cnt; i++)
                dps.emplace_back(dps[i-1].first+rand_plus_minus(0,5000)+30000, dps[i-1].second+(double)rand_plus_minus(0,50)/101.0);
        }
        else
        {
            dps.emplace_back(ts+tt::random(0,10), tt::random(0,100));
            for (int i = 1; i < cnt; i++)
                dps.emplace_back(dps[i-1].first+rand_plus_minus(0,10)+30, dps[i-1].second+(double)rand_plus_minus(0,50)/101.0);
        }
    }

    static int gen_random_string(char *buff, int min, int max)
    {
        static const char alphanum[] =
            "_=; "
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        int len = tt::random(min, max);

        for (int i = 0; i < len; i++)
            buff[i] = alphanum[tt::random(0, sizeof(alphanum)-2)];
        buff[len] = 0;

        return len + 1;
    }

    static void cleanup_data_dir()
    {
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "exec rm -f -r %s/*", tt::Config::get_str(CFG_TSDB_DATA_DIR).c_str());
        if (tt::starts_with(cmd, "exec rm -f -r /tmp/"))
            system(cmd);
    }

    static void clean_start(bool rm_data)
    {
        if (rm_data) cleanup_data_dir();
        tt::MemoryManager::init();
        tt::Tsdb::init();
    }

    static void clean_shutdown()
    {
        tt::Tsdb::shutdown();
    }

    const char *get_name() const
    {
        return m_name;
    }

    void log(const char *format, ...)
    {
        va_list args;
        va_start(args, format);

        time_t sec;
        unsigned int msec;
        tt::ts_now(sec, msec);
        struct tm timeinfo;
        localtime_r(&sec, &timeinfo);
        char fmt[1024];
        std::strftime(fmt, sizeof(fmt), "%Y-%m-%d %H:%M:%S", &timeinfo);
        sprintf(fmt+std::strlen(fmt), ".%03d [%s] %s\n", msec, m_name, format);

        std::vprintf(fmt, args);
        va_end(args);
    }

    static char * str_join(const char *s1, const char *s2, const char *s3 = nullptr)
    {
        size_t len = std::strlen(s1) + std::strlen(s2);
        if (s3 != nullptr) len += std::strlen(s3);
        char *buff = (char*) malloc(len + 1);
        if (s3 == nullptr)
            sprintf(buff, "%s%s", s1, s2);
        else
            sprintf(buff, "%s%s%s", s1, s2, s3);
        return buff;
    }

protected:
    const char *m_name;
    TestStats m_stats;
};


}
