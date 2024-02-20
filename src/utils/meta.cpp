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

#include <fcntl.h>
#include <fstream>
#include <limits.h>
#include <sys/stat.h>
#include "append.h"
#include "config.h"
#include "fd.h"
#include "limit.h"
#include "logger.h"
#include "memmgr.h"
#include "meta.h"
#include "ts.h"
#include "type.h"
#include "utils.h"


namespace tt
{


MetaFile * MetaFile::m_instance;    // Singleton instance


void
MetaFile::init(void (*restore_metrics)(MetricId, std::string& name), TimeSeries* (*restore_ts)(std::string& metric, std::string& key, TimeSeriesId id),
               void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv))
{
    m_instance = new MetaFile();            // create the Singleton
    m_instance->restore_metrics(restore_metrics);
    m_instance->restore_ts(restore_ts, restore_measurement);

    m_instance->open(); // open for append

    if (! m_instance->is_open())
    {
        Logger::fatal("Failed to open meta file for writing");
        throw new std::runtime_error("Failed to open meta file for writing");
    }
}

void
MetaFile::restore_metrics(void (*restore_metrics)(MetricId, std::string& name))
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/metrics", Config::get_data_dir().c_str());
    m_metrics_name.assign(buff);
    m_metrics_file = nullptr;

    //bool restore_needed = AppendLog::restore_needed();
    std::vector<TimeSeries*> tsv;
    std::ifstream is(m_metrics_name);

    tsv.reserve(4096);

    if (is)
    {
        std::string line;   // format: <id> <metric-name>

        // skip 1st line: ticktockdb.version
        std::getline(is, line);

        while (std::getline(is, line))
        {
            std::vector<TimeSeries*> tsv2;
            std::vector<std::string> tokens;
            tokenize(line, tokens, ' ');

            if (tokens.size() != 2)
            {
                Logger::error("Bad line in %s: %s", m_metrics_name.c_str(), line.c_str());
                continue;
            }

            (*restore_metrics)(std::stoul(tokens[0]), tokens[1]);
        }
    }

    is.close();
}

void
MetaFile::restore_ts(TimeSeries* (*restore_ts)(std::string& metric, std::string& key, TimeSeriesId id),
                  void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv))
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/ts", Config::get_data_dir().c_str());
    m_ts_name.assign(buff);
    m_ts_file = nullptr;

    bool restore_needed = AppendLog::restore_needed();
    std::vector<TimeSeries*> tsv;
    std::ifstream is(m_ts_name);

    tsv.reserve(4096);

    if (is)
    {
        std::string line;

        // skip 1st line: ticktockdb.version
        std::getline(is, line);

        while (std::getline(is, line))
        {
            std::vector<TimeSeries*> tsv2;
            std::vector<std::string> tokens;
            tokenize(line, tokens, ' ');

            if (tokens.size() < 3)
            {
                Logger::error("Bad line in %s: %s", m_ts_name.c_str(), line.c_str());
                continue;
            }

            if ((tokens.size() == 3) && (tokens[2].find_first_of('=') == std::string::npos))
            {
                // Updated OpenTSDB format: metric tag1=val1,tag2=val2 id
                std::string metric = tokens[0]; // metric
                std::string tags = tokens[1];   // tags
                TimeSeriesId id = std::stoi(tokens[2]);

                TimeSeries *ts = (*restore_ts)(metric, tags, id);
                tsv2.push_back(ts);
            }
            else
            {
                // InfluxDB format: measurement tag1=val1,tag2=val2 field1=id1 field2=id2
                std::string measurement = tokens[0]; // metric
                std::string tags = tokens[1];   // tags
                std::vector<std::pair<std::string,TimeSeriesId>> fields;

                for (int i = 2; i < tokens.size(); i++)
                {
                    std::tuple<std::string,std::string> kv;
                    if (tokenize(tokens[i], kv, '='))
                        fields.emplace_back(std::get<0>(kv), std::stoi(std::get<1>(kv)));
                }

                (*restore_measurement)(measurement, tags, fields, tsv2);
            }

            if (restore_needed)
            {
                // TODO: optimize this
                for (auto ts: tsv2)
                {
                    // collect all ts into tsv[] such that tsv[ts->get_id()] == ts;
                    TimeSeriesId id = ts->get_id();
                    while (tsv.size() <= id) tsv.push_back(nullptr);
                    tsv[id] = ts;
                }
            }
        }
    }

    is.close();

    if (! tsv.empty())
        AppendLog::restore(tsv);
}

void
MetaFile::open()
{
    ASSERT(m_ts_file == nullptr);
    ASSERT(m_metrics_file == nullptr);

    // open ts file
    bool is_new = ! file_exists(m_ts_name);
    int fd = ::open(m_ts_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

    if (fd == -1)
    {
        Logger::error("Failed to open file %s for append: %d", m_ts_name.c_str(), errno);
    }
    else
    {
        m_ts_file = fdopen(fd, "a");
        if (m_ts_file == nullptr)
            Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
        else if (is_new)    // 1st line: ticktock version
            fprintf(m_ts_file, "# ticktockdb.%d.%d.%d\n", TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION);
    }

    // open metrics file
    is_new = ! file_exists(m_metrics_name);
    fd = ::open(m_metrics_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

    if (fd == -1)
    {
        Logger::error("Failed to open file %s for append: %d", m_metrics_name.c_str(), errno);
    }
    else
    {
        m_metrics_file = fdopen(fd, "a");
        if (m_metrics_file == nullptr)
            Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
        else if (is_new)    // 1st line: ticktock version
            fprintf(m_metrics_file, "# ticktockdb.%d.%d.%d\n", TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION);
    }
}

void
MetaFile::close()
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_ts_file != nullptr)
    {
        std::fflush(m_ts_file);
        std::fclose(m_ts_file);
        m_ts_file = nullptr;
    }

    if (m_metrics_file != nullptr)
    {
        std::fflush(m_metrics_file);
        std::fclose(m_metrics_file);
        m_metrics_file = nullptr;
    }
}

void
MetaFile::flush()
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_ts_file != nullptr)
        std::fflush(m_ts_file);

    if (m_metrics_file != nullptr)
        std::fflush(m_metrics_file);
}

void
MetaFile::add_ts(const char *metric, const char *key, TimeSeriesId id)
{
    ASSERT(metric != nullptr);
    ASSERT(key != nullptr);
    ASSERT(id != TT_INVALID_TIME_SERIES_ID);
    ASSERT(m_ts_file != nullptr);

    std::lock_guard<std::mutex> guard(m_lock);
    fprintf(m_ts_file, "%s %s %u\n", metric, key, id);
}

void
MetaFile::add_metric(MetricId id, const char *name)
{
    ASSERT(name != nullptr);
    ASSERT(m_metrics_file != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);
    fprintf(m_metrics_file, "%d %s\n", id, name);
}

void
MetaFile::add_ts(const char *metric, Tag_v2& tags_v2, TimeSeriesId id)
{
    Tag *tags = tags_v2.get_ordered_v1_tags();
    Tag *field = KeyValuePair::remove_first(&tags, TT_FIELD_TAG_NAME);
    char buff[MAX_TOTAL_TAG_LENGTH];
    int n = 0, size = sizeof(buff);
    Tag *tag = tags;

    while ((tag != nullptr) && (size > n))
    {
        n += std::snprintf(buff+n, size-n, ",%s=%s", tag->m_key, tag->m_value);
        tag = tag->next();
    }

    buff[size-1] = 0;

    {
        std::lock_guard<std::mutex> guard(m_lock);

        if (field == nullptr)
            fprintf(m_ts_file, "%s %s %u\n", metric, (n>0) ? &buff[1] : ";", id);
        else
            fprintf(m_ts_file, "%s %s %s=%u\n", metric, (n>0) ? &buff[1] : ";", field->m_value, id);
    }

    if (field != nullptr)
        MemoryManager::free_recyclable(field);
    KeyValuePair::free_list(tags);
}

void
MetaFile::add_measurement(const char *measurement, char *tags, std::vector<std::pair<const char*,TimeSeriesId>>& fields)
{
    ASSERT(measurement != nullptr);
    ASSERT(tags != nullptr);
    ASSERT(m_ts_file != nullptr);

    //char buff[MAX_TOTAL_TAG_LENGTH];
    char *buff = MemoryManager::alloc_network_buffer();
    uint64_t size = MemoryManager::get_network_buffer_size();
    int n = snprintf(buff, size, "%s %s", measurement, tags);

    if (UNLIKELY(n >= size))
    {
        Logger::error("tags too long: %s,%s", measurement, tags);
        MemoryManager::free_network_buffer(buff);
        return;
    }

    for (auto field: fields)
    {
        n += snprintf(&buff[n], size-n, " %s=%u", field.first, field.second);

        if (UNLIKELY(n >= size))
        {
            Logger::error("tags too long: %s,%s", measurement, tags);
            MemoryManager::free_network_buffer(buff);
            return;
        }
    }

    buff[n] = 0;

    {
        std::lock_guard<std::mutex> guard(m_lock);
        fprintf(m_ts_file, "%s\n", buff);
    }

    MemoryManager::free_network_buffer(buff);
}


}
