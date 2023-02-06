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
MetaFile::init(TimeSeries* (*restore_ts)(std::string& metric, std::string& key, TimeSeriesId id),
               void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv))
{
    m_instance = new MetaFile();            // create the Singleton
    m_instance->restore(restore_ts, restore_measurement);
}

void
MetaFile::restore(TimeSeries* (*restore_ts)(std::string& metric, std::string& key, TimeSeriesId id),
                  void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv))
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/ticktock.meta", Config::get_data_dir().c_str());
    m_name.assign(buff);
    m_file = nullptr;

    bool restore_needed = AppendLog::restore_needed();
    std::vector<TimeSeries*> tsv;
    std::ifstream is(m_name);

    tsv.reserve(4096);

    if (is)
    {
        std::string line;

        while (std::getline(is, line))
        {
            std::vector<TimeSeries*> tsv2;
            std::vector<std::string> tokens;
            tokenize(line, tokens, ' ');

            if (tokens.size() < 3)
            {
                Logger::error("Bad line in %s: %s", m_name.c_str(), line.c_str());
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

    open(); // open for append

    if (! is_open())
    {
        Logger::fatal("Failed to open meta file %s for writing", m_name.c_str());
        throw new std::runtime_error("Failed to open meta file for writing");
    }
}

void
MetaFile::open()
{
    ASSERT(m_file == nullptr);

    int fd = ::open(m_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

    if (fd == -1)
    {
        Logger::error("Failed to open file %s for append: %d", m_name.c_str(), errno);
    }
    else
    {
        m_file = fdopen(fd, "a");
        if (m_file == nullptr)
            Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
    }
}

void
MetaFile::close()
{
    std::lock_guard<std::mutex> guard(m_lock);

    if (m_file != nullptr)
    {
        std::fflush(m_file);
        std::fclose(m_file);
        m_file = nullptr;
    }
}

void
MetaFile::flush()
{
    std::lock_guard<std::mutex> guard(m_lock);
    if (m_file != nullptr)
        std::fflush(m_file);
}

void
MetaFile::add_ts(const char *metric, const char *key, TimeSeriesId id)
{
    ASSERT(metric != nullptr);
    ASSERT(key != nullptr);
    ASSERT(id != TT_INVALID_TIME_SERIES_ID);
    ASSERT(m_file != nullptr);

    std::lock_guard<std::mutex> guard(m_lock);
    fprintf(m_file, "%s %s %u\n", metric, key, id);
}

void
MetaFile::add_measurement(const char *measurement, char *tags, std::vector<std::pair<const char*,TimeSeriesId>>& fields)
{
    ASSERT(measurement != nullptr);
    ASSERT(tags != nullptr);
    ASSERT(m_file != nullptr);

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
        fprintf(m_file, "%s\n", buff);
    }

    MemoryManager::free_network_buffer(buff);
}


}
