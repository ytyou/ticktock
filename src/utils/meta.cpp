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
#include <sys/stat.h>
#include "logger.h"
#include "fd.h"
#include "meta.h"
#include "tsdb.h"
#include "utils.h"


namespace tt
{


class PageInfo;
class TimeSeries;
class Tsdb;


MetaFile::MetaFile(const std::string& file_name) :
    m_name(file_name),
    m_file(nullptr)
{
}

MetaFile::~MetaFile()
{
    close();
}

void
MetaFile::open()
{
    ASSERT(m_file == nullptr);

    std::lock_guard<std::mutex> guard(m_lock);
    int fd = ::open(m_name.c_str(), O_CREAT|O_WRONLY|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);

    if (fd == -1)
    {
        Logger::error("Failed to open file %s for append: %d", m_name.c_str(), errno);
    }
    else
    {
        m_file = fdopen(fd, "a");

        if (m_file == nullptr)
        {
            Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
        }
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
    {
        std::fflush(m_file);
    }
}

// Empty the file and start over. Used during compaction.
void
MetaFile::reset()
{
    close();
    truncate(m_name.c_str(), 0);
    open();
}

void
MetaFile::append(TimeSeries *ts, PageInfo *info)
{
    ASSERT(ts != nullptr);
    ASSERT(info != nullptr);
    ASSERT(m_file != nullptr);

    std::lock_guard<std::mutex> guard(m_lock);
    fprintf(m_file, "%s %s %u %u\n", ts->get_metric(), ts->get_key(), info->get_file_id(), info->get_id());
}

void
MetaFile::append(TimeSeries *ts, unsigned int file_id, unsigned int from_id, unsigned int to_id)
{
    ASSERT(ts != nullptr);
    ASSERT(m_file != nullptr);
    ASSERT(from_id <= to_id);

    std::lock_guard<std::mutex> guard(m_lock);

    if (from_id == to_id)
        fprintf(m_file, "%s %s %u %u\n", ts->get_metric(), ts->get_key(), file_id, to_id);
    else
        fprintf(m_file, "%s %s %u %u-%u\n", ts->get_metric(), ts->get_key(), file_id, from_id, to_id);
}

void
MetaFile::load(Tsdb *tsdb)
{
    ASSERT(tsdb != nullptr);

    std::lock_guard<std::mutex> guard(m_lock);
    std::ifstream is(m_name);

    if (! is)
    {
        Logger::debug("failed to open meta data file: %s", m_name.c_str());
        return;
    }

    Logger::trace("loading tsdb meta data from %s", m_name.c_str());

    std::string line;

    while (std::getline(is, line))
    {
        std::vector<std::string> tokens;
        tokenize(line, tokens, ' ');

        if (tokens.size() != 4)
        {
            Logger::error("Bad line in %s: %s", m_name.c_str(), line.c_str());
            continue;
        }

        std::string metric = tokens[0]; // metric
        std::string tags = tokens[1];   // tags
        std::string id = tokens[2];     // file id
        std::string index = tokens[3];  // page index

        if (index.find('-') == std::string::npos)
        {
            Logger::trace("restoring ts for metric %s, tags %s, id %s, index %s",
                metric.c_str(), tags.c_str(), id.c_str(), index.c_str());
            tsdb->add_ts(metric, tags, std::atoi(id.c_str()), std::atoi(index.c_str()));
        }
        else    // from-to
        {
            std::tuple<std::string,std::string> ft;
            tokenize(index, ft, '-');
            int from = std::atoi(std::get<0>(ft).c_str());
            int to = std::atoi(std::get<1>(ft).c_str());
            int fid = std::atoi(id.c_str());

            for (int i = from; i <= to; i++)
            {
                Logger::trace("restoring ts for metric %s, tags %s, id %s, index %d",
                    metric.c_str(), tags.c_str(), id.c_str(), i);
                tsdb->add_ts(metric, tags, fid, i);
            }
        }
    }

    is.close();
}


}
