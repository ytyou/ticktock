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
#include "config.h"
#include "fd.h"
#include "logger.h"
#include "meta.h"
#include "ts.h"


namespace tt
{


MetaFile * MetaFile::m_instance;    // Singleton instance


void
MetaFile::init(void (*restore_func)(std::string& metric, std::string& key, TimeSeriesId id))
{
    m_instance = new MetaFile();            // create the Singleton
    m_instance->restore(restore_func);      // restore from it
}

void
MetaFile::restore(void (*restore_func)(std::string& metric, std::string& key, TimeSeriesId id))
{
    char buff[PATH_MAX];
    snprintf(buff, sizeof(buff), "%s/ticktock.meta",
        Config::get_str(CFG_TSDB_DATA_DIR, CFG_TSDB_DATA_DIR_DEF).c_str());
    m_name.assign(buff);
    m_file = nullptr;

    std::ifstream is(m_name);

    if (is)
    {
        std::string line;

        while (std::getline(is, line))
        {
            std::vector<std::string> tokens;
            tokenize(line, tokens, ' ');

            if (tokens.size() != 3)
            {
                Logger::error("Bad line in %s: %s", m_name.c_str(), line.c_str());
                continue;
            }

            std::string metric = tokens[0]; // metric
            std::string tags = tokens[1];   // tags
            std::string id = tokens[2];     // TimeSeries id

            (*restore_func)(metric, tags, std::stoi(id));
        }
    }

    is.close();

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

    int fd = ::open(m_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_SYNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
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
MetaFile::add_ts(TimeSeries *ts)
{
    ASSERT(m_file != nullptr);
    std::lock_guard<std::mutex> guard(m_lock);
    fprintf(m_file, "%s %s %u\n", ts->get_metric(), ts->get_key(), ts->get_id());
}


}
