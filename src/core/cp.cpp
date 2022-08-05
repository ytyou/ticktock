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
#include <unistd.h>
#include "config.h"
#include "cp.h"
#include "fd.h"
#include "leak.h"
#include "logger.h"


namespace tt
{


std::mutex CheckPointManager::m_lock;
cps_map CheckPointManager::m_cps;
cps_map CheckPointManager::m_snapshot;
cps_map CheckPointManager::m_persisted;


void
CheckPointManager::init()
{
    // load from disk
    std::string pattern = Config::get_str(CFG_TSDB_DATA_DIR,CFG_TSDB_DATA_DIR_DEF) + "/*.cp";
    std::string file_name = last_file(pattern);
    if (file_name.empty()) return;
    std::ifstream is(file_name);

    if (! is)
    {
        Logger::warn("Failed to open cp file: %s", file_name.c_str());
        return;
    }

    Logger::debug("Loading check-points from %s", file_name.c_str());

    std::string line;

    while (std::getline(is, line))
    {
        char buff[1024];
        std::strncpy(buff, line.c_str(), sizeof(buff));
        CheckPointManager::add(buff);
    }

    is.close();
}

// format of cp: <leader>:<channel>:<check-point>
// no spaces allowed; max length of <check-point> is 30
bool
CheckPointManager::add(char *cp)
{
    ASSERT(cp != nullptr);
    std::vector<char*> tokens;
    tokenize(cp, ':', tokens);
    if (tokens.size() != 3)
    {
        Logger::warn("Bad check-point received and ignored");
        return false;
    }

    std::lock_guard<std::mutex> guard(m_lock);
    auto search1 = m_cps.find(tokens[0]);
    if (search1 == m_cps.end())
    {
        m_cps.insert(make_pair(STRDUP(tokens[0]), cp_map()));
        search1 = m_cps.find(tokens[0]);
    }
    ASSERT(search1 != m_cps.end());
    cp_map& map = search1->second;

    auto search2 = map.find(tokens[1]);
    if (search2 == map.end())
        map.insert(std::make_pair(STRDUP(tokens[1]), std::string(tokens[2])));
    else
        search2->second.assign(tokens[2]);

    return true;
}

void
CheckPointManager::take_snapshot()
{
    std::lock_guard<std::mutex> guard(m_lock);
    m_snapshot = m_cps;
}

/* Return format:
 *  [{"leader":"1","channels":[{"channel":"ch1","checkpoint":"cp1"},{"channel":"ch2","cp":"cp2"}]},{...}]
 */
int
CheckPointManager::get_persisted(const char *leader, char *buff, int size)
{
    ASSERT(size > 3);
    ASSERT(buff != nullptr);

    int idx = 1;
    buff[0] = '[';
    size -= 3;

    std::lock_guard<std::mutex> guard(m_lock);

    if (leader == nullptr)
    {
        for (auto it = m_persisted.begin(); it != m_persisted.end(); it++)
        {
            if (idx > 1)
            {
                buff[idx++] = ',';
                size--;
            }

            int len;
            len = get_persisted_of(it->first, it->second, &buff[idx], size);
            ASSERT(len < size);
            idx += len;
            size -= len;
            if (size <= 1) break;
        }
    }
    else
    {
        auto search = m_persisted.find(leader);
        if (search != m_persisted.end())
            idx += get_persisted_of(search->first, search->second, &buff[idx], size);
    }

    buff[idx++] = ']';
    buff[idx] = 0;

    return idx;
}

/* Return format:
 *  {"leader":"1","channels":[{"channel":"ch1","checkpoint":"cp1"},{"channel":"ch2","cp":"cp2"}]}
 */
int
CheckPointManager::get_persisted_of(const char *leader, cp_map& map, char *buff, int size)
{
    int len = 0;

    len = snprintf(buff, size, "{\"leader\":\"%s\",\"channels\":[", leader);
    size -= len;
    ASSERT(size >= 0);

    for (auto it = map.begin(); it != map.end(); it++)
    {
        if (size <= 1) break;
        int n = snprintf(&buff[len], size, "{\"channel\":\"%s\",\"checkpoint\":\"%s\"},",
            it->first, it->second.c_str());
        len += n;
        size -= n;
    }

    // remove last comma
    if (buff[len-1] == ',')
    {
        len--;
        size++;
    }

    len += snprintf(&buff[len], size, "]}");
    return len;
}

void
CheckPointManager::persist()
{
    std::lock_guard<std::mutex> guard(m_lock);

    // write m_snapshot to disk
    if (persist_to_file())
        m_persisted = m_snapshot;
}

// file name: <ts>.cp
bool
CheckPointManager::persist_to_file()
{
    if (m_snapshot.empty())
        return true;

    Timestamp ts = ts_now_sec();
    std::string file_name = Config::get_str(CFG_TSDB_DATA_DIR,CFG_TSDB_DATA_DIR_DEF) + "/" + std::to_string(ts) + ".cp";
    int fd = ::open(file_name.c_str(), O_CREAT|O_WRONLY|O_TRUNC|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

    fd = FileDescriptorManager::dup_fd(fd, FileDescriptorType::FD_FILE);
    if (fd == -1)
    {
        Logger::error("Failed to open file %s for write: %d", file_name.c_str(), errno);
        return false;
    }

    std::FILE *fp = fdopen(fd, "a");
    if (fp == nullptr)
    {
        Logger::error("Failed to convert fd %d to FILE: %d", fd, errno);
        ::close(fd);
        return false;
    }

    for (auto it1 = m_snapshot.begin(); it1 != m_snapshot.end(); it1++)
    {
        const char *leader = it1->first;
        cp_map& map = it1->second;

        for (auto it2 = map.begin(); it2 != map.end(); it2++)
            fprintf(fp, "%s:%s:%s\n", leader, it2->first, it2->second.c_str());
    }

    std::fflush(fp);
    std::fclose(fp);

    std::string pattern = Config::get_str(CFG_TSDB_DATA_DIR,CFG_TSDB_DATA_DIR_DEF) + "/*.cp";
    rotate_files(pattern, 10);
    return true;
}

void
CheckPointManager::close()
{
    take_snapshot();
    persist();
}


}
