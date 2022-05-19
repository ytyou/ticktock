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

#include "cp.h"
#include "cp_test.h"


using namespace tt;

namespace tt_test
{


void
CheckPointTests::run()
{
    get_persisted_tests();
}

void
CheckPointTests::get_persisted_tests()
{
    log("Running %s...", m_name);

    char buff[1024];
    int len;

    // initially there's no cp
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(len == 2);
    CONFIRM(std::strcmp(buff, "[]") == 0);
    log("persisted = %s", buff);

    // add a checkpoint
    std::strcpy(buff, "leader1:channel1:checkpoint1");
    CheckPointManager::add(buff);
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(len == 2);
    CONFIRM(std::strcmp(buff, "[]") == 0);

    // now take a snapshot
    CheckPointManager::take_snapshot();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(len == 2);
    CONFIRM(std::strcmp(buff, "[]") == 0);

    // now persist the snapshot
    CheckPointManager::persist();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);
    log("persisted = %s", buff);

    // add another checkpoint
    std::strcpy(buff, "leader1:channel2:checkpoint2");
    CheckPointManager::add(buff);
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);

    // now take another snapshot
    CheckPointManager::take_snapshot();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);

    // now persist the snapshot
    CheckPointManager::persist();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);
    log("persisted = %s", buff);

    // override the first checkpoint
    std::strcpy(buff, "leader1:channel1:checkpoint3");
    CheckPointManager::add(buff);
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);

    // now take another snapshot
    CheckPointManager::take_snapshot();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]}]") == 0);

    // now persist the snapshot
    CheckPointManager::persist();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint3\"}]}]") == 0);
    log("persisted = %s", buff);

    // override the second checkpoint
    std::strcpy(buff, "leader1:channel2:checkpoint4");
    CheckPointManager::add(buff);
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint3\"}]}]") == 0);

    // now take another snapshot
    CheckPointManager::take_snapshot();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint2\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint3\"}]}]") == 0);

    // now persist the snapshot
    CheckPointManager::persist();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint4\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint3\"}]}]") == 0);
    log("persisted = %s", buff);

    // add another leader
    std::strcpy(buff, "leader2:channel1:checkpoint1");
    CheckPointManager::add(buff);
    CheckPointManager::take_snapshot();
    CheckPointManager::persist();
    len = CheckPointManager::get_persisted(nullptr, buff, sizeof(buff));
    CONFIRM(std::strcmp(buff, "[{\"leader\":\"leader2\",\"channels\":[{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint1\"}]},{\"leader\":\"leader1\",\"channels\":[{\"channel\":\"channel2\",\"checkpoint\":\"checkpoint4\"},{\"channel\":\"channel1\",\"checkpoint\":\"checkpoint3\"}]}]") == 0);
    log("persisted = %s", buff);

    m_stats.add_passed(1);
}


}
