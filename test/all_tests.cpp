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

#include "global.h"
#include "bitset_test.h"
#include "compact_test.h"
#include "compress_test.h"
#include "cp_test.h"
#include "json_test.h"
#include "max_subset_test.h"
#include "misc_test.h"
#include "query_test.h"
#include "task_test.h"


using namespace tt_test;

// array of test cases to run
static TestCase *tests[] =
{
    new BitSetTests(),
    new CheckPointTests(),
    new CompactTests(),
    new CompressTests(),
    new HashTests(),
    new JsonTests(),
    //new MaxSubsetTests(),
    new MiscTests(),
    new QueryTests(),
    //new TaskTests()
};

int
main(int argc, char *argv[])
{
    if (argc > 1)
        std::srand(std::atoi(argv[1]));
    else
    {
        long seed = std::time(0);
        std::srand(seed);
        printf("rand() seed used: %ld\n", seed);
    }

    // update g_config_file to point to our test config
    tt::g_config_file = TestCase::str_join(TEST_ROOT, "test.conf");
    system(TestCase::str_join("mkdir -p ", TEST_ROOT, "data"));
    system("rm -f /tmp/*.cp");

    // generate our own config file
    TestCase::create_config(CFG_LOG_FILE, TestCase::str_join(TEST_ROOT, "test.log"));
    Config::init();
    Tsdb::init();
    QueryExecutor::init();

    TestStats stats;
    int test_cnt = sizeof(tests) / sizeof(tests[0]);

    for (int i = 0; i < test_cnt; i++)
    {
        try
        {
            tests[i]->run();
        }
        catch (...)
        {
            stats.add_failed(1);
            tests[i]->log("Test %s FAILED", tests[i]->get_name());
            continue;
        }

        stats.add(tests[i]->get_stats());
    }

    printf("PASSED: %d, FAILED: %d, TOTAL: %d\n", stats.get_passed(), stats.get_failed(), stats.get_total());
    return 0;
}
