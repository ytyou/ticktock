/*
    TickTockDB is an open-source Time Series Database, maintained by
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

#include "utils.h"
#include "agg_test.h"


using namespace tt;

namespace tt_test
{


void
AggregateTests::run()
{
    log("Running aggregate tests...");
    percentile("p50", 13.439454, 0.000000);
    percentile("p75", 26.878908, 0.000000);
    percentile("p90", 26.878908, 0.000000);
    percentile("p95", 26.878908, 0.000000);
    percentile("p99", 26.878908, 0.000000);
    percentile("p999", 26.878908, 0.000000);
}

void
AggregateTests::percentile(const char *pct, double v1, double v2)
{
    log("Running %s(%s)...", m_name, pct);

    QueryTask task1, task2;
    QueryResults results;

    DataPointVector& dpv1 = task1.get_dps();
    DataPointVector& dpv2 = task2.get_dps();

    dpv1.emplace_back(1569859300000, 26.878908);
    dpv1.emplace_back(1569859310000, 0.000000);

    dpv2.emplace_back(1569859300000, 0.000000);
    dpv2.emplace_back(1569859310000, 0.000000);

    results.get_query_tasks().push_back(&task1);
    results.get_query_tasks().push_back(&task2);

    Aggregator *aggregator = Aggregator::create(pct);
    aggregator->aggregate(&results);
    delete aggregator;

    CONFIRM(results.get_dps().size() == 2);
    CONFIRM(results.get_dps()[0].first == 1569859300000);
    CONFIRM(results.get_dps()[0].second == v1);
    CONFIRM(results.get_dps()[1].first == 1569859310000);
    CONFIRM(results.get_dps()[1].second == v2);

    for (DataPointPair& dp: results.get_dps())
        printf("[%lu, %f]", dp.first, dp.second);
    printf("\n");

    log("Finished %s", m_name);
}


}
