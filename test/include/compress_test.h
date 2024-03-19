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

#include "compress.h"
#include "test.h"


using namespace tt;

namespace tt_test
{


class CompressTests : public TestCase
{
public:
    CompressTests() { m_name = "compress_tests"; }
    void run();

private:
    void run_with(bool ms);
    void compress_uncompress(Compressor *compressor, Timestamp ts, bool best, bool floating);
    void save_restore(Compressor *compressor, Timestamp ts);
    void save_restore2(Compressor *compressor, Timestamp ts);
    void stress_test(Compressor *compressor, Timestamp ts);
    void best_scenario(bool ms);
    void rollup_compress1();
    void rollup_compress2();
};


}
