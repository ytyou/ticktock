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

#include <mutex>


namespace tt
{


class TimeSeries;


/* This is a singleton.
 */
class MetaFile
{
public:
    static void init(void (*restore_metrics)(MetricId, std::string& name), TimeSeries* (*restore_func)(std::string& metric, std::string& key, TimeSeriesId id),
                     void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv));
    static MetaFile *instance() { return m_instance; }

    void open();    // for append
    void close();
    void flush();

    inline bool is_open() const { return (m_ts_file != nullptr); }
    void add_metric(MetricId id, const char *name);
    void add_ts(const char *metric, const char *key, TimeSeriesId id);
    void add_ts(const char *metric, Tag_v2& tags, TimeSeriesId id);
    void add_measurement(const char *measurement, char *tags, std::vector<std::pair<const char*,TimeSeriesId>>& fields);

private:
    void restore_ts(TimeSeries* (*restore_func)(std::string& metric, std::string& key, TimeSeriesId id),
                 void (*restore_measurement)(std::string& measurement, std::string& tags, std::vector<std::pair<std::string,TimeSeriesId>>& fields, std::vector<TimeSeries*>& tsv));
    void restore_metrics(void (*restore_metrics)(MetricId, std::string& name));

    std::mutex m_lock;
    std::string m_ts_name;
    std::string m_metrics_name;
    std::FILE *m_ts_file;
    std::FILE *m_metrics_file;

    static MetaFile *m_instance;
};


}
