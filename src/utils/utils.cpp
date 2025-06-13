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

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <dirent.h>
#include <fstream>
#include <glob.h>
#include <string>
#include <cstring>
#include <regex>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <arpa/inet.h>
#include <execinfo.h>
#include <thread>
#include <tuple>
#include <vector>
#include <cctype>
#include "config.h"
#include "global.h"
#include "limit.h"
#include "logger.h"
#include "stats.h"
#include "utils.h"


namespace tt
{


const int SPIN_YIELD_THRESHOLD = 10;


void
segv_handler(int sig)
{
    void *array[100];
    size_t size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 100);

    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d:\n", sig);
    backtrace_symbols_fd(array, size, STDERR_FILENO);

    exit(1);
}

int
random(int from, int to)
{
    ASSERT(0 <= from);
    ASSERT(from <= to);
    ASSERT(to <= RAND_MAX);

    if (from == to) return to;

    int n = std::rand();    // 0 <= n <= RAND_MAX
    int cnt = to - from + 1;

    return (n % cnt) + from;
}

double
random(double from, double to)
{
    ASSERT(from <= to);

    if (from == to) return to;
    int n = std::rand();    // 0 <= n <= RAND_MAX
    return ((double)n / (double)RAND_MAX) * (to - from) + from;
}

// TODO: this is NOT WORKING!!!
Timestamp
ts_now_ms()
{
    using namespace std::chrono;
    system_clock::time_point now = system_clock::now();
    return duration_cast<milliseconds>(now.time_since_epoch()).count();
}

Timestamp
ts_now_sec()
{
    return std::time(0);
}

void
ts_now(time_t& sec, unsigned int& msec)
{
    using namespace std::chrono;
    system_clock::time_point now = system_clock::now();
    sec = system_clock::to_time_t(now);

    milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());
    msec = ms.count() % 1000;
}

Timestamp
ts_now()
{
    return (g_tstamp_resolution_ms ? ts_now_ms() : ts_now_sec());
}

// generate current time: 2020-06-08 17:59:23.456;
// the buff passed in must be at least 24 bytes long;
// this function does not allocate memory;
void
ts_now(char *buff, const size_t size)
{
    if ((buff == nullptr) || (size < 24)) return;

    time_t sec;
    unsigned int msec;

    ts_now(sec, msec);

    struct tm timeinfo;
    localtime_r(&sec, &timeinfo);
    std::strftime(buff, size, "%Y-%m-%d %H:%M:%S", &timeinfo);
    sprintf(buff+std::strlen(buff), ".%03d", msec);    // add the fraction of sec part
}

Timestamp
step_down(Timestamp ts, Timestamp interval)
{
    return ts - (ts % interval);
}

/* return beginning of month time in UTC
 *
 * @param year Year minus 1900
 * @param month Month [0, 11] (January = 0)
 * @return Unix time of the beginning of the specified month
 */
std::time_t
begin_month(int year, int month)
{
    struct tm timeinfo;

    std::memset(&timeinfo, 0, sizeof(timeinfo));
    timeinfo.tm_mday = 1;
    timeinfo.tm_mon = month;
    timeinfo.tm_year = year;

    return timegm(&timeinfo);
}

/* return beginning of month time in UTC
 *
 * @param ts Current time
 * @return Beginning of the month of given ts, in seconds
 */
std::time_t
begin_month(std::time_t ts)
{
    ASSERT(is_sec(ts));
    struct tm timeinfo;
    gmtime_r(&ts, &timeinfo);
    return begin_month(timeinfo.tm_year, timeinfo.tm_mon);
}

/* return beginning of month time in UTC
 *
 * @param ts Current time
 * @return Beginning of next month of given ts, in seconds
 */
std::time_t
end_month(std::time_t ts)
{
    ASSERT(is_sec(ts));
    struct tm timeinfo;
    gmtime_r(&ts, &timeinfo);
    int month = timeinfo.tm_mon + 1;    // next month
    int year = timeinfo.tm_year;
    if (month > 11)
    {
        month = 0;
        year++;
    }
    return begin_month(year, month);
}

/* return beginning of year time in UTC
 *
 * @param ts Current time
 * @return Beginning of the year of given ts, in seconds
 */
std::time_t
begin_year(time_t ts)
{
    ASSERT(is_sec(ts));
    struct tm timeinfo;
    gmtime_r(&ts, &timeinfo);
    return begin_month(timeinfo.tm_year, 0);
}

/* @param ts Number of seconds since the Epoch
 */
void
get_year_month(std::time_t ts, int& year, int& month)
{
    struct tm timeinfo;
    gmtime_r(&ts, &timeinfo);
    month = timeinfo.tm_mon + 1;
    year = timeinfo.tm_year + 1900;
}

/* @param ts Number of seconds since the Epoch
 * @param begin Returns beginning of the day, in secs
 * @param end Returns beginning of the next day, in secs
 */
void
get_day_range(std::time_t ts, std::time_t& begin, std::time_t& end)
{
    ASSERT(is_sec(ts));

    struct tm timeinfo;
    gmtime_r(&ts, &timeinfo);

    timeinfo.tm_sec = 0;
    timeinfo.tm_min = 0;
    timeinfo.tm_hour = 0;

    begin = timegm(&timeinfo);
    ts = begin + 86460;         // this should fall into next day

    gmtime_r(&ts, &timeinfo);

    timeinfo.tm_sec = 0;
    timeinfo.tm_min = 0;
    timeinfo.tm_hour = 0;
    end = timegm(&timeinfo);    // beginning of next day
}

bool
is_off_hour()
{
    std::time_t sec = std::time(nullptr);
    struct tm timeinfo;
    struct tm *now = localtime_r(&sec, &timeinfo);
    int off_hour_begin = Config::inst()->get_int(CFG_TSDB_OFF_HOUR_BEGIN, CFG_TSDB_OFF_HOUR_BEGIN_DEF);
    int off_hour_end = Config::inst()->get_int(CFG_TSDB_OFF_HOUR_END, CFG_TSDB_OFF_HOUR_END_DEF);

    if (off_hour_begin == off_hour_end)
    {
        return true;
    }
    else if (off_hour_begin < off_hour_end)
    {
        return (off_hour_begin <= now->tm_hour) && (now->tm_hour <= off_hour_end);
    }
    else    // off_hour_begin > off_hour_end
    {
        return (off_hour_begin <= now->tm_hour) || (now->tm_hour <= off_hour_end);
    }
}

bool
is_my_ip(std::string& ip)
{
    if (ip == "127.0.0.1") return true;

    struct addrinfo hints, *ap;
    struct addrinfo *result = nullptr;

    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;        // both IPv4 and IPv6 are ok
    hints.ai_socktype = SOCK_STREAM;    // TCP socket
    hints.ai_flags = AI_CANONNAME;

    int retval = getaddrinfo(g_host_name.c_str(), nullptr, &hints, &result);
    if (retval != 0) return false;

    for (ap = result; ap != nullptr; ap = ap->ai_next)
    {
        void *ptr;
        char addrstr[128];

        switch (ap->ai_family)
        {
            case AF_INET:
                ptr = &((struct sockaddr_in *)ap->ai_addr)->sin_addr;
                break;

            case AF_INET6:
                ptr = &((struct sockaddr_in6 *)ap->ai_addr)->sin6_addr;
                break;
        }

        inet_ntop(ap->ai_family, ptr, addrstr, sizeof(addrstr));
        if (ip == addrstr) return true;
    }

    return false;
}

bool
ts_resolution_ms()
{
    return starts_with(Config::inst()->get_str(CFG_TSDB_TIMESTAMP_RESOLUTION, CFG_TSDB_TIMESTAMP_RESOLUTION_DEF), 'm');
}

Timestamp
validate_resolution(Timestamp tstamp)
{
    return validate_resolution(tstamp, g_tstamp_resolution_ms);
}

Timestamp
validate_resolution(Timestamp tstamp, bool ms)
{
    if (ms)
    {
        if (tstamp < MAX_SEC_SINCE_EPOCH)
            tstamp *= 1000L;
        else if (tstamp >= MAX_US_SINCE_EPOCH)
            tstamp /= 1000000ul;    // ns to ms
        else if (tstamp >= MAX_MS_SINCE_EPOCH)
            tstamp /= 1000ul;       // us to ms
    }
    else    // second
    {
        if (tstamp >= MAX_US_SINCE_EPOCH)
            tstamp /= 1000000000ull;    // ns to sec
        else if (tstamp >= MAX_MS_SINCE_EPOCH)
            tstamp /= 1000000ull;       // us to sec
        else if (tstamp >= MAX_SEC_SINCE_EPOCH)
            tstamp /= 1000ull;          // ms to sec
    }

    return tstamp;
}

// TODO: make it inlined
bool
is_ms(Timestamp tstamp)
{
    return ((tstamp >= MAX_SEC_SINCE_EPOCH) && (tstamp < MAX_MS_SINCE_EPOCH));
}

bool
is_ns(Timestamp tstamp)
{
    return (tstamp >= MAX_US_SINCE_EPOCH);
}

bool
is_sec(Timestamp tstamp)
{
    return (tstamp < MAX_SEC_SINCE_EPOCH);
}

bool
is_us(Timestamp tstamp)
{
    return ((tstamp >= MAX_MS_SINCE_EPOCH) && (tstamp < MAX_US_SINCE_EPOCH));
}

// TODO: make it inlined
Timestamp
to_ms(Timestamp tstamp)
{
    if (tstamp < MAX_SEC_SINCE_EPOCH)
    //if (! g_tstamp_resolution_ms)
        tstamp *= 1000L;
    return tstamp;
}

// TODO: make it inlined
Timestamp
to_sec(Timestamp tstamp)
{
    if (tstamp > MAX_SEC_SINCE_EPOCH)
    //if (g_tstamp_resolution_ms)
        tstamp /= 1000L;
    return tstamp;
}

// 'value' should be either an absolute time (e.g. 1633418206),
// a relative time (e.g. 2h-ago), or an absolute formatted time
// (e.g. 2024/01/23-05:10:22)
Timestamp
parse_ts(const JsonValue *value, Timestamp now, const char *tz)
{
    ASSERT(tz != nullptr);
    ASSERT(value != nullptr);

    if (JsonValueType::JVT_DOUBLE == value->get_type())
        return (Timestamp)(value->to_double());

    const char *str = value->to_string();
    Timestamp ts = (Timestamp)std::atoll(str);
    int len = std::strlen(str);

    // relative time? (e.g. 2h-ago)
    if ((len > 1) && (str[len-1] == 'o'))
    {
        TimeUnit unit = to_time_unit(str, len);
        if (unit == TimeUnit::UNKNOWN) throw std::exception();
        ts = convert_time(ts, unit, g_tstamp_resolution_ms ? TimeUnit::MS : TimeUnit::SEC);
        ts = now - ts;  // relative to 'now'
    }
    // absolute formatted time?
    else if ((len >= 10) && (str[4] == '/') && (str[7] == '/'))
    {
        struct tm tm;

        memset(&tm, 0, sizeof(struct tm));

        if (len >= 19)
        {
            // yyyy/MM/dd HH:mm:ss or yyyy/MM/dd-HH:mm:ss
            ASSERT(str[10] == ' ' || str[10] == '-');
            if (str[10] == ' ')
                ::strptime(str, "%Y/%m/%d %H:%M:%S", &tm);
            else
                ::strptime(str, "%Y/%m/%d-%H:%M:%S", &tm);
        }
        else if (len >= 16)
        {
            // yyyy/MM/dd HH:mm or yyyy/MM/dd-HH:mm
            ASSERT(str[10] == ' ' || str[10] == '-');
            if (str[10] == ' ')
                ::strptime(str, "%Y/%m/%d %H:%M", &tm);
            else
                ::strptime(str, "%Y/%m/%d-%H:%M", &tm);
        }
        else
        {
            // yyyy/MM/dd
            ::strptime(str, "%Y/%m/%d", &tm);
        }

        // set timezone
        long tzdiff = 0;    // in seconds

        if (strcmp(tz, CFG_TSDB_TIMEZONE_DEF) != 0)
            tzdiff = get_tz_diff(tz);

        ts = timegm(&tm);

        if (g_tstamp_resolution_ms)
            ts -= tzdiff * 1000;
        else
            ts -= tzdiff;
    }

    return ts;
}

bool
is_timestamp(std::string& str)
{
    return !str.empty() && (str.find_first_not_of("0123456789") == std::string::npos);
}

long
get_tz_diff(const char *tz)
{
    static std::mutex s_tz_lock;
    std::lock_guard<std::mutex> guard(s_tz_lock);

    setenv("TZ", tz, 1);
    tzset();
    return timezone;
}

// 'str' should look something like: "2h"
TimeUnit
to_time_unit(const char *str, size_t len)
{
    TimeUnit unit = TimeUnit::UNKNOWN;
    size_t i;

    for (i = 0; i < len; i++)
    {
        auto ch = std::tolower(str[i]);
        if (('d' <= ch) && (ch <= 'y'))
            break;
    }

    if (i < len)
    {
        switch (std::tolower(str[i]))
        {
            case 'd':   unit = TimeUnit::DAY;   break;
            case 'h':   unit = TimeUnit::HOUR;  break;
            case 'm':
                i++;
                if (i < len)
                {
                    switch (std::tolower(str[i]))
                    {
                        case 'i':   unit = TimeUnit::MIN;   break;
                        case 'o':   unit = TimeUnit::MONTH; break;
                        case 's':   unit = TimeUnit::MS;    break;
                        default:    unit = TimeUnit::MIN;   break;
                    }
                }
                else
                    unit = TimeUnit::MIN;
                break;
            case 'n':   unit = TimeUnit::MONTH; break;
            case 's':   unit = TimeUnit::SEC;   break;
            case 'w':   unit = TimeUnit::WEEK;  break;
            case 'y':   unit = TimeUnit::YEAR;  break;
            default:                            break;
        }
    }

    return unit;
}

Timestamp
convert_time(Timestamp time, TimeUnit from_unit, TimeUnit to_unit)
{
    if (from_unit == to_unit)
        return time;

    switch (from_unit)
    {
        case TimeUnit::MS:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time /= 365 * 24 * (Timestamp)3600000;  break;
                case TimeUnit::MONTH:   time /= 30 * 24 * (Timestamp)3600000;   break;
                case TimeUnit::WEEK:    time /= 7;
                case TimeUnit::DAY:     time /= 24;
                case TimeUnit::HOUR:    time /= 60;
                case TimeUnit::MIN:     time /= 60;
                case TimeUnit::SEC:     time /= 1000;
                default:                break;
            }
            break;

        case TimeUnit::SEC:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time /= 365 * 24 * 3600;    break;
                case TimeUnit::MONTH:   time /= 30 * 24 * 3600;     break;
                case TimeUnit::WEEK:    time /= 7;
                case TimeUnit::DAY:     time /= 24;
                case TimeUnit::HOUR:    time /= 60;
                case TimeUnit::MIN:     time /= 60;     break;
                case TimeUnit::MS:      time *= 1000;   break;
                default:                                break;
            }
            break;

        case TimeUnit::MIN:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time /= 365 * 24 * 60;  break;
                case TimeUnit::MONTH:   time /= 30 * 24 * 60;   break;
                case TimeUnit::WEEK:    time /= 7;
                case TimeUnit::DAY:     time /= 24;
                case TimeUnit::HOUR:    time /= 60;     break;
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;     break;
                default:                                break;
            }
            break;

        case TimeUnit::HOUR:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time /= 365 * 24;   break;
                case TimeUnit::MONTH:   time /= 30 * 24;    break;
                case TimeUnit::WEEK:    time /= 7;
                case TimeUnit::DAY:     time /= 24;     break;
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;
                case TimeUnit::MIN:     time *= 60;     break;
                default:                                break;
            }
            break;

        case TimeUnit::DAY:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time /= 365;    break;
                case TimeUnit::MONTH:   time /= 30;     break;
                case TimeUnit::WEEK:    time /= 7;      break;
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;
                case TimeUnit::MIN:     time *= 60;
                case TimeUnit::HOUR:    time *= 24;     break;
                default:                                break;
            }
            break;

        case TimeUnit::WEEK:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time = (time * 7) / 365;    break;
                case TimeUnit::MONTH:   time = (time * 7) / 30;     break;
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;
                case TimeUnit::MIN:     time *= 60;
                case TimeUnit::HOUR:    time *= 24;
                case TimeUnit::DAY:     time *= 7;      break;
                default:                                break;
            }
            break;

        case TimeUnit::MONTH:
            switch (to_unit)
            {
                case TimeUnit::YEAR:    time = (time * 30) / 365;   break;
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;
                case TimeUnit::MIN:     time *= 60;
                case TimeUnit::HOUR:    time *= 24;
                case TimeUnit::DAY:     time *= 30;     break;
                case TimeUnit::WEEK:    time = (time * 30) / 7;     break;
                default:                                break;
            }
            break;

        case TimeUnit::YEAR:
            switch (to_unit)
            {
                case TimeUnit::MS:      time *= 1000;
                case TimeUnit::SEC:     time *= 60;
                case TimeUnit::MIN:     time *= 60;
                case TimeUnit::HOUR:    time *= 24;
                case TimeUnit::DAY:     time *= 365;    break;
                case TimeUnit::WEEK:    time = (time * 365) / 7;    break;
                case TimeUnit::MONTH:   time = (time * 365) / 30;   break;
                default:                                break;
            }
            break;

        default:
            break;
    }

    return time;
}

uint64_t
get_bytes_factor(const std::string& str)
{
    uint64_t factor = 1;
    size_t i;

    for (i = 0; i < str.size(); i++)
    {
        auto ch = std::tolower(str[i]);
        if (('b' <= ch) && (ch <= 't'))
            break;
    }

    if (i < str.size())
    {
        switch (std::tolower(str[i]))
        {
            case 't':   factor *= 1024;
            case 'g':   factor *= 1024;
            case 'm':   factor *= 1024;
            case 'k':   factor *= 1024;
            default:    break;
        }
    }

    return factor;
}

char *
trim_cstr(char* str)
{
    if (str == nullptr) return nullptr;
    while (WHITE_SPACES.find(*str) != std::string::npos) str++;
    int len = std::strlen(str);
    for (len--; (len >= 0) && (WHITE_SPACES.find(str[len]) != std::string::npos); len--) /*do nothing*/;
    str[len+1] = 0;
    return str;
}

// tokenize in place. no memory is allocated.
bool
tokenize(char* str, char* &key, char* &value, char delim)
{
    char* separator = strchr(str, delim);
    if (separator == nullptr)
    {
        key = trim_cstr(str);
        return false;
    }
    *separator = 0;
    key = trim_cstr(str);
    value = trim_cstr(separator + 1);
    return true;
}

bool
tokenize(char* str, char delim, std::vector<char*>& tokens)
{
    char *separator;

    while ((separator = strchr(str, delim)) != nullptr)
    {
        *separator = 0;
        tokens.push_back(str);
        str = separator + 1;
    }

    if (*str != 0)
    {
        tokens.push_back(str);
    }

    return true;
}

bool
tokenize(char* str, char *delim, std::vector<char*>& tokens)
{
    char *separator;
    int len = std::strlen(delim);

    while ((separator = std::strstr(str, delim)) != nullptr)
    {
        *separator = 0;
        tokens.push_back(str);
        str = separator + len;
    }

    if (*str != 0)
    {
        tokens.push_back(str);
    }

    return true;
}

void
tokenize(const std::string& str, std::vector<std::string>& tokens, std::regex& delim)
{
    //std::sregex_token_iterator it(str.begin(), str.end(), delim, -1);
    //std::sregex_token_iterator end;
    //std::vector<std::string> vec(it, end);

    //tokens.clear();
    //tokens.insert(std::end(tokens), std::begin(vec), std::end(vec));
    //tokens.insert(std::end(tokens), std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
}

bool
tokenize(const std::string& str, std::tuple<std::string,std::string>& kv, char delim)
{
    auto idx = str.find_first_of(delim);
    if (idx == std::string::npos) return false; // no match
    std::get<0>(kv) = trim(str.substr(0, idx));
    std::get<1>(kv) = trim(str.substr(idx+1));
    return true;
}

bool
tokenize(const std::string& str, std::vector<std::string>& tokens, char delim)
{
    std::string::size_type from = 0;
    std::string::size_type to;
    std::string::size_type len = str.length();

    while ((to = str.find_first_of(delim, from)) != std::string::npos)
    {
        tokens.push_back(str.substr(from, to-from));
        from = to + 1;
        if (from >= len) break;
    }

    if (from < len)
    {
        tokens.push_back(str.substr(from, len-from));
    }

    return true;
}

void
replace_last(std::string& str, const std::string& old_sub, const std::string& new_sub)
{
    auto pos = str.rfind(old_sub);
    if (pos != std::string::npos)
        str.replace(pos, old_sub.size(), new_sub, 0, new_sub.size());
}

int
replace_all(std::string& str, const std::string& from, const std::string& to)
{
    if (from.empty()) return 0;

    int count = 0;
    size_t start_pos = 0;

    while ((start_pos = str.find(from, start_pos)) != std::string::npos)
    {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
        count++;
    }

    return count;
}

bool
url_unescape(const char *url, char *buff, size_t len)
{
    ASSERT(url != nullptr);
    ASSERT(buff != nullptr);
    ASSERT(len > 1);

    while ((*url != 0) && (len > 1))
    {
        if (*url == '%')
        {
            char hex_chars[3];
            unsigned long hex;

            url++;
            hex_chars[0] = *url++;
            if (hex_chars[0] == 0) return false;
            hex_chars[1] = *url++;
            if (hex_chars[1] == 0) return false;
            hex_chars[2] = 0;       // null terminate

            hex = std::stoul(hex_chars, 0, 16);
            *buff++ = (unsigned char)hex;
        }
        else
        {
            *buff++ = *url++;
        }

        len--;
    }

    *buff = 0;  // null terminate
    return (*url == 0);
}

bool
file_exists(const std::string& full_path)
{
    struct stat buff;   
    return (stat(full_path.c_str(), &buff) == 0);
}

void
copy_file(const std::string& src_file, const std::string& dst_file)
{
    std::ifstream src(src_file, std::ios::binary);
    std::ofstream dst(dst_file, std::ios::binary);
    dst << src.rdbuf();
}

int
rm_file(const std::string& full_path)
{
    return std::remove(full_path.c_str());
}

int
rm_all_files(const std::string& pattern)
{
    glob_t glob_result;
    unsigned int cnt;

    glob(pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);

    for (cnt = 0; cnt < glob_result.gl_pathc; cnt++)
    {
        std::remove(glob_result.gl_pathv[cnt]);
    }

    globfree(&glob_result);

    return (int)cnt;
}

void
rm_dir(const std::string& full_path)
{
    if (file_exists(full_path))
    {
        rm_all_files(full_path + "/*");
        rm_file(full_path); // will do empty dir as well
    }
}

int
rotate_files(const std::string& pattern, int retain_count)
{
    glob_t glob_result;
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);

    std::vector<std::string> files;

    for (unsigned int i=0; i < glob_result.gl_pathc; i++)
    {
        files.push_back(std::string(glob_result.gl_pathv[i]));
    }

    globfree(&glob_result);

    std::sort(files.begin(), files.end());

    int cnt = (int)files.size() - retain_count;

    for (std::string& file: files)
    {
        if (cnt <= 0) break;
        std::remove(file.c_str());
        cnt--;
    }

    return files.size() - retain_count;
}

std::string
last_file(const std::string& pattern)
{
    glob_t glob_result;
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);

    std::vector<std::string> files;

    for (unsigned int i=0; i < glob_result.gl_pathc; i++)
        files.push_back(std::string(glob_result.gl_pathv[i]));

    globfree(&glob_result);

    if (files.empty())
        return std::string();
    else
    {
        std::sort(files.begin(), files.end());
        return files.back();
    }
}

uint64_t
get_disk_block_size(const std::string& full_path)
{
    struct statvfs st;
    int rc = statvfs(full_path.c_str(), &st);
    if (rc != 0) return 0;
    return st.f_bsize;
}

uint64_t
get_disk_available_blocks(const std::string& full_path)
{
    struct statvfs st;
    int rc = statvfs(full_path.c_str(), &st);
    if (rc != 0) return 0;
    return st.f_bavail;
}

uint64_t
get_ram_total()
{
    std::string token;
    std::ifstream meminfo("/proc/meminfo");

    while (meminfo >> token)
    {
        if (token == "MemTotal:")
        {
            uint64_t mem_total;

            if (meminfo >> mem_total)
            {
                uint64_t factor = 1;
                if (meminfo >> token)
                    factor = get_bytes_factor(token);
                return mem_total * factor;
            }
            else
                return 0;
        }

        meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }

    return 0;
}

// perform func() for all dirs at 'level'
void
for_all_dirs(const std::string& root, void (*func)(const std::string& dir), int level)
{
    if (level == 0)
    {
        (*func)(root);
        return;
    }

    DIR *dir;
    struct dirent *dir_ent;

    dir = opendir(root.c_str());

    if (dir != nullptr)
    {
        std::vector<std::string> entries;

        while (dir_ent = readdir(dir))
        {
            if (dir_ent->d_type != DT_DIR)
                continue;
            if (dir_ent->d_name[0] == '.')
                continue;
            std::string dir_name(root + "/" + dir_ent->d_name);
            entries.push_back(dir_name);
            //for_all_dirs(dir_name, func, level-1);
        }

        std::sort(entries.begin(), entries.end());

        for (auto entry: entries)
            for_all_dirs(entry, func, level-1);
    }

    closedir(dir);
}

bool
is_dir_empty(const std::string& path)
{
    DIR *dir;
    struct dirent *dir_ent;
    bool empty = true;

    dir = opendir(path.c_str());

    if (dir != nullptr)
    {
        while (dir_ent = readdir(dir))
        {
            if (dir_ent->d_name[0] != '.')
            {
                empty = false;
                break;
            }
        }
    }

    closedir(dir);
    return empty;
}

void
set_hostname_working_dir()
{
    char buff[PATH_MAX];

    gethostname(buff, sizeof(buff));
    g_host_name.assign(buff);

    getcwd(buff, sizeof(buff)); // get current working dir
    g_working_dir.assign(buff);
}

// find for all files matching 'pattern'
void
get_all_files(const std::string& pattern, std::vector<std::string>& files)
{
    glob_t glob_result;
    unsigned int cnt;

    glob(pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);

    for (cnt = 0; cnt < glob_result.gl_pathc; cnt++)
        files.push_back(std::string(glob_result.gl_pathv[cnt]));

    globfree(&glob_result);
}

int
create_dir(const std::string& path, bool except_last)
{
    if (path.length() >= PATH_MAX)
    {
        Logger::error("Path too long: %s", path.c_str());
        return -1;
    }

    std::vector<std::string> dirs;
    tokenize(path, dirs, '/');
    size_t size = dirs.size();
    if (except_last && (1 <= size)) size--;
    if (size <= 0) return -1;

    char buff[PATH_MAX];
    mode_t mode = S_IRWXU|S_IRGRP|S_IROTH;

    buff[0] = 0;

    for (int i = 0; i < size; i++)
    {
        if (dirs[i].length() <= 0) continue;
        std::strcat(buff, "/");
        std::strcat(buff, dirs[i].c_str());

        struct stat st;
        if (stat(buff, &st) == 0)
        {
            if (S_ISDIR(st.st_mode))
                continue;
            Logger::error("File %s already exist", buff);
            return -1;
        }

        if (mkdir(buff, mode) != 0)
        {
            Logger::error("Failed to create directory %s, errno = %d", buff, errno);
            return -1;
        }
    }

    return 0;
}

std::string
get_dir_of(std::string& file_name)
{
    auto pos = file_name.rfind('/');
    return (pos == std::string::npos) ? "" : file_name.substr(0, pos);
}

FileIndex
get_file_suffix(const std::string& file_name)
{
    const char *dot = std::strrchr(file_name.c_str(), '.');
    if (dot == nullptr) return TT_INVALID_FILE_INDEX;
    ASSERT(std::isdigit(*(dot+1)));
    return std::atoi(dot+1);
}

bool
dp_pair_less(const DataPointPair& lhs, const DataPointPair& rhs)
{
    return (lhs.first < rhs.first);
}

std::string
ltrim(const std::string& str)
{
    size_t start = str.find_first_not_of(WHITE_SPACES);
    return (start == std::string::npos) ? "" : str.substr(start);
}

std::string
rtrim(const std::string& str)
{
    size_t end = str.find_last_not_of(WHITE_SPACES);
    return (end == std::string::npos) ? "" : str.substr(0, end+1);
}

std::string
trim(const std::string& str)
{
    return rtrim(ltrim(str));
}

bool
starts_with(const std::string& str, char ch)
{
    return (str.find_first_of(ch) == 0);
}

bool
starts_with(const char *str, const char *prefix)
{
    if (prefix == nullptr) return false;
    int len = strlen(prefix);
    if (len > strlen(str)) return false;
    return (::strncmp(str, prefix, len) == 0);
}

bool
starts_with_case_insensitive(const char *str, const char *prefix)
{
    if (prefix == nullptr) return false;
    int len = strlen(prefix);
    if (len > strlen(str)) return false;
    return (::strncasecmp(str, prefix, len) == 0);
}

bool
ends_with(const char *str, const char tail)
{
    std::size_t len = std::strlen(str);
    return (len > 0) && (str[len-1] == tail);
}

bool
ends_with(const std::string& str, const std::string& tail)
{
    auto size1 = str.size();
    auto size2 = tail.size();

    if (size1 < size2) return false;

    return (str.compare(size1-size2, size2, tail) == 0);
}

void
spin_yield(unsigned int k)
{
    if (k >= SPIN_YIELD_THRESHOLD)
    {
        k = std::min((k - SPIN_YIELD_THRESHOLD) / SPIN_YIELD_THRESHOLD, (unsigned int)1000);
        std::this_thread::sleep_for(std::chrono::milliseconds(k));
    }
}

bool
is_aligned(uintptr_t ptr, unsigned long align)
{
    return ((ptr % align) == 0);
}

bool
operator<(const DataPointPair& lhs, const DataPointPair& rhs)
{
    return lhs.first < rhs.first;
}

void
print_double_in_hex(double n)
{
    uint8_t *x = reinterpret_cast<uint8_t*>(&n);
    printf("%lf = 0x%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx\n",
        n, x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]);
}

void
print_uint16_t_in_hex(uint16_t n)
{
    uint8_t *x = reinterpret_cast<uint8_t*>(&n);
    printf("%d = 0x%02hhx%02hhx\n", n, x[0], x[1]);
}

void
print_uint32_t_in_hex(uint32_t n)
{
    uint8_t *x = reinterpret_cast<uint8_t*>(&n);
    printf("%d = 0x%02hhx%02hhx%02hhx%02hhx\n", n, x[0], x[1], x[2], x[3]);
}

void
print_uint64_t_in_hex(uint64_t n)
{
    uint8_t *x = reinterpret_cast<uint8_t*>(&n);
    printf("%" PRIu64 " = 0x%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx\n",
        n, x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]);
}

/* Given an array of numbers ('set'), find a subset whose sum is
 * as large as possible without exceeding the target (4096).
 *
 * The output is the sum of the resulting subset. Upon exiting,
 * the 'subset' will contain the indices of the members of the
 * maximum subset.
 */
int
max_subset_4k(int16_t set[], size_t size, std::vector<int>& subset)
{
    const int16_t target = 4096;
    const size_t size1 = size + 1;
    const size_t target1 = target + 1;
    DynamicArray2D<std::pair<int16_t,int16_t> > matrix(size1, target1);

    // initialize
    subset.clear();
    for (size_t t = 0; t <= target; t++)
    {
        matrix.elem(size,t).first = t;
        matrix.elem(size,t).second = 0;
    }

    // calculate answer
    for (int s = size-1; s >= 0; s--)
    {
        for (int t = target; t >= 0; t--)
        {
            int16_t include = 0;

            if ((t + set[s]) <= target)
            {
                include = matrix.elem(s+1,t+set[s]).first;
            }

            int exclude = matrix.elem(s+1,t).first;
            std::pair<int16_t,int16_t>& st = matrix.elem(s,t);

            if (include >= exclude)
            {
                st.first = include;
                st.second = 1;
            }
            else
            {
                st.first = exclude;
                st.second = 0;
            }
        }
    }

    // construct solution
    int sum = 0;

    for (int s = 0; s < size1; s++)
    {
        if (matrix.elem(s,sum).second != 0)
        {
            subset.push_back(s);
            sum += set[s];
        }
    }

    sum = matrix.elem(0,0).first;
    return sum;
}

void set_rollup_level(RollupType& rollup, bool level2)
{
    ASSERT(rollup != RollupType::RU_NONE);
    ASSERT(rollup != RollupType::RU_LEVEL2);

    if (level2)
        rollup = (RollupType) (rollup | RU_LEVEL2);
    else
        rollup = (RollupType) (rollup & ~RU_LEVEL2);
}

bool is_rollup_level2(RollupType rollup)
{
    return (rollup & RU_LEVEL2) == RU_LEVEL2;
}


}
