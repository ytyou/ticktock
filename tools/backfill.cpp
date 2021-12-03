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

#include <cstdio>
#include <climits>
#include <string>
#include <cstring>
#include <assert.h>
#include <getopt.h>
#include <glob.h>
#include <vector>
#include <algorithm>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "zlib/zlib.h"


#define BUF_SIZE 8192

std::string g_append_log_dir;
uint64_t g_from_tstamp = 0;
uint64_t g_to_tstamp = ULONG_MAX;
unsigned long g_rotation_sec = 3600;

int g_socket_fd = -1;
std::string g_ticktock_host("127.0.0.1");
int g_ticktock_port = 6182;
bool g_verbose = false;
bool g_dry_run = false;


void
log_verbose(const char *msg)
{
    if (g_verbose)
    {
        printf("%s\n", msg);
    }
}

static int
process_cmdline_opts(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "a:df:h:p:r:t:v")) != -1)
    {
        switch (c)
        {
            case 'a':
                g_append_log_dir = optarg;
                break;

            case 'd':
                g_dry_run = true;
                break;

            case 'f':
                g_from_tstamp = std::stoull(optarg);
                break;

            case 'h':
                g_ticktock_host = optarg;
                break;

            case 'p':
                g_ticktock_port = std::stoi(optarg);
                break;

            case 'r':
                g_rotation_sec = std::stoull(optarg);
                break;

            case 't':
                g_to_tstamp = std::stoull(optarg);
                break;

            case 'v':
                g_verbose = true;
                break;

            case '?':
                if (optopt == 'a')
                {
                    fprintf(stderr, "Option -a requires an append log file.\n");
                }
                else if (optopt == 'f')
                {
                    fprintf(stderr, "Option -f requires a 'from' timestamp.\n");
                }
                else if (optopt == 't')
                {
                    fprintf(stderr, "Option -t requires a 'to' timestamp.\n");
                }
                else
                {
                    fprintf(stderr, "Unknown option: '\\x%x'.\n", optopt);
                }
                return 1;

            default:
                return 2;
        }
    }

    if (g_dry_run)
    {
        fprintf(stderr, "Dry run! Data will be sent to stdout!\n");
    }

    if (g_append_log_dir.empty())
    {
        fprintf(stderr, "Append log directory from which to backfill is not specified (-a) and required!\n");
        return 3;
    }

    if (g_from_tstamp > g_to_tstamp)
    {
        fprintf(stderr, "'From' timstamp (%" PRIu64 ") can't be greater than 'to' timestamp (%" PRIu64 ")!\n",
            g_from_tstamp, g_to_tstamp);
        return 4;
    }

    printf("Restoring from append logs under: %s (time range: %" PRIu64 " - %" PRIu64 ")\n",
        g_append_log_dir.c_str(), g_from_tstamp, g_to_tstamp);
    return 0;
}

int
http_setup()
{
    if (g_dry_run) return 0;

    g_socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (g_socket_fd < 0)
    {
        fprintf(stderr, "socket error: %d\n", errno);
        return 1;
    }

    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, g_ticktock_host.c_str(), &addr.sin_addr) <= 0)
    {
        fprintf(stderr, "invalid host: %s\n", g_ticktock_host.c_str());
        return 2;
    }
    addr.sin_port = htons(g_ticktock_port);

    if (connect(g_socket_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0)
    {
        fprintf(stderr, "connect error: %d\n", errno);
        return 3;
    }

    return 0;
}

void
http_cleanup()
{
    if (g_socket_fd >= 0)
    {
        close(g_socket_fd);
        g_socket_fd = 0;
    }
}

int
http_post(char *body)
{
    // trim first
    while ((*body == ' ') || (*body == '\r') || (*body == '\n')) body++;

    char buff[BUF_SIZE + 512];
    int length = std::strlen(body);
    int sent_total = 0;

    if (length <= 4) return 0;

    // send POST request
    int n = snprintf(buff, sizeof(buff),
        "POST /api/put HTTP/1.1\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: keep-alive\r\n\r\n%s\r\n",
        length+2, body);
    length = std::strlen(buff);

    if (g_dry_run)
    {
        printf("%s", buff);
        return 0;
    }
    else
    {
        log_verbose("Sending HTTP request...");
    }

    while (length > 0)
    {
        int sent = send(g_socket_fd, &buff[sent_total], length, 0);

        if (sent == -1)
        {
            fprintf(stderr, "http_post failed: %d\n", errno);
            return 1;
        }

        length -= sent;
        sent_total += sent;
    }

    log_verbose("Waiting for HTTP response...");

    // receive response
    int ret = recv(g_socket_fd, buff, sizeof(buff), 0);

    if (ret == -1)
    {
        fprintf(stderr, "recv() failed: %d\n", errno);
        return 2;
    }
    if (ret == 0) return 0;

    buff[ret] = 0;
    const char *ok = std::strstr(buff, "200 OK");
    log_verbose(buff);

    return (ok == nullptr) ? 3 : 0;
}

int
backfill_from(std::FILE *src)
{
    int ret;
    unsigned cnt;
    z_stream strm;
    unsigned char in[BUF_SIZE];
    unsigned char outs[2][BUF_SIZE+1];

    // allocate inflate state
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;

    ret = inflateInit(&strm);
    if (ret != Z_OK) return ret;

    unsigned buf_idx = 0;
    unsigned buf_off = 0;

    do
    {
        strm.avail_in = fread(in, 1, BUF_SIZE, src);

        if (ferror(src))
        {
            inflateEnd(&strm);
            return Z_ERRNO;
        }

        if (strm.avail_in == 0)
            break;

        strm.next_in = in;

        /* run inflate() on input until output buffer not full */
        do
        {
            unsigned buf_size = BUF_SIZE - buf_off;

            strm.avail_out = buf_size;
            strm.next_out = &outs[buf_idx][buf_off];
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */

            switch (ret)
            {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    inflateEnd(&strm);
                    return ret;
            }

            cnt = buf_size - strm.avail_out + buf_off;

            assert(cnt <= BUF_SIZE);
            outs[buf_idx][cnt] = 0;

            char *last_nl = std::strrchr((char*)&outs[buf_idx][0], '\n');
            //assert(last_nl != nullptr);

            if (last_nl == nullptr)
            {
                buf_off = cnt;
            }
            else
            {
                last_nl++;
                unsigned other_buf = (buf_idx + 1) % 2;
                std::strcpy((char*)&outs[other_buf][0], last_nl);
                *last_nl = 0;
                buf_off = std::strlen((const char*)&outs[other_buf][0]);

                if (http_post((char*)&outs[buf_idx][0]) != 0) return 1;
                buf_idx = other_buf;
            }
        } while (strm.avail_out == 0);

        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);

    /* clean up and return */
    inflateEnd(&strm);
    return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

int
backfill_from2(std::FILE *src)
{
    int ret;
    unsigned cnt;
    z_stream strm;
    unsigned char in[BUF_SIZE];
    unsigned char out[BUF_SIZE+1];

    // allocate inflate state
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;

    ret = inflateInit(&strm);
    if (ret != Z_OK) return ret;

    do
    {
        strm.avail_in = fread(in, 1, BUF_SIZE, src);

        if (ferror(src))
        {
            inflateEnd(&strm);
            return Z_ERRNO;
        }

        if (strm.avail_in == 0)
            break;

        strm.next_in = in;

        /* run inflate() on input until output buffer not full */
        do
        {
            strm.avail_out = BUF_SIZE;
            strm.next_out = out;
            ret = inflate(&strm, Z_NO_FLUSH);
            assert(ret != Z_STREAM_ERROR);  /* state not clobbered */

            switch (ret)
            {
                case Z_NEED_DICT:
                    ret = Z_DATA_ERROR;     /* and fall through */
                case Z_DATA_ERROR:
                case Z_MEM_ERROR:
                    inflateEnd(&strm);
                    return ret;
            }

            cnt = BUF_SIZE - strm.avail_out;

            assert(cnt <= BUF_SIZE);
            out[cnt] = 0;

            printf("%s", out);
        } while (strm.avail_out == 0);

        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);

    /* clean up and return */
    inflateEnd(&strm);
    return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

int
main(int argc, char *argv[])
{
    int ret = process_cmdline_opts(argc, argv);
    if (ret != 0) return ret;

    if (g_append_log_dir.back() != '/')
        g_append_log_dir.append("/");

    int dir_len = g_append_log_dir.size();
    std::string pattern(g_append_log_dir.append("append.*.log.zip"));
    glob_t glob_result;
    glob(pattern.c_str(), GLOB_TILDE, nullptr, &glob_result);

    std::vector<std::string> files;

    for (unsigned int i = 0; i < glob_result.gl_pathc; i++)
    {
        files.push_back(std::string(glob_result.gl_pathv[i]));
    }

    globfree(&glob_result);

    std::sort(files.begin(), files.end());

    if (http_setup() != 0) return 1;

    for (std::string& file: files)
    {
        std::string f = file.substr(dir_len+7); // 7 is length of 'append.'
        std::string::size_type n = f.find_first_of('.');
        uint64_t ts = std::stoull(f.substr(0, n));
        // [g_from_tstamp - g_to_tstamp] intersect [ts - ts+g_rotation_sec]?
        if (((ts+g_rotation_sec) < g_from_tstamp) || (g_to_tstamp < ts))
        {
            printf("Skipped: %s\n", file.c_str());
            continue;
        }

        // backfill from this file
        printf("Backfilling from %s...\n", file.c_str());
        std::FILE *src = std::fopen(file.c_str(), "r");

        if (src == nullptr)
        {
            fprintf(stderr, "Failed to open append log %s to read, errno = %d\n",
                file.c_str(), errno);
            continue;
        }

        int ret = backfill_from(src);
        if (ret != Z_OK) fprintf(stderr, "Failed to backfill from %s!\n", file.c_str());

        std::fclose(src);
    }

    http_cleanup();
    return 0;
}
