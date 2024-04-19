#include <cassert>
#include <cstdio>
#include <cstring>
#include <chrono>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <thread>
#include <unistd.h>

const char *dp[] = {"POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-0\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000000000 0 sensor=s_0 device=d_0\nput g_1 150000000000 1 sensor=s_1 device=d_1\nput g_2 150000000000 2 sensor=s_2 device=d_2\nput g_3 150000000000 3 sensor=s_3 device=d_3\nput g_4 150000000000 4 sensor=s_4 device=d_4", "POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-1\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000010000 0 sensor=s_0 device=d_0\nput g_1 150000010000 1 sensor=s_1 device=d_1\nput g_2 150000010000 2 sensor=s_2 device=d_2\nput g_3 150000010000 3 sensor=s_3 device=d_3\nput g_4 150000010000 4 sensor=s_4 device=d_4", "POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-2\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000020000 0 sensor=s_0 device=d_0\nput g_1 150000020000 1 sensor=s_1 device=d_1\nput g_2 150000020000 2 sensor=s_2 device=d_2\nput g_3 150000020000 3 sensor=s_3 device=d_3\nput g_4 150000020000 4 sensor=s_4 device=d_4", "POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-3\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000030000 0 sensor=s_0 device=d_0\nput g_1 150000030000 1 sensor=s_1 device=d_1\nput g_2 150000030000 2 sensor=s_2 device=d_2\nput g_3 150000030000 3 sensor=s_3 device=d_3\nput g_4 150000030000 4 sensor=s_4 device=d_4", "POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-4\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000040000 0 sensor=s_0 device=d_0\nput g_1 150000040000 1 sensor=s_1 device=d_1\nput g_2 150000040000 2 sensor=s_2 device=d_2\nput g_3 150000040000 3 sensor=s_3 device=d_3\nput g_4 150000040000 4 sensor=s_4 device=d_4", "POST /api/put HTTP/1.1\r\nX-Request-ID: pool-3-thread-299-5\r\nContent-Length: 224\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6182\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\nput g_0 150000050000 0 sensor=s_0 device=d_0\nput g_1 150000050000 1 sensor=s_1 device=d_1\nput g_2 150000050000 2 sensor=s_2 device=d_2\nput g_3 150000050000 3 sensor=s_3 device=d_3\nput g_4 150000050000 4 sensor=s_4 device=d_4"};

const char *q = "POST /api/query HTTP/1.1\r\nX-Request-ID: pool-3-thread-338-0\r\nContent-Length: 149\r\nContent-Type: text/plain; charset=UTF-8\r\nHost: tt-dev0-s:6183\r\nConnection: Keep-Alive\r\nUser-Agent: Apache-HttpClient/4.5.13 (Java/1.8.0_301)\r\nAccept-Encoding: gzip,deflate\r\n\r\n{\"msResolution\":true,\"start\":150000000000,\"end\":150000050001,\"queries\":[{\"metric\":\"g_2\",\"aggregator\":\"none\",\"tags\":{\"sensor\":\"s_2\",\"device\":\"d_2\"}}]}";


int main(int argc, char *argv[])
{
    // connect to server
    printf("connecting...\n");
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) std::perror("socket() failed");

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(6182);

    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc < 0) std::perror("connect() failed");

    // inject some data points
    printf("connected, injecting...\n");
    char buff[8192];
    int n;

    for (int i = 0; i < 6; i++)
    {
        printf("write(%d)...\n", i);
        n = write(fd, dp[i], strlen(dp[i]));
        if (n != strlen(dp[i])) std::perror("write() failed");

        printf("reading response...\n");
        n = read(fd, buff, sizeof(buff));
        if (n < 0 || n >= sizeof(buff)) std::perror("read() failed");
        buff[n] = 0;
        printf("response: %s\n", buff);
    }

    // query, whole package
    printf("query, all at once...\n");
    n = write(fd, q, strlen(q));
    if (n != strlen(q)) std::perror("query-all-at-once() failed");

    printf("reading response...\n");
    n = read(fd, buff, sizeof(buff));
    if (n < 0 || n >= sizeof(buff)) std::perror("read() failed");
    buff[n] = 0;
    printf("response: %s\n", buff);

    // query, header first, then body
    printf("query, header first, then body...\n");
    const char *body = std::strchr(q, '{');
    assert(body != nullptr);
    n = write(fd, q, strlen(q)-strlen(body));
    if (n != (strlen(q)-strlen(body))) std::perror("write-header() failed");

    int pause = 6;
    std::this_thread::sleep_for(std::chrono::seconds(pause));

    n = write(fd, body, strlen(body));
    if (n != strlen(body)) std::perror("write-body() failed");

    printf("reading response...\n");
    n = read(fd, buff, sizeof(buff));
    if (n < 0 || n >= sizeof(buff)) std::perror("read() failed");
    buff[n] = 0;
    printf("response: %s\n", buff);

    // query, 1 char at a time
    printf("query, 1-char at a time, be patient...\n");
    for (int i = 0; i < strlen(q); i++)
    {
        std::cerr << "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\bi=" << i << " (of " << strlen(q) << ")";
        n = write(fd, q+i, 1);
        if (n != 1) std::perror("write-char() failed");
        std::this_thread::sleep_for(std::chrono::seconds(pause));
    }

    printf("\nreading response...\n");
    n = read(fd, buff, sizeof(buff));
    if (n < 0 || n >= sizeof(buff)) std::perror("read() failed");
    buff[n] = 0;
    printf("response: %s\n", buff);

    // disconnect
    printf("disconnecting...\n");
    close(fd);
    return 0;
}
