// Compile with: gcc -std=c99 client_tcp.c -o client_tcp -pthread -lm
//
// For stress testing of TcpServer.

#define _GNU_SOURCE
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <pthread.h>

#define DAYS    7
#define STEP    7
#define HOSTS   100
#define METRICS 150
#define BUF_SIZE 256


long now_ms()
{
    struct timespec spec;
    clock_gettime(CLOCK_MONOTONIC, &spec);
    long ms;

    time_t sec = spec.tv_sec;
    ms = round(spec.tv_nsec/1.0e6);

    if (ms > 999)
    {
        sec++;
        ms = 0;
    }

    return sec * 1000 + ms;
}

double
random_between(double from, double to)
{
    int n = rand();
    return ((double)n / (double)RAND_MAX) * (to - from) + from;
}

struct pthread_data
{
    pthread_t tid;
    int id;
    int host_from;  // inclusive
    int host_to;    // exclusive
    int host_fd[HOSTS];
};

int
tcp_send(int fd, char *body, int len)
{
    int sent_total = 0;

    while (len > 0)
    {
        int sent = send(fd, body+sent_total, len, 0);

        if (sent == -1)
        {
            fprintf(stderr, "tcp_send failed: %d\n", errno);
            return 1;
        }

        len -= sent;
        sent_total += sent;
    }

    return 0;
}

int
connect_to_host()
{
    int fd, retval;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd == -1)
    {
        printf("socket() failed, errno = %d\n", errno);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(6181);
    inet_pton(AF_INET, "172.16.16.100", &addr.sin_addr.s_addr);

    retval = connect(fd, &addr, sizeof(addr));
    if (fd == -1)
    {
        printf("connect() failed, errno = %d\n", errno);
        return -1;
    }

    return fd;
}

void *pthread_main(void *data)
{
    struct pthread_data *pdata = (struct pthread_data*)data;
    int retval;

    printf("thread %d started\n", pdata->id);

    long now = time(0);
    long then = now - 3600 * 24 * DAYS;
    int buf_size = BUF_SIZE * (METRICS + 4);
    char buff[buf_size + 16];

    for (long ts = then; ts < now; ts += STEP)
    {
        for (int host = pdata->host_from; host < pdata->host_to; host++)
        {
            int len = 0;

            for (int metric = 0; metric < METRICS; metric++)
            {
                len += snprintf(&buff[len], buf_size-len,
                    "put metric_%d %ld %f thread=%d host=host_%d\n",
                    metric, ts, random_between(-10.0, 100.0), pdata->id, host);
            }

            buff[len] = 0;

            if (tcp_send(pdata->host_fd[host], &buff[0], len) != 0)
            {
                fprintf(stderr, "FAILED\n");
                break;
            }
        }
    }

    printf("thread %d ended\n", pdata->id);
    return NULL;
}

int main(int argc, char *argv[])
{
    int thread_cnt = 1;

    srand(time(0));

    if (argc > 1) thread_cnt = atoi(argv[1]);
    printf("will create %d threads\n", thread_cnt);

    struct pthread_data data[thread_cnt];
    long start_ms = now_ms();
    int host_per_thread = HOSTS / thread_cnt;

    for (int i = 0; i < thread_cnt; i++)
    {
        data[i].id = i;
        data[i].host_from = i * host_per_thread;
        data[i].host_to = data[i].host_from + host_per_thread;

        for (int h = data[i].host_from; h < data[i].host_to; h++)
            data[i].host_fd[h] = connect_to_host();

        int retval = pthread_create(&data[i].tid, NULL, pthread_main, &data[i]);
        if (retval != 0)
        {
            printf("[ERROR] Failed to create thread, error = %d\n", retval);
            exit(1);
        }
    }

    for (int i = 0; i < thread_cnt; i++)
    {
        pthread_join(data[i].tid, NULL);

        for (int h = data[i].host_from; h < data[i].host_to; h++)
            close(data[i].host_fd[h]);
    }

    long end_ms = now_ms();
    double cnt = ((double)(24*3600*DAYS) / (double)STEP) * METRICS * thread_cnt * host_per_thread;
    printf("sent %f msgs in %ld ms (%f/s)\n", cnt, end_ms - start_ms, (cnt/(end_ms-start_ms))*1000);
    return 0;
}
