/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),
    Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).

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

#include <atomic>
#include <iostream>
#include <csignal>
#include <memory>
#include <random>
#include <limits.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "config.h"
#include "append.h"
#include "memmgr.h"
#include "http.h"
#include "udp.h"
#include "query.h"
#include "tsdb.h"
#include "stats.h"
#include "timer.h"
#include "utils.h"
#include "logger.h"
#include "leak.h"


static void shutdown();

namespace tt
{


static bool g_run_as_daemon = false;
static std::string g_pid_file = "/var/run/ticktock.pid";


static void
intr_handler(int sig)
{
    g_shutdown_requested = true;

    printf("Interrupted, shutting down...\n");
    Logger::info("Interrupted, shutting down...");

    if (http_server_ptr != nullptr)
    {
        http_server_ptr->shutdown();
    }
}

}


using namespace tt;

static int
process_cmdline_opts(int argc, char *argv[])
{
    int c;

    while ((c = getopt(argc, argv, "c:dl:p:rt:")) != -1)
    {
        switch (c)
        {
            case 'c':
                g_config_file = optarg;     // config file
                break;

            case 'd':
                g_run_as_daemon = true;     // run in daemon mode
                break;

            case 'l':
                Logger::set_level(optarg);  // log level
                break;

            case 'p':
                g_pid_file.assign(optarg);
                break;

            case 'r':
                g_opt_reuse_port = true;    // reuse port
                break;

            case '?':
                if (optopt == 'c')
                {
                    fprintf(stderr, "Option -c requires an config file.\n");
                }
                else if (optopt == 'l')
                {
                    fprintf(stderr, "Option -l requires a log level.\n");
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

    return 0;
}

static void
daemonize(const char *cwd)
{
    pid_t pid;

    // fork off the parent process
    pid = fork();
    if (pid < 0) exit(EXIT_FAILURE);

    // let the parent terminate
    if (pid > 0) exit(EXIT_SUCCESS);

    // child process becomes session leader
    if (setsid() < 0) exit(EXIT_FAILURE);

    // ignore signals sent from child to parent process
    std::signal(SIGCHLD, SIG_IGN);

    // fork again
    pid = fork();
    if (pid < 0) exit(EXIT_FAILURE);

    // let the parent terminate
    if (pid > 0) exit(EXIT_SUCCESS);

    // set file permissions
    umask(S_IWGRP | S_IWOTH);

    int retval = chdir(cwd);

    if (retval < 0)
    {
        fprintf(stderr, "chdir failed: errno = %d\n", errno);
    }

    // close all open file descriptors
    int fd;
    for (fd = sysconf(_SC_OPEN_MAX); fd >= 0; fd--)
    {
        close(fd);
    }

    // write pid file
    fd = open(g_pid_file.c_str(), O_RDWR|O_CREAT, 0640);
    if (fd < 0)
    {
        fprintf(stderr, "Failed to write pid file %s: errno = %d\n", g_pid_file.c_str(), errno);
        exit(EXIT_FAILURE);
    }
    if (lockf(fd, F_TLOCK, 0) < 0) exit(EXIT_FAILURE);
    char buff[16];
    sprintf(buff, "%d\n", getpid());
    write(fd, buff, strlen(buff));
    close(fd);
}

// one time initialization, at the beginning of the execution
static void
initialize()
{
    g_thread_id = "main";
    std::srand(std::time(0));

    // get our host name
    char buff[PATH_MAX];
    gethostname(buff, sizeof(buff));
    g_host_name.assign(buff);

    if (g_run_as_daemon)
    {
        // get our working directory
        getcwd(buff, sizeof(buff));
        daemonize(buff);
    }
    else
    {
        printf(" TickTock v%d.%d.%d\n"
               " Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),\n"
               " Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).\n"
               " This program comes with ABSOLUTELY NO WARRANTY. It is free software, and\n"
               " you are welcome to redistribute it under certain conditions. For details,\n"
               " see <https://www.gnu.org/licenses/>.\n",
               TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION);
    }

    Config::init();
    Logger::init();
    MemoryManager::init();
    Tsdb::init();
    AppendLog::init();
    Stats::init();
    QueryExecutor::init();
    //SanityChecker::init();
    Timer::inst()->start();

    unsigned int n = std::thread::hardware_concurrency();
    Logger::info("TickTock version: %d.%d.%d, on %s",
        TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION, g_host_name.c_str());
    Logger::info("std::thread::hardware_concurrency() = %d", n);
    Logger::info("sizeof(std::pair<Timestamp,double>) = %d", sizeof(std::pair<Timestamp,double>));
    Logger::info("sizeof(struct page_info_on_disk) = %d", sizeof(struct page_info_on_disk));
    Logger::info("page-size = %d", sysconf(_SC_PAGE_SIZE));
    Logger::info("Using config file: %s", g_config_file.c_str());
    Logger::info("Timestamp resolution: %s", (g_tstamp_resolution_ms ? "millisecond" : "second"));
}

static void
shutdown()
{
    LD_STATS("Before shutdown");
    printf("Start shutdown process...\n");
    Logger::info("Start shutdown process...");

    try
    {
        Timer::inst()->stop();
        QueryExecutor::inst()->shutdown();
        Tsdb::shutdown();
        MemoryManager::log_stats();
        // MM are thread-local singletons, and can't be cleaned up from another thread
        //MemoryManager::cleanup();
    }
    catch (std::exception& ex)
    {
        Logger::warn("caught exception when shutting down: %s", ex.what());
        fprintf(stderr, "caught exception when shutting down: %s\n", ex.what());
    }

    Logger::info("Shutdown process complete");
    LD_STATS("After shutdown");
    Logger::inst()->close();
    printf("Shutdown process complete\n");
}

int
main(int argc, char *argv[])
{
#ifdef _DEBUG
    std::signal(SIGFPE, segv_handler);
    std::signal(SIGSEGV, segv_handler);
#else
    std::signal(SIGFPE, intr_handler);
    std::signal(SIGSEGV, intr_handler);
#endif

    if (process_cmdline_opts(argc, argv) != 0) return 1;

    initialize();

    // start an HttpServer
    HttpServer http_server;
    http_server.init();
    http_server.start(Config::get_int(CFG_HTTP_SERVER_PORT, CFG_HTTP_SERVER_PORT_DEF));
    http_server_ptr = &http_server;

    // start a TcpServer
    TcpServer tcp_server;
    tcp_server.start(Config::get_int(CFG_TCP_SERVER_PORT, CFG_TCP_SERVER_PORT_DEF));
    tcp_server_ptr = &tcp_server;

    // start an UdpServer
    UdpServer udp_server;
    if (Config::get_bool(CFG_UDP_SERVER_ENABLED, CFG_UDP_SERVER_ENABLED_DEF))
    {
        udp_server.start(Config::get_int(CFG_UDP_SERVER_PORT, CFG_UDP_SERVER_PORT_DEF));
        udp_server_ptr = &udp_server;
    }

    std::signal(SIGINT, intr_handler);
    std::signal(SIGTERM, intr_handler);
    std::signal(SIGABRT, intr_handler);

    // wait for HTTP server to stop
    http_server.wait(0);
    http_server.close_conns();

    tcp_server.shutdown();
    tcp_server.wait(0);

    udp_server.shutdown();

    shutdown();

    return 0;
}
