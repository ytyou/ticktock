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

#include <atomic>
#include <csignal>
#include <fstream>
#include <memory>
#include <random>
#include <limits.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <execinfo.h>
#include <fcntl.h>
#include <getopt.h>
#ifdef __GLIBC__
#include <gnu/libc-version.h>
#endif
#include <unistd.h>
#include "admin.h"
#include "compress.h"
#include "config.h"
#include "append.h"
#include "fd.h"
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


static void print_stack_trace()
{
    void *array[32];
    size_t size = backtrace(array, sizeof(array)/sizeof(array[0]));
    backtrace_symbols_fd(array, size, STDERR_FILENO);
}

static void
intr_handler(int sig)
{
    if (g_shutdown_requested) return;

    g_handler_thread_id = std::this_thread::get_id();   // don't wait for me
    g_shutdown_requested = true;

    if (sig != SIGINT)
        print_stack_trace();

    if (! g_quiet) printf("Interrupted (%d), shutting down...\n", sig);
    Logger::info("Interrupted (%d), shutting down...", sig);

    if (http_server_ptr != nullptr)
        http_server_ptr->shutdown();
}

static void
terminate_handler()
{
    if (g_shutdown_requested) return;

    g_handler_thread_id = std::this_thread::get_id();   // don't wait for me
    g_shutdown_requested = true;

    std::exception_ptr exptr = std::current_exception();

    if (exptr != nullptr)
    {
        try
        {
            std::rethrow_exception(exptr);
        }
        catch (std::exception &ex)
        {
            if (! g_quiet) printf("Uncaught exception: %s\n", ex.what());
        }
        catch (...)
        {
            if (! g_quiet) printf("Unknown exception\n");
        }
    }
    else
    {
        if (! g_quiet) printf("Unknown exception\n");
    }

    print_stack_trace();
}

}


using namespace tt;

static int
process_cmdline_opts(int argc, char *argv[])
{
    int c;
    int digit_optind = 0;
    const char *optstring = "c:dl:p:qr";
    static struct option long_options[] =
    {
        // ALL options should require argument
        { CFG_APPEND_LOG_ENABLED,                   required_argument,  0,  0 },
        { CFG_APPEND_LOG_FLUSH_FREQUENCY,           required_argument,  0,  0 },
        { CFG_HTTP_LISTENER_COUNT,                  required_argument,  0,  0 },
        { CFG_HTTP_RESPONDERS_PER_LISTENER,         required_argument,  0,  0 },
        { CFG_HTTP_SERVER_PORT,                     required_argument,  0,  0 },
        { CFG_LOG_FILE,                             required_argument,  0,  0 },
        { CFG_LOG_LEVEL,                            required_argument,  0,  0 },
        { CFG_LOG_RETENTION_COUNT,                  required_argument,  0,  0 },
        { CFG_LOG_ROTATION_SIZE,                    required_argument,  0,  0 },
        { CFG_STATS_FREQUENCY,                      required_argument,  0,  0 },
        { CFG_TCP_LISTENER_COUNT,                   required_argument,  0,  0 },
        { CFG_TCP_BUFFER_SIZE,                      required_argument,  0,  0 },
        { CFG_TCP_RESPONDERS_PER_LISTENER,          required_argument,  0,  0 },
        { CFG_TCP_RESPONDERS_QUEUE_SIZE,            required_argument,  0,  0 },
        { CFG_TCP_SERVER_PORT,                      required_argument,  0,  0 },
        { CFG_TICKTOCK_HOME,                        required_argument,  0,  0 },
        { CFG_TSDB_ARCHIVE_THRESHOLD,               required_argument,  0,  0 },
        //{ CFG_TSDB_COMPACT_FREQUENCY,               required_argument,  0,  0 },
        { CFG_TSDB_COMPRESSOR_VERSION,              required_argument,  0,  0 },
        { CFG_TSDB_DATA_DIR,                        required_argument,  0,  0 },
        { CFG_TSDB_FLUSH_FREQUENCY,                 required_argument,  0,  0 },
        { CFG_TSDB_GC_FREQUENCY,                    required_argument,  0,  0 },
        { CFG_TSDB_PAGE_SIZE,                       required_argument,  0,  0 },
        { CFG_TSDB_READ_ONLY_THRESHOLD,             required_argument,  0,  0 },
        { CFG_TSDB_RETENTION_THRESHOLD,             required_argument,  0,  0 },
        { CFG_TSDB_SELF_METER_ENABLED,              required_argument,  0,  0 },
        { CFG_TSDB_THRASHING_THRESHOLD,             required_argument,  0,  0 },
        { CFG_TSDB_TIMESTAMP_RESOLUTION,            required_argument,  0,  0 },
        { CFG_UDP_LISTENER_COUNT,                   required_argument,  0,  0 },
        { CFG_UDP_BATCH_SIZE,                       required_argument,  0,  0 },
        { CFG_UDP_SERVER_ENABLED,                   required_argument,  0,  0 },
        { CFG_UDP_SERVER_PORT,                      required_argument,  0,  0 },
        {0, 0, 0, 0},
    };

    while (1)
    {
        int this_option_optind = optind ? optind : 1;
        int option_index = 0;

        c = getopt_long(argc, argv, optstring, long_options, &option_index);
        if (c == -1) break;

        switch (c)
        {
            case 0:
                ASSERT(optarg != nullptr);
                Config::add_override(long_options[option_index].name, optarg);
                break;

            case 'c':
                g_config_file = optarg;     // config file
                break;

            case 'd':
                g_run_as_daemon = true;     // run in daemon mode
                break;

            case 'p':
                g_pid_file.assign(optarg);
                break;

            case 'q':
                g_quiet = true;             // quiet mode
                break;

            case 'r':
                g_opt_reuse_port = true;    // reuse port
                break;

            case '?':
                if (optopt == 'c')
                {
                    if (! g_quiet) fprintf(stderr, "Option -c requires an config file.\n");
                }
                else if (optopt == 'l')
                {
                    if (! g_quiet) fprintf(stderr, "Option -l requires a log level.\n");
                }
                else
                {
                    if (! g_quiet) fprintf(stderr, "Unknown option: '\\x%x'.\n", optopt);
                }
                return 1;
            default:
                return 2;
        }
    }

    if ((optind < argc) && ! g_quiet)
    {
        fprintf(stderr, "Unknown options that are ignored: ");
        while (optind < argc) fprintf(stderr, "%s ", argv[optind++]);
        fprintf(stderr, "\n");
    }

    return 0;
}

static void
daemonize()
{
    if ((daemon(1, 0) != 0) && ! g_quiet)
        fprintf(stderr, "daemon() failed: errno = %d\n", errno);
}

// one time initialization, at the beginning of the execution
static void
initialize()
{
    g_thread_id = "main";
    std::srand(std::time(0));

    // get our host name
    set_hostname_working_dir();

    if (g_run_as_daemon)
    {
        // get our working directory
        daemonize();
    }
    else if (! g_quiet)
    {
        printf(" TickTockDB v%d.%d.%d,  Maintained by\n"
               " Yongtao You (yongtao.you@gmail.com) and Yi Lin (ylin30@gmail.com).\n"
               " This program comes with ABSOLUTELY NO WARRANTY. It is free software,\n"
               " and you are welcome to redistribute it under certain conditions.\n"
               " For details, see <https://www.gnu.org/licenses/>.\n",
               TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION);
    }

    Config::init();
    FileDescriptorManager::init();

    // make sure folders exist
    std::string data_dir = Config::get_data_dir();
    std::string log_dir = Config::get_log_dir();
    create_dir(data_dir);
    create_dir(log_dir);

    Logger::init();
    auto pid = getpid();
    Logger::info("TickTockDB version: %d.%d.%d, on %s, pid: %d",
        TT_MAJOR_VERSION, TT_MINOR_VERSION, TT_PATCH_VERSION, g_host_name.c_str(), pid);
    try
    {
        // write to pid file
        std::ofstream pidfile(g_pid_file);
        pidfile << pid << std::endl;
        pidfile.close();
    }
    catch (...)
    {
        Logger::warn("Failed to write own pid to file %s", g_pid_file.c_str());
    }
#ifdef __GLIBC__
    Logger::info("GNU libc compile-time version: %u.%u", __GLIBC__, __GLIBC_MINOR__);
    Logger::info("GNU libc runtime version: %s", gnu_get_libc_version());
#endif
    Tag_v2::init();
    MemoryManager::init();
    Compressor::initialize();
    Tsdb::init();
    RollupManager::init();
    AppendLog::init();
    Stats::init();
    Admin::init();
    Timer::inst()->start();

    unsigned int n = std::thread::hardware_concurrency();
    Logger::info("std::thread::hardware_concurrency() = %d", n);
    Logger::info("sizeof(std::pair<Timestamp,double>) = %d", sizeof(std::pair<Timestamp,double>));
    Logger::info("sizeof(struct page_info_on_disk) = %d", sizeof(struct page_info_on_disk));
    Logger::info("page-size = %d", g_page_size);
    Logger::info("sys-page-size = %d", sysconf(_SC_PAGE_SIZE));
    Logger::info("Using config file: %s", g_config_file.c_str());
    Logger::info("Timestamp resolution: %s", (g_tstamp_resolution_ms ? "millisecond" : "second"));
    Logger::info("Timezone: %s", g_timezone.c_str());
    if (g_run_as_daemon)
        Logger::info("Running TickTockDB as daemon");
}

static void
shutdown()
{
    g_shutdown_requested = true;

    LD_STATS("Before shutdown");
    if (! g_quiet) printf("Start shutdown process...\n");
    Logger::info("Start shutdown process...");

    try
    {
        Timer::inst()->stop();
        Tsdb::shutdown();
        RollupManager::shutdown();
        // MM are thread-local singletons, and can't be cleaned up from another thread
        //MemoryManager::cleanup();
        AppendLog::shutdown();  // normal shutdown
        Stats::cleanup();
    }
    catch (std::exception& ex)
    {
        Logger::warn("caught exception when shutting down: %s", ex.what());
        if (! g_quiet) fprintf(stderr, "caught exception when shutting down: %s\n", ex.what());
    }

    Logger::info("Shutdown process complete\n\n");
    LD_STATS("After shutdown");
    Logger::inst()->close();
    if (! g_quiet) printf("Shutdown process complete\n");
}

int
main(int argc, char *argv[])
{
#ifdef _DEBUG
    std::signal(SIGFPE, segv_handler);
    std::signal(SIGKILL, segv_handler);
    std::signal(SIGSEGV, segv_handler);
#else
    std::signal(SIGFPE, intr_handler);
    std::signal(SIGKILL, intr_handler);
    std::signal(SIGSEGV, intr_handler);
#endif

    if (process_cmdline_opts(argc, argv) != 0) return 1;

    try
    {
        initialize();
    }
    catch (std::exception &ex)
    {
        if (! g_quiet) fprintf(stderr, "Initialization failed: %s\n", ex.what());
        return 9;
    }
    catch (...)
    {
        if (! g_quiet) fprintf(stderr, "Initialization failed. Abort!\n");
        return 9;
    }

    // verify configs
    if (Config::inst()->get_bool(CFG_UDP_SERVER_ENABLED, CFG_UDP_SERVER_ENABLED_DEF) &&
        (Config::inst()->get_int(CFG_UDP_SERVER_PORT, CFG_UDP_SERVER_PORT_DEF) <= 0))
    {
        Logger::fatal("UDP Server port must be greater than 0 (instead of %d)",
            Config::inst()->get_int(CFG_UDP_SERVER_PORT, CFG_UDP_SERVER_PORT_DEF));
        shutdown();
        return 1;
    }

    // start an HttpServer
    HttpServer http_server;
    http_server.init();
    http_server.start(Config::inst()->get_str(CFG_HTTP_SERVER_PORT, CFG_HTTP_SERVER_PORT_DEF));
    http_server_ptr = &http_server;

    // start a TcpServer
    TcpServer tcp_server;
    tcp_server.init();
    if (Config::inst()->get_bool(CFG_TCP_SERVER_ENABLED, CFG_TCP_SERVER_ENABLED_DEF))
    {
        tcp_server.start(Config::inst()->get_str(CFG_TCP_SERVER_PORT, CFG_TCP_SERVER_PORT_DEF));
        tcp_server_ptr = &tcp_server;
    }

    // start an UdpServer
    UdpServer udp_server;
    if (Config::inst()->get_bool(CFG_UDP_SERVER_ENABLED, CFG_UDP_SERVER_ENABLED_DEF))
    {
        udp_server.start(Config::inst()->get_int(CFG_UDP_SERVER_PORT, CFG_UDP_SERVER_PORT_DEF));
        udp_server_ptr = &udp_server;
    }

    std::signal(SIGINT, intr_handler);
    std::signal(SIGTERM, intr_handler);
    std::signal(SIGABRT, intr_handler);
    std::signal(SIGBUS, intr_handler);
    std::set_terminate(terminate_handler);

    if (! g_run_as_daemon && ! g_quiet)
        printf("TickTockDB is ready...\n");

    // wait for HTTP server to stop
    http_server.wait(0);
    http_server.close_conns();

    tcp_server.shutdown();
    tcp_server.wait(0);

    udp_server.shutdown();

    shutdown();
    delete Config::inst();

    return 0;
}
