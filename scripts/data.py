#!/usr/bin/env python

import cStringIO
import optparse
import random
import socket
import sys
import threading
import time


def get_defaults():

    defaults = {
        'duration': '1hour',
        'hosts': 10,
        'increment': '10second',
        'protocol': 'opentsdb',
        'threads': 10,
        'ticktock': 'localhost'
    }

    return defaults


def get_options(argv):

    try:
        defaults = get_defaults()
    except:
        sys.stderr.write("Command-line default option error: %s\n" % sys.exc_info()[0])
        raise

    parser = optparse.OptionParser(description='Data Generator for TickTockDB.')

    parser.add_option('-d', '--duration', dest='duration',
                      default=defaults['duration'],
                      help='Generate data for this duration (e.g. 6month, 1year).')
    parser.add_option('-H', '--hosts', dest='hosts',
                      default=defaults['hosts'],
                      help='Number of hosts.')
    parser.add_option('-i', '--increment', dest='increment',
                      default=defaults['increment'],
                      help='Step between data points, in seconds.')
    parser.add_option('-p', '--protocol', dest='protocol',
                      default=defaults['protocol'],
                      help='Either opentsdb telnet, or influxdb line protocol.')
    parser.add_option('-t', '--ticktock', dest='ticktock',
                      default=defaults['ticktock'],
                      help='TickTockDB host name/ip.')
    parser.add_option('-T', '--threads', dest='threads',
                      default=defaults['threads'],
                      help='Number of threads to use.')

    (options, args) = parser.parse_args(args=argv[1:])

    return options, args


def time_in_seconds(arg):

    factor = 1

    if arg.endswith("second"):
        arg = arg[:-6]
    elif arg.endswith("minute"):
        factor *= 60
        arg = arg[:-6]
    elif arg.endswith("hour"):
        factor *= 3600
        arg = arg[:-4]
    elif arg.endswith("day"):
        factor *= 24 * 3600
        arg = arg[:-3]
    elif arg.endswith("week"):
        factor *= 7 * 24 * 3600
        arg = arg[:-4]
    elif arg.endswith("month"):
        factor *= 30 * 24 * 3600
        arg = arg[:-5]
    elif arg.endswith("year"):
        factor *= 365 * 24 * 3600
        arg = arg[:-4]

    return int(arg) * factor


def thread_target(ticktock, id, hosts, metrics, steps, ts, increment):

    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_socket.connect(ticktock)

    buff = cStringIO.StringIO()

    for i in range(steps):
        for h in range(hosts):
            host = "host-{:02d}-{:06d}".format(id, h)
            buff.truncate(0)
            offset = random.randint(0,1000)
            if random.randint(0,1) == 1:
                offset = -offset
            for line in metrics:
                tokens = line.split()
                if len(tokens) < 2:
                    buff.write("put {} {} {} host={}\n".format(tokens[0], ts+offset, random.randint(0,100), host))
                else:
                    buff.write("put {} {} {} {} host={}\n".format(tokens[0], ts+offset, random.randint(0,100), tokens[1], host))
            #print("{}".format(buff.getvalue()))
            tcp_socket.sendall(buff.getvalue())
        ts += increment

    buff.close()
    tcp_socket.close()


def main(argv):

    try:
        options, args = get_options(argv)
    except:
        #sys.stderr.write("Command-line option error: %s\n" % sys.exc_info()[0])
        sys.exit(1)

    # get duration, in secs
    duration = time_in_seconds(options.duration)
    increment = time_in_seconds(options.increment)
    print "duration = {} secs; hosts = {}; increment = {} secs".format(duration, options.hosts, increment)
    steps = duration / increment

    ts = 1000 * (int(time.time()) - int(duration))
    increment *= 1000

    meta = open('/var/share/metrics.txt', 'r')
    lines = meta.readlines()

    #tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = (str(options.ticktock), 6181)
    #tcp_socket.connect(address)

    start = time.time()
    threads = []
    num_hosts = int(options.hosts)
    num_threads = int(options.threads)

    for i in range(num_threads):
        thread = threading.Thread(target=thread_target, args=(address, i, num_hosts/num_threads, lines, steps, ts, increment))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

        #if i % 100 == 0:
            #sys.stdout.write("\b\b\b\b\b\b\b\b{:.2f}%".format(100.0*float(i)/float(steps)))
            #sys.stdout.flush()

    #tcp_socket.close()
    end = time.time()

    print "\ntime took: {}".format(time.strftime("%H:%M:%S", time.gmtime(end-start))),

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
