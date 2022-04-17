#!/usr/bin/env python

import optparse
import re
import requests
import shlex
import signal
import socket
import sys
import time
import yaml


dry_run = False
stop_requested = False


class Config(object):

    def __init__(self, conf_file):
        with open(conf_file, "r") as stream:
            try:
                self._config = yaml.safe_load(stream)
            except yaml.YAMLError as err:
                print(err)

    def get_jobs(self):
        jobs = []
        global_configs = self._config['global']
        external_labels = global_configs['external_labels']
        scrape_interval = get_time_in_seconds(global_configs['scrape_interval'])
        scrape_configs = self._config['scrape_configs']
        for scrape_config in scrape_configs:
            if not 'static_configs' in scrape_config:
                continue    # do not support dynamic discovery yet
            job_name = None
            if 'job_name' in scrape_config:
                job_name = scrape_config['job_name']
            metrics_path = "/metrics"
            if 'metrics_path' in scrape_config:
                metrics_path = scrape_config['metrics_path']
            interval = scrape_interval
            if 'scrape_interval' in scrape_config:
                interval = scrape_config['scrape_interval']
            static_configs = scrape_config['static_configs']
            for static_config in static_configs:
                if not 'targets' in static_config:
                    continue
                targets = static_config['targets']
                jobs.append(Job(name=job_name, path=metrics_path, interval=interval, targets=targets))
        return jobs

    def get_ticktock_addr(self):
        global_configs = self._config['global']
        ticktock = global_configs['ticktock']
        address = ticktock['address']
        tokens = address.split(':')
        return (tokens[0], int(tokens[1]))


class Job(object):

    def __init__(self, name, path, interval, targets):
        self._name = name
        self._path = path
        self._scheme = 'http'
        self._scrape_interval = interval    # seconds
        self._targets = targets
        self._last_scrape = time.time()

    def is_time_to_scrape(self, now):
        if (now - self._last_scrape) >= self._scrape_interval:
            self._last_scrape = now
            return True
        else:
            return False

    def get_metrics_path(self):
        return self._path

    def get_scheme(self):
        return self._scheme

    def get_targets(self):
        return self._targets


class Scraper(object):

    def __init__(self, config):
        self._config = config
        self._ticktock = TickTock(config)
        self._ticktock.connect()

    def scrape_loop(self):
        jobs = self._config.get_jobs()
        while not stop_requested:
            scraped = False
            now = time.time()
            for job in jobs:
                if job.is_time_to_scrape(now):
                    scraped = True
                    self.scrape(now, job)
                if stop_requested:
                    break
            if not scraped:
                time.sleep(1)
        self._ticktock.close()

    def scrape(self, now, job):
        path = job.get_metrics_path()
        scheme = job.get_scheme()
        for target in job.get_targets():
            response = requests.get(scheme + "://" + target + path, timeout=10)
            self._ticktock.send(round(now), target, response.text)


class TickTock(object):

    def __init__(self, config):
        self._config = config
        self._address = config.get_ticktock_addr()
        self._histo_sum = set()
        self._sum = {}
        self._count = {}
        self._sock = None
        self._pattern = re.compile('^([a-zA-Z_:][a-zA-Z0-9_:]*)({.+})* ([0-9\.eE\+\-]+)')

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect(self._address)

    def close(self):
        if self._sock:
            self._sock.close()

    def send(self, ts, target, msgs):
        host = target.split(':')[0]
        puts = ""
        lines = msgs.splitlines()
        for line in lines:
            if line.startswith("#"):
                if line.startswith("# TYPE "):
                    tokens = line[7:].split()
                    if tokens[1] == "histogram" or tokens[1] == "summary":
                        self._histo_sum.add(tokens[0])
                continue
            m = self._pattern.search(line)
            if m.group(0) is None:
                print "Unrecognized format: {}".format(line)
                continue
            metric = m.group(1)
            labels = m.group(2)
            value = float(m.group(3))
            if metric.endswith("_sum"):
                met = metric[:-4]
                if met in self._histo_sum:
                    if met in self._count:
                        cnt = self._count[met]
                        self._count.pop(met, None)
                        if int(cnt) > 0:
                            value /= float(cnt)
                            metric = met
                        else:
                            continue
                    else:
                        self._sum[met] = value
                        continue
            elif metric.endswith("_count"):
                met = metric[:-6]
                if met in self._histo_sum:
                    if met in self._sum:
                        s = float(self._sum[met])
                        self._sum.pop(met, None)
                        if int(value) > 0:
                            s /= float(value)
                            value = s
                            metric = met
                        else:
                            continue
                    else:
                        self._count[met] = value
                        continue
            elif metric.endswith("_bucket"):
                met = metric[:-7]
                if met in self._histo_sum:
                    continue
            elif metric.endswith("_info"):
                continue
            dp = "put {} {} {} host={}".format(metric, int(ts), value, host)
            if not labels is None:
                kvs = parse_key_value_pairs(labels[1:-1])
                for k, v in kvs.iteritems():
                    dp += " " + k + "=" + v.replace(' ','_').replace('=','_').replace(';','_')
            puts += dp + "\n"
        if puts != "":
            if dry_run:
                print puts
            else:
                try:
                    self._sock.sendall(puts)
                except:
                    print "failed to send to ticktock"


def sigint_handler(sig, frame):
    global stop_requested
    stop_requested = True


def get_time_in_seconds(time_str):
    time_sec = int(time_str[:-1])
    if time_str[-1] == 'm':
        time_sec *= 60
    elif time_str[-1] == 'h':
        time_sec *= 3600
    elif time_str[-1] == 'd':
        time_sec *= 24 * 3600
    return time_sec


def parse_key_value_pairs(kvs_str):
    pairs = {}
    lexer = shlex.shlex(kvs_str.encode('utf-8'), posix=True)
    lexer.whitespace = ','
    lexer.wordchars += "+_="
    for kv in lexer:
        pair = str(kv).split('=', 1)
        assert(len(pair) == 2)
        if pair[1]:
            pairs[pair[0]] = pair[1]
    return pairs


def main(argv):

    global dry_run

    try:
        options, args = get_options(argv)
        dry_run = options.dryrun
    except:
        sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])
        return 1

    signal.signal(signal.SIGINT, sigint_handler)

    config = Config(options.config)
    scraper = Scraper(config)
    scraper.scrape_loop()
    return 0


def get_options(argv):

    defaults = get_defaults()
    parser = optparse.OptionParser(description='Prometheus scraper')

    parser.add_option('-c', '--config', dest='config',
                      default=defaults['config'],
                      help='Configuration file')
    parser.add_option('-d', '--dryrun', dest='dryrun', action='store_true',
                      default=defaults['dryrun'],
                      help='Print metrics to stdout instead of sending them to TickTock')
    parser.add_option('-t', '--ticktock', dest='ticktock',
                      default=defaults['ticktock'],
                      help='TickTock server address, including port')

    (options, args) = parser.parse_args(args=argv[1:])
    return options, args


def get_defaults():

    defaults = {
        'config': 'conf/prom_config.yml',
        'dryrun': False,
        'ticktock': '127.0.0.1:6181'
    }

    return defaults;


if __name__ == '__main__':
    sys.exit(main(sys.argv))
