#!/usr/bin/env python

import copy
import datetime
import json
import optparse
import os
import random
import requests
import shutil
import socket
import subprocess
import sys
import threading
import time

from collections import deque



class TickTockConfig(object):

    def __init__(self, options):
        self._options = options
        self._dict = {}

        sec_from_start = int(round(time.time())) - int(options.start/1000)

        # setup default config
        self._dict["append.log.dir"] = os.path.join(self._options.root,"append")
        self._dict["append.log.enabled"] = "true"
        self._dict["append.log.rotation.sec"] = 30

        self._dict["http.listener.count"] = 1
        self._dict["http.max.connections.per.listener"] = 1024
        self._dict["http.max.request.size.kb"] = 1024
        self._dict["http.responders.per.listener"] = 1
        self._dict["http.request.format"] = self._options.form
        self._dict["http.server.port"] = self._options.port
        self._dict["tcp.server.port"] = self._options.dataport
        self._dict["udp.server.port"] = self._options.dataport
        self._dict["udp.server.enabled"] = "true"

        self._dict["log.level"] = "INFO"
        self._dict["log.file"] = os.path.join(self._options.root,"tt.log")

        self._dict["query.executor.parallel"] = "false"

        self._dict["stats.frequency.sec"] = 30

        self._dict["sanity.check.frequency.sec"] = 3600

        self._dict["tsdb.archive.hour"] = (sec_from_start / 3600) + 2
        self._dict["tsdb.compressor.version"] = 1
        self._dict["tsdb.data.dir"] = os.path.join(self._options.root,"data")
        self._dict["tsdb.page.count"] = 128
        self._dict["tsdb.partition.sec"] = 120
        self._dict["tsdb.read_only.min"] = (sec_from_start / 60) + 10
        self._dict["tsdb.rotation.frequency.sec"] = 30
        self._dict["tsdb.timestamp.resolution"] = "millisecond"

    def add_entry(self, key, value):
        self._dict[key] = value

    def __call__(self, conf_file="tt.conf"):
        filename = os.path.join(self._options.root,conf_file)
        sys.stdout.write("generating ticktock config: %s\n" % filename)
        with open(filename, "w") as f:
            for k,v in self._dict.iteritems():
                f.write("%s = %s\n" % (k, v))


class DataPoint(object):

    def __init__(self, metric, timestamp, value, tags=None):
        self._metric = metric
        self._timestamp = timestamp
        self._value = value
        self._tags = tags

    def get_metric(self):
        return self._metric

    def get_timestamp(self):
        return self._timestamp

    def get_value(self):
        return self._value

    def get_tags(self):
        return self._tags

    def set_timestamp(self, tstamp):
        self._timestamp = tstamp

    def set_value(self, value):
        self._value = value

    def to_json(self):
        j = {"metric":self._metric,"timestamp":self._timestamp,"value":self._value}
        if self._tags:
            j["tags"] = self._tags
        else:
            j["tags"] = {}
        return j

    def to_plain(self):
        p = "{} {} {:.12f}".format(self._metric, self._timestamp, self._value)
        if self._tags:
            for k,v in self._tags.items():
                p += " " + k + "=" + v
        return p

    def to_tuple(self):
        return (self._timestamp, self._value)


class DataPoints(object):

    def __init__(self, prefix, start, interval_ms=5000, metric_count=2, metric_cardinality=2, tag_cardinality=2, out_of_order=False):

        self._dps = []
        self._start = start
        self._end = 0

        if metric_count == 0:
            return

        tstamp = self._start

        # OpenTSDB requirements:
        #   A data point must have at least one tag and every time series
        #   for a metric should have the same number of tags.
        # generate random dps
        for i in range(metric_count):
            if out_of_order:
                tstamp = tstamp + random.randint(-interval_ms, interval_ms)
            else:
                tstamp = tstamp + random.randint(1, interval_ms)
            for m in range(1, metric_cardinality+1):
                tags = {}
                for t in range(1, m+1):
                    if interval_ms < 200:   # in this case we need perfect data to avoid interpolation
                        tags["tag"+str(t)] = "val"+str(t)
                    else:
                        tags["tag"+str(t)] = "val"+str(random.randint(1,tag_cardinality))
                dp = DataPoint(prefix+"_metric_"+str(m), tstamp, random.uniform(0,100), tags)
                self._dps.append(dp)
                tstamp = tstamp + random.randint(0, 1000)

        self._end = tstamp + 1;

    def add_dp(self, dp):
        self._dps.append(dp)
        if self._start > dp.get_timestamp():
            self._start = dp.get_timestamp()
        if self._end < dp.get_timestamp():
            self._end = dp.get_timestamp()

    def get_dps(self):
        return self._dps

    def update_values(self):
        for dp in self._dps:
            dp.set_value(dp.get_value() + random.randint(1,10))

    def to_json(self):
        arr = []
        for dp in self._dps:
            arr.append(dp.to_json())
        return {"metrics": arr}

    def to_plain(self):
        p = ""
        for dp in self._dps:
            p += "put " + dp.to_plain() + "\n"
        return p + "\n"


class Query(object):

    def __init__(self, metric, start, end=None, tags=None, aggregator=None, downsampler=None, rate_options=None):
        self._metric = metric
        self._start = start
        self._end = end
        self._tags = tags
        self._aggregator = aggregator
        self._downsampler = downsampler
        self._rate_options = rate_options

        if not self._end:
            self._end = int(round(time.time() * 1000))

        # aggregator is required by OpenTSDB
        if not aggregator:
            self._aggregator = "none"

    def get_tag_value(self, key):
        if not self._tags:
            return None
        for k,v in self._tags.items():
            if k == key:
                return v
        return None

    def to_json(self):
        arr = []
        q = {}
        q["metric"] = self._metric
        if self._tags:
            q["tags"] = self._tags
        if self._aggregator and self._aggregator != "raw":
            q["aggregator"] = self._aggregator
        if self._downsampler:
            q["downsample"] = self._downsampler
        if self._rate_options:
            q["rate"] = 'true'
            q["rateOptions"] = self._rate_options
        arr.append(q)
        query = {}
        query["start"] = self._start
        query["end"] = self._end
        query["msResolution"] = "true"  # using True will cause opentsdb to choke
        query["globalAnnotations"] = "true"
        query["queries"] = arr
        return query

    def to_params(self):
        params = {}
        params["start"] = self._start
        params["end"] = self._end
        params["msResolution"] = "true"
        m = str(self._aggregator)
        if self._downsampler:
            m = m + ":" + str(self._downsampler)
        m = m + ":" + self._metric
        if self._tags:
            m = m + str(json.dumps(self._tags)).replace(" ", "").replace(":","=")
        params["m"] = m
        return params


class Test(object):

    def __init__(self, options, prefix, tcp_socket=None):
        self._options = options
        self._prefix = prefix
        self._passed = 0
        self._failed = 0
        self._opentsdb_time = 0.0
        self._ticktock_time = 0.0
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tcp_socket = tcp_socket

    def start_tt(self, conf_file="tt.conf"):
        while True:
            self._tt = subprocess.Popen([self._options.tt, "-c", os.path.join(self._options.root,conf_file), "-q", "-r"])
            time.sleep(4)
            if not self.tt_stopped():
                break
        print "tt started"

    def stop_tt(self, port=0):
        if port == 0:
            port = self._options.port
        if self._tcp_socket:
            self._tcp_socket.close()
            self._tcp_socket = None
        time.sleep(1)
        response = requests.post("http://"+self._options.ip+":"+str(port)+"/api/admin?cmd=stop")
        response.raise_for_status()

    def tt_stopped(self):
        self._tt.poll()
        return self._tt.returncode is not None

    def wait_for_tt(self, timeout):
        elapse = 0
        while not self.tt_stopped() and elapse <= timeout:
            time.sleep(1)
            elapse = elapse + 1

    def connect_to_tcp(self):
        if not self._tcp_socket:
            # establish tcp connection
            self._tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            address = (str(self._options.ip), int(self._options.dataport))
            print "connecting to {}:{}".format(self._options.ip, self._options.dataport)
            self._tcp_socket.connect(address)

    def get_checkpoint(self, leader=None):
        if leader:
            response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/admin?cmd=cp&leader="+leader)
        else:
            response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/admin?cmd=cp")
        response.raise_for_status()
        return response.json()

    def do_compaction(self):
        response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/admin?cmd=compact")
        response.raise_for_status()

    def do_rollup(self):
        response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/admin?cmd=rollup")
        response.raise_for_status()

    def metric_name(self, idx, prefix=None):
        if not prefix:
            prefix = self._prefix
        return prefix + "_metric_" + str(idx+1)

    def tag(self, k, v):
        return {"tag"+str(k): "val"+str(v)}

    def send_data_to_opentsdb(self, dps):
        payload = dps.to_json()
        if self._options.verbose:
            print "send_data(): " + json.dumps(payload)
        # send to opentsdb
        start = time.time()
        response = requests.post("http://"+self._options.opentsdbip+":"+str(self._options.opentsdbport)+"/api/put?details", json=payload["metrics"], timeout=self._options.timeout)
        self._opentsdb_time += time.time() - start
        response.raise_for_status()

    def send_data_to_ticktock(self, dps):
        if self._options.form == "json":
            payload = dps.to_json()
            # send to ticktock
            start = time.time()
            response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/put?details", json=payload, timeout=self._options.timeout)
            self._ticktock_time += time.time() - start
            response.raise_for_status()
        else:
            payload = dps.to_plain()
            # send to ticktock
            start = time.time()
            response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/put?details", data=payload, timeout=self._options.timeout)
            self._ticktock_time += time.time() - start
            response.raise_for_status()

    def send_data_to_ticktock_udp(self, dps):
        # send to ticktock
        start = time.time()
        for dp in dps.get_dps():
            #data = dp.to_plain().encode('ascii')
            self._udp_socket.sendto(dp.to_plain(), (self._options.ip, self._options.dataport))
        self._ticktock_time += time.time() - start

    def send_data_to_ticktock_tcp(self, dps):
        # send to ticktock
        start = time.time()
        if not self._tcp_socket:
            self.connect_to_tcp()
        self._tcp_socket.sendall(dps.to_plain())
        self._ticktock_time += time.time() - start

    def send_data(self, dps, wait=True):
        self.send_data_to_opentsdb(dps)
        self.send_data_to_ticktock(dps)
        # opentsdb needs time to get ready before query
        if wait:
            time.sleep(2)

    def send_data_plain(self, dps):
        self.send_data_to_opentsdb(dps)

        # send to ticktock in plain format
        payload = dps.to_plain()
        start = time.time()
        response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/put?details", data=payload, timeout=self._options.timeout)
        self._ticktock_time += time.time() - start
        response.raise_for_status()

        # opentsdb needs time to get ready before query
        time.sleep(2)

    def send_data_udp(self, dps):
        if self._options.verbose:
            payload = dps.to_json()
            print "send_data_udp(): " + json.dumps(payload)
        self.send_data_to_opentsdb(dps)
        self.send_data_to_ticktock_udp(dps)

        # opentsdb needs time to get ready before query
        time.sleep(2)

    def send_data_tcp(self, dps):
        if self._options.verbose:
            print "send_data_tcp(): " + dps.to_plain()
        self.send_data_to_opentsdb(dps)
        self.send_data_to_ticktock_tcp(dps)

        # opentsdb needs time to get ready before query
        time.sleep(2)

    def send_checkpoint(self, leader, channel, cp):
        # send cp to ticktock
        start = time.time()
        if not self._tcp_socket:
            self.connect_to_tcp()
        self._tcp_socket.sendall("cp {}:{}:{}\n".format(leader, channel, cp))
        self._ticktock_time += time.time() - start

    def query_ticktock(self, query):
        response = None

        if self._options.method == "post":
            payload = query.to_json()
            start = time.time()
            response = requests.post("http://"+self._options.ip+":"+str(self._options.port)+"/api/query", json=payload, timeout=self._options.timeout)
            self._ticktock_time += time.time() - start
        elif self._options.method == "get":
            params = query.to_params()
            if self._options.verbose:
                print "params = " + str(params)
            start = time.time()
            response = requests.get("http://"+self._options.ip+":"+str(self._options.port)+"/api/query", params=params, timeout=self._options.timeout)
            self._ticktock_time += time.time() - start
        else:
            raise Exception("unknown http method: " + str(self._options.method))

        if not response:
            raise Exception("failed to query tt")
        if self._options.verbose:
            print "ticktock-response: " + response.text
        response.raise_for_status()
        return response.json()

    def query_opentsdb(self, query):
        payload = query.to_json()
        start = time.time()
        response = requests.post("http://"+self._options.opentsdbip+":"+str(self._options.opentsdbport)+"/api/query", json=payload, timeout=self._options.timeout)
        self._opentsdb_time += time.time() - start
        #if self._options.verbose:
        #    print "opentsdb-response: " + response.text #str(response.status_code)
        try:
            response.raise_for_status()
            if self._options.verbose:
                print "opentsdb-response: " + response.text
        except requests.exceptions.HTTPError:
            if self._options.verbose:
                print "opentsdb-response: []"
            return []
        return response.json()

    # Sometimes OpenTsdb returns empty results from a sub-query;
    # That is, results with no data points; Since TickTock does not
    # return anything in this situation, we need to remove them
    # from OpenTsdb's results in order to match results from TickTock.
    def remove_empty_dps(self, arr):
        result = []
        if isinstance(arr, list):
            for d in arr:
                if isinstance(d, dict):
                    if d.has_key("dps"):
                        dps = d["dps"]
                        if len(dps) != 0:
                            result.append(d)
        return result

    def query_and_verify(self, query):
        if self._options.verbose:
            print "query: " + str(query.to_json())
        expected = self.query_opentsdb(query)
        actual = self.query_ticktock(query)
        expected2 = self.remove_empty_dps(expected)
        if self.verify_json(expected2, actual):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1
            print "[FAIL] query: " + str(query.to_json())

    def verify_json(self, expected, actual):
        if isinstance(expected, list):
            if not isinstance(actual, list):
                if self._options.verbose:
                    print "actual not list: " + str(actual)
                return False
            # When query returns no data points, newer version of OpenTSDB
            # now returns [{"metric":"metric1","tags":{"tag1":"val1"},"aggregateTags":[],"dps":{}}]
            # instead of just []. We will not change this behavior, yet. so...
            if not actual:
                if not expected:
                    return True
                if len(expected) != 1:
                    return False
                elem = expected[0]
                if not isinstance(elem, dict):
                    return False
                if not elem.has_key("dps"):
                    return False
                elem = elem["dps"]
                if not isinstance(elem, dict):
                    return False
                if elem:
                    return False
                return True
            if len(expected) != len(actual):
                if self._options.verbose:
                    print "list len not same: %s vs %s" % (str(expected), str(actual))
                return False
            if expected and actual:
                for i in range(len(expected)):
                    match = False
                    for j in range(len(expected)):
                        if self.verify_json(expected[i], actual[j]):
                            match = True
                            break
                        elif self._options.verbose:
                            print "expected != actual: %s vs %s" % (str(expected[i]), str(actual[j]))
                    if not match:
                        if self._options.verbose:
                            print "actual does not have: %s" % str(expected[i])
                        return False
                for i in range(len(expected)):
                    match = False
                    for j in range(len(expected)):
                        if self.verify_json(expected[j], actual[i]):
                            match = True
                            break
                    if not match:
                        if self._options.verbose:
                            print "expected does not have: %s" % str(actual[i])
                        return False
            return True
        elif isinstance(expected, dict):
            if not isinstance(actual, dict):
                if self._options.verbose:
                    print "actual not dict: " + str(actual)
                return False
            if len(expected) != len(actual):
                if self._options.verbose:
                    print "dict len not same: %s vs %s" % (str(expected), str(actual))
                return False
            if expected and actual:
                for k,v in expected.items():
                    if not actual.has_key(str(k)):
                        if self._options.verbose:
                            print "actual does not have: %s" % str(k)
                        return False
                    if not self.verify_json(v, actual[str(k)]):
                        if self._options.verbose:
                            print "key %s has diff values: %s vs %s" % (str(k), str(v), str(actual[str(k)]))
                        return False
                for k,v in actual.items():
                    if not expected.has_key(str(k)):
                        if self._options.verbose:
                            print "expected does not have: %s" % str(k)
                        return False
                    if not self.verify_json(v, expected[str(k)]):
                        return False
            return True
        elif isinstance(expected, float):
            if not isinstance(actual, float):
                print "actual not float: " + str(actual)
                return False
            diff = abs(expected - actual)
            if diff > 0.00000000012:
                if diff < 0.001:
                    print "expected not same as actual (float): {:.16f} vs {:.16f}; diff = {:.16f}".format(expected, actual, diff)
                return False
        elif isinstance(expected, int):
            if not isinstance(actual, int):
                if self._options.verbose:
                    print "actual not int: " + str(actual)
                return False
            if expected != actual:
                if self._options.verbose:
                    print "expected not same as actual (int): %s vs %s" % (str(expected), str(actual))
                return False
        elif isinstance(expected, str) or isinstance(expected, unicode):
            if not (isinstance(actual, str) or isinstance(actual, unicode)):
                if self._options.verbose:
                    print "actual not same str: %s" % str(actual)
                return False
            if expected != actual:
                if self._options.verbose:
                    print "expected not same as actual (str): %s vs %s" % (str(expected), str(actual))
                return False
        else:
            print("[FAIL] expected: %s, actual: %s" % (str(expected), str(actual)))
            return False

        return True

    def cleanup(self):
        sys.stdout.write("cleanup...\n")
        #os.system("pkill -f %s" % self._options.tt)
        if os.path.exists(self._options.root):
            shutil.rmtree(self._options.root)
        os.mkdir(self._options.root)
        os.mkdir(os.path.join(self._options.root,"log"))
        os.mkdir(os.path.join(self._options.root,"data"))
        os.mkdir(os.path.join(self._options.root,"data0"))
        os.mkdir(os.path.join(self._options.root,"data1"))
        os.mkdir(os.path.join(self._options.root,"append"))
        os.mkdir(os.path.join(self._options.root,"append0"))
        os.mkdir(os.path.join(self._options.root,"append1"))


class Compaction_Tests(Test):

    def __init__(self, options, prefix="compact"):
        super(Compaction_Tests, self).__init__(options, prefix)

    def __call__(self):

        config = TickTockConfig(self._options)
        config.add_entry("log.level", "TRACE")
        config()    # generate config

        self.start_tt()

        dps = DataPoints(self._prefix, self._options.start, metric_count=32, metric_cardinality=2, tag_cardinality=2)
        self.send_data(dps)

        # query every dp...
        for dp in dps.get_dps():
            query = Query(metric=dp.get_metric(), start=dp.get_timestamp(), end=dp.get_timestamp()+1, tags=dp.get_tags())
            self.query_and_verify(query)

        # shutdown and restart
        self.stop_tt()
        self.wait_for_tt(self._options.timeout)
        time.sleep(2)

        config()    # generate config
        self.start_tt()

        # query every dp...
        for dp in dps.get_dps():
            query = Query(metric=dp.get_metric(), start=dp.get_timestamp(), end=dp.get_timestamp()+1, tags=dp.get_tags())
            self.query_and_verify(query)

        # do compaction
        print "perform compaction..."
        self.do_compaction()

        # shutdown and restart
        self.stop_tt()
        self.wait_for_tt(self._options.timeout)
        time.sleep(2)

        config()    # generate config
        self.start_tt()

        # query every dp, again...
        for dp in dps.get_dps():
            query = Query(metric=dp.get_metric(), start=dp.get_timestamp(), end=dp.get_timestamp()+1, tags=dp.get_tags())
            self.query_and_verify(query)

        # shutdown and restart
        self.stop_tt()
        self.wait_for_tt(self._options.timeout)
        time.sleep(2)


class Multi_Thread_Tests(Test):

    def __init__(self, options, prefix="mt"):
        super(Multi_Thread_Tests, self).__init__(options, prefix)

    def __call__(self):

        config = TickTockConfig(self._options)
        config.add_entry("query.executor.parallel", "true");
        config.add_entry("http.listener.count", 2);
        config.add_entry("http.responders.per.listener", 3);
        config()    # generate config

        self.start_tt()

        thread_count = 5
        threads = []

        # create threads
        for tid in range(thread_count):
            threads.append(threading.Thread(target=self.thread_target, args=(tid,)))

        # start threads
        for tid in range(thread_count):
            threads[tid].start()

        # wait for all threads to finish
        for tid in range(thread_count):
            threads[tid].join()

        self.stop_tt()
        self.wait_for_tt(self._options.timeout)

    def thread_target(self, tid):

        metric_cardinality = 4
        tag_cardinality = 4
        prefix = "mt" + str(tid)

        dps = DataPoints(prefix, self._options.start, interval_ms=128, metric_count=256, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)
        self.send_data(dps)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "*"}
                    query = Query(metric=self.metric_name(m,prefix), start=self._options.start, end=dps._end, aggregator=agg, downsampler="10s-"+down, tags=tags)
                    self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "val1"}
                    query = Query(metric=self.metric_name(m,prefix), start=self._options.start+88888, end=dps._end+88888, aggregator=agg, downsampler="10s-"+down+"-zero")
                    self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "val*"}
                    query = Query(metric=self.metric_name(m,prefix), start=self._options.start-88888, end=dps._end+88888, aggregator=agg, downsampler="10s-"+down+"-zero")
                    self.query_and_verify(query)

        # special downsample interval: all
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "val1"}
                    query = Query(metric=self.metric_name(m,prefix), start=self._options.start+88888, end=dps._end+88888, aggregator=agg, downsampler="0all-"+down+"-zero")
                    self.query_and_verify(query)


class Out_Of_Order_Write_Tests(Test):

    def __init__(self, options, prefix="o"):
        super(Out_Of_Order_Write_Tests, self).__init__(options, prefix)

    def __call__(self):

        sec_from_start = int(round(time.time())) - int(self._options.start/1000)

        # these settings will make sure tsdb are loaded in archive mode
        config = TickTockConfig(self._options)
        config.add_entry("tsdb.archive.hour", int(sec_from_start/3600)-1);
        config.add_entry("tsdb.read_only.min", int(sec_from_start/60)-120);
        config()    # generate config

        self.start_tt()

        dps1 = DataPoints(self._prefix, self._options.start, metric_count=128, metric_cardinality=4, tag_cardinality=4)
        self.send_data(dps1)

        start = self._options.start - 60000
        dps2 = DataPoints(self._prefix, start, metric_count=128, metric_cardinality=4, tag_cardinality=4, out_of_order=True)
        self.send_data(dps2)

        start = start - 1;

        query1 = Query(metric=self.metric_name(1), start=start)
        self.query_and_verify(query1)

        query2 = Query(metric=self.metric_name(2), start=start)
        self.query_and_verify(query2)

        query3 = Query(metric=self.metric_name(3), start=start)
        self.query_and_verify(query3)

        query4 = Query(metric=self.metric_name(4), start=start)
        self.query_and_verify(query4)

        self.stop_tt()
        self.wait_for_tt(self._options.timeout)


class Stop_Restart_Tests(Test):

    def __init__(self, options, prefix="sr"):
        super(Stop_Restart_Tests, self).__init__(options, prefix)

    def __call__(self):

        config = TickTockConfig(self._options)
        config.add_entry("tsdb.compressor.version", 1);
        config()    # generate config

        self.start_tt()

        dps = DataPoints(self._prefix, self._options.start, interval_ms=128, metric_count=128, metric_cardinality=4, tag_cardinality=4, out_of_order=True)
        self.send_data(dps)

        tags1 = {"tag1":"val1"}
        tags2 = {"tag1":"val1", "tag2":"val2"}
        tags3 = {"tag2":"val1|val2"}

        iterations = 5

        for i in range(1, iterations+1):
            print "iteration {}".format(i)

            # add more dps
            dps2 = DataPoints(self._prefix, self._options.start+10*i, metric_count=0)
            dp = DataPoint(metric=self._prefix+"_metric_bug0", timestamp=self._options.start+100*i, value=i*100, tags=tags1)
            dps2.add_dp(dp)
            self.send_data(dps2)

            query1 = Query(metric=self.metric_name(2), start=self._options.start, end=dps._end, tags=tags1)
            self.query_and_verify(query1)

            query2 = Query(metric=self.metric_name(3), start=self._options.start, end=dps._end, tags=tags2)
            self.query_and_verify(query2)

            query3 = Query(metric=self.metric_name(4), start=self._options.start, end=dps._end, tags=tags3)
            self.query_and_verify(query3)

            self.stop_tt()
            self.wait_for_tt(self._options.timeout)

            if i < iterations:
                time.sleep(2)
                # restart with a different compressor version
                config = TickTockConfig(self._options)
                config.add_entry("tsdb.compressor.version", 2);
                config()    # generate config
                self.start_tt()
                # ticktock needs time to restore from disk
                time.sleep(4)

                # add more dps
                dps3 = DataPoints(self._prefix, self._options.start-10*i, metric_count=0)
                dp = DataPoint(metric=self._prefix+"_metric_bug0", timestamp=self._options.start-100*i, value=i, tags=tags1)
                dps3.add_dp(dp)
                self.send_data(dps3)

                query4 = Query(metric=self._prefix+"_metric_bug0", start=self._options.start-100*i, end=dps._end, tags=tags1, downsampler="10000h-last")
                self.query_and_verify(query4)


class Backfill_Tests(Test):

    def __init__(self, options, prefix="bf"):
        super(Backfill_Tests, self).__init__(options, prefix)

    def __call__(self):

        config = TickTockConfig(self._options)
        config.add_entry("log.level", "INFO")
        config.add_entry("log.file", "{}/tt.log".format(self._options.root))
        config.add_entry("http.request.format", "plain")
        config.add_entry("append.log.enabled", "true")
        config()    # generate config

        self.start_tt()

        dps = DataPoints(self._prefix, self._options.start, interval_ms=128, metric_count=256, metric_cardinality=4, tag_cardinality=4, out_of_order=True)
        self.send_data_plain(dps)

        tags1 = {"tag1":"val1"}
        tags2 = {"tag1":"val1", "tag2":"val2"}
        tags3 = {"tag2":"val1|val2"}

        for i in range(1, 3):
            query1 = Query(metric=self.metric_name(2), start=self._options.start, end=dps._end, tags=tags1)
            self.query_and_verify(query1)

            query2 = Query(metric=self.metric_name(3), start=self._options.start, end=dps._end, tags=tags2)
            self.query_and_verify(query2)

            query3 = Query(metric=self.metric_name(4), start=self._options.start, end=dps._end, tags=tags3)
            self.query_and_verify(query3)

            self.stop_tt()
            self.wait_for_tt(self._options.timeout)

            if i == 1:
                time.sleep(2)
                # save append logs somewhere else
                bak = "/tmp/tt_i.bak"
                if os.path.exists(bak):
                    shutil.rmtree(bak)
                os.mkdir(bak)
                os.system("cp " + self._options.root + "/append.*.log.zip " + bak)

                # do a clean restart
                self.cleanup()
                config = TickTockConfig(self._options)
                config.add_entry("log.file", "{}/tt.log".format(self._options.root));
                config.add_entry("log.level", "INFO");
                config()    # generate config
                self.start_tt()
                time.sleep(1)

                # perform backfill
                os.system("bin/backfill -a {} -h {} -p {}".format(bak, self._options.ip, self._options.port));


class Basic_Query_Tests(Test):

    def __init__(self, options, prefix="bq", tcp_socket=None):
        super(Basic_Query_Tests, self).__init__(options, prefix, tcp_socket)

    def __call__(self, metric_count=2, metric_cardinality=2, tag_cardinality=2, run_tt=True, via_udp=False, via_tcp=False):

        if run_tt:
            # generate config
            config = TickTockConfig(self._options)
            #config.add_entry("tsdb.read_only.sec", "20");
            config.add_entry("tsdb.page.size", "1024");
            config()    # generate config

            # start tt
            self.start_tt()

        # insert some dps
        dps = DataPoints(self._prefix, self._options.start, metric_count=metric_count, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        if via_udp:
            self.send_data_udp(dps)
        elif via_tcp:
            self.send_data_tcp(dps)
        else:
            self.send_data(dps)

        # this metric does not exist
        print "Try non-existing metric..."
        query = Query(metric=self.metric_name(0), start=self._options.start, end=dps._end)
        self.query_and_verify(query)

        # retrieve raw dps (no tags, no downsampling)
        print "Retrieving raw dps, no tags..."
        for m in range(1, metric_cardinality+1):
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end)
            self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        print "Retrieving raw dps, with tags..."
        for m in range(1, metric_cardinality+1):
            for tk in range(tag_cardinality):
                for tv in range(tag_cardinality):
                    tags = self.tag(tk, tv)
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                    self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        print "Retrieving raw dps, with tags..."
        for m in range(1, metric_cardinality+1):
            for tk in range(tag_cardinality):
                tags = {"tag"+str(tk): "*"}
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        if tag_cardinality > 2:
            print "Retrieving raw dps, with tags..."
            for m in range(1, metric_cardinality+1):
                for tk in range(2, tag_cardinality):
                    tags = {"tag1":"val1", "tag"+str(tk): "val*"}
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                    self.query_and_verify(query)

        if run_tt:
            # stop tt
            self.stop_tt()

            # make sure tt stopped
            self.wait_for_tt(self._options.timeout)


# Do not leave any missing points when generating test data for this test.
# We are doing downsamples without fill, and in such cases OpenTSDB will
# do linear interpolation, which we do not do, so our results will not be
# the same as that of OpenTSDB, and tests will fail.
class Advanced_Query_No_Fill_Tests(Test):

    def __init__(self, options, prefix="aq", tcp_socket=None):
        super(Advanced_Query_No_Fill_Tests, self).__init__(options, prefix, tcp_socket)

    def __call__(self, metric_count=2, metric_cardinality=2, tag_cardinality=2, run_tt=True, via_tcp=True):

        if run_tt:
            # generate config
            config = TickTockConfig(self._options)
            #config.add_entry("tsdb.read_only.sec", "20");
            config()    # generate config

            # start tt
            self.start_tt()

        # insert some dps
        dps = DataPoints(self._prefix, self._options.start, interval_ms=100, metric_count=metric_count, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        if via_tcp:
            self.send_data_tcp(dps)
        else:
            self.send_data(dps)

        print "Downsamples without fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, downsampler="10000ms-"+down)
                self.query_and_verify(query)

        print "Downsamples with zero fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="10000ms-"+down+"-zero")
                self.query_and_verify(query)

        print "Aggregates without downsample..."
        for m in range(metric_cardinality):
            tags = {"tag"+str(m): "*"}
            for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, aggregator=agg, tags=tags)
                self.query_and_verify(query)

        print "Aggregates, downsamples without fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "*"}
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, aggregator=agg, downsampler="10s-"+down, tags=tags)
                    self.query_and_verify(query)

        print "Aggregates, downsamples with zero fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="10s-"+down+"-zero")
                    self.query_and_verify(query)

        if run_tt:
            # stop tt
            self.stop_tt()

            # make sure tt stopped
            self.wait_for_tt(self._options.timeout)


# Downsamplers will always do zero-fill in this test, so we can generate
# test data with lots of missing points. OpenTSDB's linear interpolation
# will not kick in since due to the zero-fill performed by the downsampler.
class Advanced_Query_With_Fill_Tests(Test):

    def __init__(self, options, prefix="aq", tcp_socket=None):
        super(Advanced_Query_With_Fill_Tests, self).__init__(options, prefix, tcp_socket)

    def __call__(self, metric_count=2, metric_cardinality=2, tag_cardinality=2, run_tt=True, via_tcp=True):

        if run_tt:
            # generate config
            config = TickTockConfig(self._options)
            #config.add_entry("tsdb.read_only.sec", "20");
            config()    # generate config

            # start tt
            self.start_tt()

        # insert some dps
        dps = DataPoints(self._prefix, self._options.start, interval_ms=50000, metric_count=metric_count, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        if via_tcp:
            self.send_data_tcp(dps)
        else:
            self.send_data(dps)

        print "Downsamples with zero fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="10000ms-"+down+"-zero")
                self.query_and_verify(query)

        print "Aggregates, downsamples with zero fill..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="10s-"+down+"-zero")
                    self.query_and_verify(query)

        if run_tt:
            # stop tt
            self.stop_tt()

            # make sure tt stopped
            self.wait_for_tt(self._options.timeout)


class Advanced_Query_With_Aggregation(Test):

    def __init__(self, options, prefix="aq", tcp_socket=None):
        super(Advanced_Query_With_Aggregation, self).__init__(options, prefix, tcp_socket)

    def __call__(self, run_tt=False, via_tcp=False):

        if run_tt:
            # generate config
            config = TickTockConfig(self._options)
            #config.add_entry("tsdb.read_only.sec", "20");
            config()    # generate config

            # start tt
            self.start_tt()

        # insert some dps
        dps = DataPoints("notused", 0, metric_count=0)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag2"] = "val2"
        tags["tag3"] = "val3"
        dp = DataPoint(self._prefix+"_metric_agg", self._options.start, random.uniform(0,100), tags)
        dps.add_dp(dp)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag2"] = "val3"
        tags["tag3"] = "val2"
        dp = DataPoint(self._prefix+"_metric_agg", self._options.start, random.uniform(0,100), tags)
        dps.add_dp(dp)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag2"] = "val2"
        tags["tag3"] = "val24"
        dp = DataPoint(self._prefix+"_metric_agg", self._options.start, random.uniform(0,100), tags)
        dps.add_dp(dp)

        if via_tcp:
            self.send_data_tcp(dps)
        else:
            self.send_data(dps)
        time.sleep(2)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag2"] = "*"
        query = Query(metric=self._prefix+"_metric_agg", start=self._options.start, end=dps._end+99999, downsampler="1h-avg", aggregator="sum", tags=tags)
        self.query_and_verify(query)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag3"] = "val2|val24"
        query = Query(metric=self._prefix+"_metric_agg", start=self._options.start, end=dps._end+99999, downsampler="1h-avg", aggregator="sum", tags=tags)
        self.query_and_verify(query)

        tags = {}
        tags["tag1"] = "val1"
        tags["tag3"] = "val2*"
        query = Query(metric=self._prefix+"_metric_agg", start=self._options.start, end=dps._end+99999, downsampler="1h-avg", aggregator="sum", tags=tags)
        self.query_and_verify(query)

        if run_tt:
            # stop tt
            self.stop_tt()

            # make sure tt stopped
            self.wait_for_tt(self._options.timeout)


class Query_Tests(Test):

    def __init__(self, options, metric_count=2, metric_cardinality=2, tag_cardinality=2):

        super(Query_Tests, self).__init__(options, "q")

        self._metric_count = metric_count
        self._metric_cardinality = metric_cardinality
        self._tag_cardinality = tag_cardinality

    def __call__(self):

        conf = {}
        conf["query.executor.parallel"] = "false"
        self._options.method = "get"
        self.test_once("q1", conf)

        self.cleanup()

        # default TIME_WAIT is 60 seconds; make sure we wait it out
        #time.sleep(2)

        conf["query.executor.parallel"] = "true"
        conf["http.listener.count"] = 2
        conf["http.responders.per.listener"] = 2
        self._options.method = "post"
        self.test_once("q2", conf)

    def test_once(self, prefix, conf):

        # generate config
        config = TickTockConfig(self._options)
        for k,v in conf.items():
            config.add_entry(k, v);
        config()    # generate config

        self.start_tt()
        self.connect_to_tcp()

        print "Running Advanced_Query_With_Aggregation()..."
        test = Advanced_Query_With_Aggregation(self._options, prefix=prefix)
        test(run_tt=False)
        if test._failed > 0:
            print "[FAIL] Advanced_Query_With_Aggregation() failed"

        for metric_cnt in range(2, self._metric_count+1):
            for metric_card in range(2, self._metric_cardinality+1):
                for tag_card in range(2, self._tag_cardinality+1):

                    prefix1 = prefix + "_bq_%d_%d_%d" % (metric_cnt, metric_card, tag_card)
                    print "Running Basic_Query_Tests(%s)..." % prefix1
                    test = Basic_Query_Tests(self._options, prefix=prefix1)
                    test(metric_count=metric_cnt, metric_cardinality=metric_card, tag_cardinality=tag_card, run_tt=False, via_udp=False)
                    if test._failed > 0:
                        print "Basic_Query_Tests(metric_count=%d, metric_cardinality=%d, tag_cardinality=%d) failed" % (metric_cnt, metric_card, tag_card)
                    self._passed = self._passed + test._passed
                    self._failed = self._failed + test._failed

                    prefix2 = prefix + "_bqu_%d_%d_%d" % (metric_cnt, metric_card, tag_card)
                    print "Running Basic_Query_Tests(%s) with UDP..." % prefix2
                    test = Basic_Query_Tests(self._options, prefix=prefix2, tcp_socket=self._tcp_socket)
                    test(metric_count=metric_cnt, metric_cardinality=metric_card, tag_cardinality=tag_card, run_tt=False, via_udp=True)
                    if test._failed > 0:
                        print "Basic_Query_Tests(metric_count=%d, metric_cardinality=%d, tag_cardinality=%d) failed" % (metric_cnt, metric_card, tag_card)
                    self._passed = self._passed + test._passed
                    self._failed = self._failed + test._failed

                    prefix3 = prefix + "_aqnf_%d_%d_%d" % (metric_cnt, metric_card, tag_card)
                    print "Running Advanced_Query_No_Fill_Tests(%s)..." % prefix3
                    test = Advanced_Query_No_Fill_Tests(self._options, prefix=prefix3, tcp_socket=self._tcp_socket)
                    test(metric_count=metric_cnt, metric_cardinality=metric_card, tag_cardinality=tag_card, run_tt=False)
                    if test._failed > 0:
                        print "Advanced_Query_No_Fill_Tests(metric_count=%d, metric_cardinality=%d, tag_cardinality=%d) failed" % (metric_cnt, metric_card, tag_card)
                    self._passed = self._passed + test._passed
                    self._failed = self._failed + test._failed

                    prefix4 = prefix + "_aqwf_%d_%d_%d" % (metric_cnt, metric_card, tag_card)
                    print "Running Advanced_Query_With_Fill_Tests(%s)..." % prefix4
                    test = Advanced_Query_With_Fill_Tests(self._options, prefix=prefix4, tcp_socket=self._tcp_socket)
                    test(metric_count=metric_cnt, metric_cardinality=metric_card, tag_cardinality=tag_card, run_tt=False)
                    if test._failed > 0:
                        print "Advanced_Query_With_Fill_Tests(metric_count=%d, metric_cardinality=%d, tag_cardinality=%d) failed" % (metric_cnt, metric_card, tag_card)
                    self._passed = self._passed + test._passed
                    self._failed = self._failed + test._failed

        # stop tt
        self.stop_tt()

        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Query_With_Rollup(Test):

    def __init__(self, options, prefix="rq", tcp_socket=None):
        super(Query_With_Rollup, self).__init__(options, prefix, tcp_socket)

    def __call__(self, metric_cardinality=4):

        self.cleanup()

        # generate config
        config = TickTockConfig(self._options)
        config.add_entry("tcp.buffer.size", "1mb")
        config()    # generate config

        # start tt
        self.start_tt()

        # insert some dps
        print "insert data..."
        ts = self._options.start
        for i in range(20*24*60):
            dps = DataPoints(self._prefix, ts, interval_ms=60000, metric_count=1, metric_cardinality=metric_cardinality, tag_cardinality=2)
            self.send_data(dps, wait=False)
            ts += 60000
        time.sleep(5)

        # do rollup
        print "perform rollup..."
        for i in range(10):
            self.do_rollup()

        print "Downsamples with hourly rollup..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="2h-"+down)
                self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="2h-"+down+"-zero")
                    self.query_and_verify(query)

        print "Downsamples with daily rollup..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="1d-"+down+"-zero")
                self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="1d-"+down)
                    self.query_and_verify(query)

        # stop tt
        print "restarting TickTockDB..."
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)

        # restart tt
        self.start_tt()

        print "Downsamples with hourly rollup, after restart..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="2h-"+down)
                self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="2h-"+down+"-zero")
                    self.query_and_verify(query)

        print "Downsamples with daily rollup, after restart..."
        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="1d-"+down+"-zero")
                self.query_and_verify(query)

        for m in range(metric_cardinality):
            for down in ["avg", "count", "max", "min", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="1d-"+down)
                    self.query_and_verify(query)

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Rate_Tests(Test):

    def __init__(self, options, prefix="r"):
        super(Rate_Tests, self).__init__(options, prefix)

    def __call__(self):

        # generate config
        config = TickTockConfig(self._options)
        config.add_entry("tcp.buffer.size", "2mb")
        config()

        self.start_tt()

        metric_cardinality = 4

        dps = DataPoints(self._prefix, self._options.start, interval_ms=50000, metric_count=256, metric_cardinality=metric_cardinality, tag_cardinality=3)
        self.send_data(dps)

        rate_options = {"counter":'false'}
        for m in range(metric_cardinality):
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, rate_options=rate_options, downsampler="10s-"+down+"-zero")
                self.query_and_verify(query)

        # NOTE: we behave differently than OpenTSDB when aggregator is involved
        rate_options = {"counter":'false',"dropResets":'true'}
        for m in range(metric_cardinality):
            tags = {"tag"+str(m): "*"}
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, rate_options=rate_options, aggregator="none", tags=tags)
            self.query_and_verify(query)

        rate_options = {"counter":'true',"dropResets":'true'}
        for m in range(metric_cardinality):
            tags = {"tag"+str(m): "*"}
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, rate_options=rate_options, tags=tags)
            self.query_and_verify(query)

        rate_options = {"counter":'true',"dropResets":'true',"counterMax":200}
        for m in range(metric_cardinality):
            tags = {"tag"+str(m): "*"}
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, rate_options=rate_options, tags=tags)
            self.query_and_verify(query)

        # NOTE: we behave differently than OpenTSDB when aggregator is involved
        rate_options = {"counter":'true',"dropResets":'true',"counterMax":200,"resetValue":100}
        for m in range(metric_cardinality):
            tags = {"tag"+str(m): "*"}
            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, rate_options=rate_options, aggregator="none", downsampler="10s-"+down+"-zero", tags=tags)
                self.query_and_verify(query)

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Duplicate_Tests(Test):

    def __init__(self, options, prefix="dup"):
        super(Duplicate_Tests, self).__init__(options, prefix)

    def __call__(self):

        # generate config
        config = TickTockConfig(self._options)
        config()

        self.start_tt()
        print "Running Duplicate_Tests..."

        metric_cardinality = 4

        dps = DataPoints(self._prefix, self._options.start, interval_ms=50000, metric_count=256, metric_cardinality=metric_cardinality, tag_cardinality=3)

        self.send_data(dps)
        time.sleep(20)
        dps.update_values();    # change values to see which one will win
        self.send_data(dps)     # send again to create duplicates
        time.sleep(20)
        dps.update_values();    # change values to see which one will win
        self.send_data(dps)     # send again to create duplicates
        time.sleep(20)
        dps.update_values();    # change values to see which one will win
        self.send_data(dps)     # send again to create duplicates
        time.sleep(20)

        for m in range(metric_cardinality):
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, aggregator="none", tags={})
            self.query_and_verify(query)

            for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, aggregator=agg, downsampler="60s-"+down+"-zero")
                    self.query_and_verify(query)

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Replication_Test(Test):

    def __init__(self, options, idx, prefix="rep"):
        super(Replication_Test, self).__init__(options, prefix)
        self._idx = idx
        self._conf_file = "tt" + str(idx) + ".conf"

    def start(self):

        # generate config
        config = TickTockConfig(self._options)
        config.add_entry("append.log.dir", os.path.join(self._options.root,"append{}".format(self._idx)))
        config.add_entry("tsdb.data.dir", os.path.join(self._options.root,"data{}".format(self._idx)))
        config.add_entry("log.file", os.path.join(self._options.root,"tt{}.log".format(self._idx)))
        config.add_entry("http.server.port", self._options.port+int(self._idx)*1000)
        config.add_entry("tcp.server.port", self._options.dataport+int(self._idx)*1000)
        config.add_entry("udp.server.port", self._options.dataport+int(self._idx)*1000)
        config.add_entry("cluster.servers", '[{{"id": 0, "address": "{}", "tcp_port": {}, "http_port": {}}}, {{"id": 1, "address": "{}", "tcp_port": {}, "http_port": {}}}]'.format(self._options.ip, self._options.dataport, self._options.port, self._options.ip, self._options.dataport+1000, self._options.port+1000));
        config.add_entry("cluster.partitions", '[{"servers": [0,1]}]');
        config(self._conf_file)

        self.start_tt(self._conf_file)
        print "Running Replication_Test{} with config {}...".format(self._idx, self._conf_file)

    def write_data(self, dps):
        dataport = self._options.dataport   # save
        self._options.dataport = self._options.dataport+int(self._idx)*1000
        self.send_data_to_ticktock_tcp(dps)
        self._options.dataport = dataport   # restore

    def query_data(self, dps, metric_cardinality, tag_cardinality):

        port = self._options.port                   # save
        opentsdbport = self._options.opentsdbport   # save

        if self._idx == 0:
            self._options.opentsdbport = str(int(self._options.port) + 1000)
        else:
            self._options.opentsdbport = self._options.port
            self._options.port = str(int(self._options.port) + 1000)

        # this metric does not exist
        print "Try non-existing metric..."
        query = Query(metric=self.metric_name(0), start=self._options.start, end=dps._end)
        self.query_and_verify(query)

        # retrieve raw dps (no tags, no downsampling)
        print "Retrieving raw dps, no tags..."
        for m in range(1, metric_cardinality+1):
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end)
            self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        print "Retrieving raw dps, with tags..."
        for m in range(1, metric_cardinality+1):
            for tk in range(tag_cardinality):
                for tv in range(tag_cardinality):
                    tags = self.tag(tk, tv)
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                    self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        print "Retrieving raw dps, with tags..."
        for m in range(1, metric_cardinality+1):
            for tk in range(tag_cardinality):
                tags = {"tag"+str(tk): "*"}
                query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                self.query_and_verify(query)

        # retrieve raw dps with 1 tag (no aggregation, no downsampling)
        if tag_cardinality > 2:
            print "Retrieving raw dps, with tags..."
            for m in range(1, metric_cardinality+1):
                for tk in range(2, tag_cardinality):
                    tags = {"tag1":"val1", "tag"+str(tk): "val*"}
                    query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end, tags=tags)
                    self.query_and_verify(query)

        self._options.port = port                   # restore
        self._options.opentsdbport = opentsdbport   # restore

    def stop(self):

        # stop tt
        self.stop_tt(self._options.port+int(self._idx)*1000)
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Replication_Tests(Test):

    def __init__(self, options, prefix="rep"):
        super(Replication_Tests, self).__init__(options, prefix)

    def __call__(self):

        metric_cardinality = 4
        tag_cardinality = 2

        dps = DataPoints(self._prefix, self._options.start, interval_ms=50000, metric_count=1024, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        self.cleanup()
        self.test1(dps, metric_cardinality, tag_cardinality)
        self.cleanup()
        self.test2(dps, metric_cardinality, tag_cardinality)

    def test1(self, dps, metric_cardinality, tag_cardinality):

        rep0 = Replication_Test(self._options, idx=0)
        rep1 = Replication_Test(self._options, idx=1)

        rep0.start()
        rep1.start()

        # write to rep0, query from rep1
        rep0.write_data(dps)
        time.sleep(2)
        rep0.query_data(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)
        rep1.query_data(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0.stop()
        rep1.stop()

        if (rep0._failed) > 0 or (rep1._failed > 0):
            print "[FAIL] Replication.test1 failed"

        self._passed = self._passed + rep0._passed
        self._failed = self._failed + rep0._failed
        self._passed = self._passed + rep1._passed
        self._failed = self._failed + rep1._failed

    def test2(self, dps, metric_cardinality, tag_cardinality):

        rep0 = Replication_Test(self._options, idx=0)
        rep1 = Replication_Test(self._options, idx=1)

        rep0.start()

        # write to rep0, query from rep1
        rep0.write_data(dps)

        # delay second replica start
        time.sleep(8)
        rep1.start()
        time.sleep(4)

        rep0.query_data(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)
        rep1.query_data(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0.stop()
        rep1.stop()

        if (rep0._failed) > 0 or (rep1._failed > 0):
            print "[FAIL] Replication.test2 failed"

        self._passed = self._passed + rep0._passed
        self._failed = self._failed + rep0._failed
        self._passed = self._passed + rep1._passed
        self._failed = self._failed + rep1._failed


class Partition_Test(Test):

    def __init__(self, options, idx, prefix="part"):
        super(Partition_Test, self).__init__(options, prefix)
        self._idx = idx
        self._conf_file = "tt" + str(idx) + ".conf"

    def start(self):

        # generate config
        config = TickTockConfig(self._options)
        config.add_entry("append.log.dir", os.path.join(self._options.root,"append{}".format(self._idx)))
        config.add_entry("tsdb.data.dir", os.path.join(self._options.root,"data{}".format(self._idx)))
        config.add_entry("log.file", os.path.join(self._options.root,"tt{}.log".format(self._idx)))
        config.add_entry("http.server.port", self._options.port+int(self._idx)*1000)
        config.add_entry("tcp.server.port", self._options.dataport+int(self._idx)*1000)
        config.add_entry("udp.server.port", self._options.dataport+int(self._idx)*1000)
        config.add_entry("cluster.servers", '[{{"id": 0, "address": "{}", "tcp_port": {}, "http_port": {}}}, {{"id": 1, "address": "{}", "tcp_port": {}, "http_port": {}}}]'.format(self._options.ip, self._options.dataport, self._options.port, self._options.ip, self._options.dataport+1000, self._options.port+1000));
        config.add_entry("cluster.partitions", '[{"from": "a", "to": "n", "servers": [0]}, {"from": "n", "to": "{", "servers": [1]}]');
        config(self._conf_file)

        self.start_tt(self._conf_file)
        print "Running Partition_Test{} with config {}...".format(self._idx, self._conf_file)

    def write_data(self, dps):
        dataport = self._options.dataport   # save
        self._options.dataport = self._options.dataport+int(self._idx)*1000
        self.send_data_to_ticktock_tcp(dps)
        self._options.dataport = dataport   # restore

    def query_data_not_empty(self, dps, metric_cardinality, tag_cardinality):

        port = self._options.port                   # save
        opentsdbport = self._options.opentsdbport   # save

        if self._idx == 0:
            self._options.opentsdbport = str(int(self._options.port) + 1000)
        else:
            self._options.opentsdbport = self._options.port
            self._options.port = str(int(self._options.port) + 1000)

        # retrieve raw dps (no tags, no downsampling)
        print "Retrieving raw dps, no tags..."
        for m in range(1, metric_cardinality+1):
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end)
            # self.query_and_verify(query)
            actual = self.query_ticktock(query)
            if not isinstance(actual, list):
                self._failed = self._failed + 1
                return
            if len(actual) == 0:
                self._failed = self._failed + 1
                return
            for i in range(len(actual)):
                act = actual[i]
                if not isinstance(act, dict):
                    self._failed = self._failed + 1
                    return
                if not act.has_key("dps"):
                    self._failed = self._failed + 1
                    return
                d = act["dps"]
                if len(d) == 0:
                    self._failed = self._failed + 1
                    return
            self._passed = self._passed + 1

        self._options.port = port                   # restore
        self._options.opentsdbport = opentsdbport   # restore

    def query_data_empty(self, dps, metric_cardinality, tag_cardinality):

        port = self._options.port                   # save
        opentsdbport = self._options.opentsdbport   # save

        if self._idx == 0:
            self._options.opentsdbport = str(int(self._options.port) + 1000)
        else:
            self._options.opentsdbport = self._options.port
            self._options.port = str(int(self._options.port) + 1000)

        # retrieve raw dps (no tags, no downsampling)
        for m in range(1, metric_cardinality+1):
            query = Query(metric=self.metric_name(m), start=self._options.start, end=dps._end)
            actual = self.query_ticktock(query)
            if not isinstance(actual, list):
                self._failed = self._failed + 1
                return
            if len(actual) != 0:
                self._failed = self._failed + 1
                return
            self._passed = self._passed + 1

        self._options.port = port                   # restore
        self._options.opentsdbport = opentsdbport   # restore

    def stop(self):

        # stop tt
        self.stop_tt(self._options.port+int(self._idx)*1000)
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Partition_Tests(Test):

    def __init__(self, options, prefix="part"):
        super(Partition_Tests, self).__init__(options, prefix)

    def __call__(self):

        self.cleanup()
        self.test1()
        self.cleanup()
        self.test2()

    def test1(self):

        metric_cardinality = 4
        tag_cardinality = 2

        dps = DataPoints("aa", self._options.start, interval_ms=50000, metric_count=1024, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0 = Partition_Test(self._options, idx=0, prefix="aa")
        rep1 = Partition_Test(self._options, idx=1, prefix="aa")

        rep0.start()
        rep1.start()

        # write to rep0, query from rep1
        rep0.write_data(dps)
        time.sleep(2)

        rep0.query_data_not_empty(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)
        rep1.query_data_empty(dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0.stop()
        rep1.stop()

        if (rep0._failed) > 0 or (rep1._failed > 0):
            print "[FAIL] Partition.test1 failed"

        self._passed = self._passed + rep0._passed
        self._failed = self._failed + rep0._failed
        self._passed = self._passed + rep1._passed
        self._failed = self._failed + rep1._failed

    def test2(self):

        metric_cardinality = 4
        tag_cardinality = 2

        dps = DataPoints("zz", self._options.start, interval_ms=50000, metric_count=64, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0 = Partition_Test(self._options, idx=0, prefix="zz")
        rep1 = Partition_Test(self._options, idx=1, prefix="zz")

        rep0.start()
        rep1.start()

        # write to rep0, query from rep1
        rep0.write_data(dps)
        time.sleep(4)

        rep0.query_data_empty(dps=dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)
        rep1.query_data_not_empty(dps=dps, metric_cardinality=metric_cardinality, tag_cardinality=tag_cardinality)

        rep0.stop()
        rep1.stop()

        if (rep0._failed) > 0 or (rep1._failed > 0):
            print "[FAIL] Partition.test2 failed"

        self._passed = self._passed + rep0._passed
        self._failed = self._failed + rep0._failed
        self._passed = self._passed + rep1._passed
        self._failed = self._failed + rep1._failed


class Check_Point_Tests(Test):

    def __init__(self, options, prefix="cp"):
        super(Check_Point_Tests, self).__init__(options, prefix)

    def __call__(self):

        # generate config
        config = TickTockConfig(self._options)
        config.add_entry("tsdb.flush.frequency", "1s");
        config()

        self.start_tt()

        # insert some dps
        dps = DataPoints(self._prefix, self._options.start, metric_count=16)
        self.send_data_to_ticktock_tcp(dps)
        cp = self.get_checkpoint()
        j = []
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # send first cp
        self.send_checkpoint("l1", "ch1", "cp1")
        time.sleep(2)
        cp = self.get_checkpoint()
        j = [{"leader":"l1","channels":[{"channel":"ch1","checkpoint":"cp1"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # insert some more dps
        dps = DataPoints(self._prefix, self._options.start, metric_count=16)
        self.send_data_to_ticktock_tcp(dps)
        time.sleep(2)
        cp = self.get_checkpoint()
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # send second cp
        self.send_checkpoint("l1", "ch1", "cp2")
        time.sleep(2)
        cp = self.get_checkpoint("l1")
        j = [{"leader":"l1","channels":[{"channel":"ch1","checkpoint":"cp2"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # get non-existing cp
        cp = self.get_checkpoint("l9")
        j = []
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # send third cp
        self.send_checkpoint("l2", "ch1", "cp1")
        time.sleep(2)
        cp = self.get_checkpoint("l2")
        j = [{"leader":"l2","channels":[{"channel":"ch1","checkpoint":"cp1"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # send fourth cp
        self.send_checkpoint("l1", "ch2", "cp1")
        time.sleep(2)
        cp = self.get_checkpoint()
        j = [{"leader":"l1","channels":[{"channel":"ch1","checkpoint":"cp2"},{"channel":"ch2","checkpoint":"cp1"}]},{"leader":"l2","channels":[{"channel":"ch1","checkpoint":"cp1"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # send fifth cp
        self.send_checkpoint("l1", "ch1", "cp3")
        time.sleep(2)
        cp = self.get_checkpoint()
        j = [{"leader":"l1","channels":[{"channel":"ch1","checkpoint":"cp3"},{"channel":"ch2","checkpoint":"cp1"}]},{"leader":"l2","channels":[{"channel":"ch1","checkpoint":"cp1"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)

        # restart
        self.start_tt()

        cp = self.get_checkpoint()
        j = [{"leader":"l1","channels":[{"channel":"ch1","checkpoint":"cp3"},{"channel":"ch2","checkpoint":"cp1"}]},{"leader":"l2","channels":[{"channel":"ch1","checkpoint":"cp1"}]}]
        if self.verify_json(cp, j):
            self._passed = self._passed + 1
        else:
            self._failed = self._failed + 1

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class Memory_Leak_Tests(Test):

    def __init__(self, options, prefix="ml"):
        super(Memory_Leak_Tests, self).__init__(options, prefix)

    def __call__(self):

        # generate config
        #config = TickTockConfig(self._options)
        #config.add_entry("query.executor.parallel", "true");
        #config.add_entry("http.listener.count", 2);
        #config.add_entry("http.responders.per.listener", 3);
        ##config.add_entry("tsdb.rotation.frequency.sec", 9999999);
        ##config.add_entry("sanity.check.frequency.sec", 9999999);
        #config.add_entry("stats.frequency.sec", 9999999);
        #config.add_entry("tsdb.self_meter.enabled", "false");
        #config.add_entry("log.level", "INFO");
        #config()    # generate config

        # start tt
        #self.start_tt()

        metric_cardinality = 4

        # insert some dps
        dps = DataPoints(self._prefix, self._options.start, interval_ms=50000, metric_count=64, metric_cardinality=metric_cardinality, tag_cardinality=4)
        self.send_data_to_ticktock(dps)

        for i in range(999999):
            for m in range(metric_cardinality):
                for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    tags = {"tag"+str(m): "*"}
                    query = Query(metric=self.metric_name(m), start=self._options.start-99999, end=dps._end+99999, downsampler="10000ms-"+down+"-zero", tags=tags)
                    self.query_ticktock(query)

            for m in range(metric_cardinality):
                for down in ["avg", "count", "dev", "first", "last", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                    for agg in ["none", "avg", "count", "dev", "max", "min", "p50", "p75", "p90", "p95", "p99", "p999", "sum"]:
                        query = Query(metric=self.metric_name(m), start=self._options.start+99999, end=dps._end+99999, aggregator=agg, downsampler="10s-"+down+"-zero")
                        self.query_ticktock(query)
            sys.stderr.write("Round " + str(i) + " done\n")

        # stop tt
        #self.stop_tt()
        # make sure tt stopped
        #self.wait_for_tt(self._options.timeout)


class Long_Running_Tests(Test):

    def __init__(self, options, prefix="lr"):
        super(Long_Running_Tests, self).__init__(options, prefix)

    def __call__(self):

        config = TickTockConfig(self._options)
        config.add_entry("append.log.enabled", "true");
        config.add_entry("append.log.rotation.sec", 300);
        config.add_entry("http.responders.per.listener", 2);
        config()

        self.start_tt()

        dps = DataPoints(self._prefix, self._options.start, interval_ms=128, metric_count=64, metric_cardinality=4, tag_cardinality=4)

        freq_ms = 5000

        for i in range(1000):

            self.send_data_to_ticktock(dps)

            # update dps
            for dp in dps.get_dps():
                dp.set_timestamp(dp.get_timestamp() + freq_ms)
                dp.set_value(random.uniform(0,100))

            time.sleep(freq_ms / 1000)

        self._passed = self._passed + 1

        # stop tt
        self.stop_tt()
        # make sure tt stopped
        self.wait_for_tt(self._options.timeout)


class TestRunner(object):

    def __init__(self, options):

        self._options = options

    def __call__(self, tests):

        passed = 0
        failed = 0

        # hard-coded the seed for repeatability
        random.seed(1234567890)

        failures = []

        for test in tests:
            sys.stdout.write("==== %s ====\n" % test.__class__.__name__)

            test.cleanup()

            try:
                test()
            except Exception as e:
                test._failed = test._failed + 1
                print(e)
            except:
                test._failed = test._failed + 1
                sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])

            passed = passed + test._passed
            failed = failed + test._failed

            if test._failed > 0:
                failures.append(test.__class__.__name__)

            time.sleep(4)

        sys.stdout.write("PASSED: %d;  FAILED: %d;  TOTAL: %d\n" % (passed, failed, failed+passed))

        if failures:
            print "FAILUED TESTS:"
            print failures


def main(argv):

    try:
        options, args = get_options(argv)
    except:
        sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])
        return 1

    tests = deque()

    # collect ALL the tests to run here

    if options.leak:
        tests.append(Memory_Leak_Tests(options))
    else:
        #tests.append(Compaction_Tests(options))
        tests.append(Multi_Thread_Tests(options))
        tests.append(Stop_Restart_Tests(options))
        tests.append(Out_Of_Order_Write_Tests(options))
        tests.append(Rate_Tests(options))
        tests.append(Duplicate_Tests(options))
        tests.append(Check_Point_Tests(options))
        tests.append(Query_Tests(options, metric_count=16, metric_cardinality=4, tag_cardinality=4))
        tests.append(Query_With_Rollup(options))

        #tests.append(Long_Running_Tests(options))

        #tests.append(Backfill_Tests(options))
        #tests.append(Replication_Tests(options))
        #tests.append(Partition_Tests(options))

    start = datetime.datetime.now()

    # run the above tests
    runner = TestRunner(options)
    runner(tests)

    end = datetime.datetime.now()
    print("Elapsed time: " + time.strftime("%H:%M:%S", time.gmtime((end - start).total_seconds())))

    opentsdb_time = 0.0
    ticktock_time = 0.0

    for test in tests:
        opentsdb_time += test._opentsdb_time
        ticktock_time += test._ticktock_time

    print("OpenTSDB time: " + time.strftime("%H:%M:%S", time.gmtime(opentsdb_time)) + "; TickTock time: " + time.strftime("%H:%M:%S", time.gmtime(ticktock_time)))

    sys.exit(0)


def get_options(argv):

    try:
        defaults = get_defaults()
    except:
        sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])
        raise

    # get arguments
    parser = optparse.OptionParser(description='Tests for the TickTock server.')

    parser.add_option('-d', '--dataport', dest='dataport',
                      default=defaults['dataport'],
                      help='The port number to be used by TickTock to receive data.')
    parser.add_option('-f', '--format', dest='form',
                      default=defaults['form'],
                      help='TickTock receiving data format (plain or json).')
    parser.add_option('-i', '--ip', dest='ip',
                      default=defaults['ip'],
                      help='IP of the host on which OpenTSDB and TickTock runs.')
    parser.add_option('-l', '--leak', dest='leak', action='store_true',
                      default=defaults['leak'],
                      help='Run memory leak test.')
    parser.add_option('-m', '--method', dest='method',
                      default=defaults['method'],
                      help='HTTP method to use when querying TickTock.')
    parser.add_option('-o', '--opentsdb', dest='opentsdbip',
                      default=defaults['opentsdbip'],
                      help='IP of the host on which OpenTSDB runs.')
    parser.add_option('-p', '--port', dest='port',
                      default=defaults['port'],
                      help='The port number to be used by TickTock during test.')
    parser.add_option('-r', '--root', dest='root',
                      default=defaults['root'],
                      help='The root directory to be used by TickTock during test.')
    parser.add_option('-s', '--start', dest='start',
                      default=defaults['start'],
                      help='The start timestamp to be used by TickTock during test.')
    parser.add_option('-t', '--ticktock', dest='tt',
                      default=defaults['tt'],
                      help='The TickTock binary to be tested.')
    parser.add_option('-u', '--timeout', dest='timeout',
                      default=defaults['timeout'],
                      help='Timeout in seconds when sending requests to TickTock.')
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
                      default=defaults['verbose'],
                      help='Print more debug info.')

    (options, args) = parser.parse_args(args=argv[1:])

    # options.opentsdbip = defaults['opentsdbip']
    options.opentsdbport = defaults['opentsdbport']

    return options, args


def get_defaults():

    defaults = {
        'form': 'plain',
        'ip': '127.0.0.1',
        'leak': False,
        'method': 'post',
        'port': 7182,
        'dataport': 7181,
        'opentsdbip': '127.0.0.1',
        'opentsdbport': 4242,
        'root': '/tmp/tt_i',
        'start': 1569859200000,
        'timeout': 10,
        'tt': 'bin/tt',
        'verbose': False
    }

    return defaults


if __name__ == '__main__':
    sys.exit(main(sys.argv))
