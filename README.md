# TickTock [![Docker Pulls](https://img.shields.io/docker/pulls/ytyou/ticktock)](https://hub.docker.com/r/ytyou/ticktock)

An [OpenTSDB](http://opentsdb.net)-like Time Series Database, with much better performance.
It is not 100% compatible with OpenTSDB. However, you can use OpenTSDB's
[TCollector](https://github.com/OpenTSDB/tcollector) to send data to it;
you can use [Grafana](https://grafana.com) to query it (select OpenTSDB as the data source type).


## Highlights

* High Performance - High write throughput, low query latency.
* Replication - Write to any server in the cluster, get replicated to any other servers.
* Scalability - Partition the database by metric names.
* Compatibility - Compatible with OpenTSDB enough that you can use TCollector to send data to it; use Grafana to query it.
* No Dependencies - No runtime dependencies.
* Docker Ready - Start running in seconds; no installation required.
* Open Source - You can redistribute it and/or modify it under the terms of the GNU General Public License. For details, see below.


## Quick Start

### Run TickTock Demo

You need to install [Docker Engine](https://docs.docker.com/engine/install/) first. Then simply run

    docker run -d --name ticktock -p 3000:3000 -p 6181-6182:6181-6182 -p 6181:6181/udp ytyou/ticktock:latest-grafana

To use the built-in Grafana server, point your browser to your docker host at port 3000 (e.g. http://localhost:3000).
When configuring data source in Grafana, select the OpenTSDB type, with URL: http://localhost:6182. Initial
username/password is admin/admin.

### Run TickTock Only

If you already have Docker Engine installed, or if you want to install
[Docker Engine](https://docs.docker.com/engine/install/) first, then run the official docker image as follows:

    docker run -d --name ticktock -p 6181-6182:6181-6182 -p 6181:6181/udp ytyou/ticktock:latest

You can also download the binaries from [GitHub](https://github.com/ytyou/ticktock/releases)(coming soon),
unpack it, and run it with:

    bin/tt -c conf/tt.conf [-d]

Use `-d` to run it as daemon.

### Collect Metrics

Clone OpenTSDB's TCollector, and run it:

    git clone https://github.com/OpenTSDB/tcollector.git
    cd tcollector
    ./tcollector start --host <ticktock-host> --port 6181

Where `<ticktock-host>` is where you run TickTock (the host).

### Run Grafana Server

Run Grafana server in Docker:

    docker run -d --name=grafana -p 3000:3000 grafana/grafana

You can also download the latest binaries from [here](https://grafana.com/grafana/download),
unpack and run:

    bin/grafana-server web

### Visualize

Fire up your favorite browser and point it to `http://<grafana-host>:3000/`, here `<grafana-host>`
is where you run Grafana server; initial username/password is admin/admin; add an `OpenTSDB` data source
with URL `http://<ticktock-host>:6182`; Create Dashboards using the data source. Enjoy it!


## User Guide

* [Installation][docs/installation.md]
* [Configuration][docs/configuration.md]
* [Writing Data][docs/writing_data.md]
* [Querying Data][docs/querying_data.md]
* [Logging][docs/logging.md]
* [Utilities][docs/utilities.md]
* [Benchmark][docs/benchmark.md]
* [Reference][docs/reference.md]
* [License][docs/license.md]
