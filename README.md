# TickTockDB [![GitHub license](https://img.shields.io/github/license/ytyou/ticktock.svg)](https://github.com/ytyou/ticktock/blob/main/LICENSE) [![Docker Pulls](https://img.shields.io/docker/pulls/ytyou/ticktock)](https://hub.docker.com/r/ytyou/ticktock)

TickTockDB is an open source Time Series DataBase (TSDB) for DevOps, Internet of Things (IoT), and financial data. Based on many years of unsatisfied experience with TSDBs, we set our **design goals** of TickTockDB as:

* **Low resource requirement:**
  - It can run even with very low resources.
* **High performance:** 
  - At least 10X better than OpenTSDB, competitive to the best TSDBs like InfluxDB.
* **Easy to install and maintain:** 
  - Many TSDBs are built on top of other DBs, e.g., OpenTSDB on HBase, Timescaledb on Postgresql, Clickhouse and Druid on relational DBs. This incurs complexity in installation and maintenance besides performance overhead. TickTockDB is natively developed in C++ and has a single process only. It doesn't require additional expertise like HBase for OpenTSDB in maintence. It doesn't have painful Garbage Collection issues to deal with in high load scenarios.
* **Compatible with OpenTSDB APIs:** 
  - OpenTSDB is one of the most widely used TSDBs. TickTockDB includes storing and querying APIs compatible with [OpenTSDB](http://opentsdb.net). You can use OpenTSDB's [TCollector](https://github.com/OpenTSDB/tcollector) to collect data and use [Grafana](https://grafana.com) to visualize data.

## Highlights

* **High Performance** 
  - High write throughput, low query latency. In our performance testing with some public available benchmarks, TickTockDB is at least 50X better than Opentsdb, 10X than InfluxDB.
  - Run well even in RaspBerryPI with ARMv6, 32bit OS.
* **Compatibility** 
  - Compatible with OpenTSDB enough that you can use TCollector to send data to it; use Grafana to query it.
  - Compatible with InfluxDB line write protocol to maximize write throughput.
* **No Dependencies** 
  - No runtime dependencies.
* **Simplicity** 
  - One process per instance; Low maintenance.
* **Docker Ready** 
  - Start running in seconds; no installation required.
* **Open Source** 
  - You can redistribute it and/or modify it under the terms of the GNU General Public License. For details, see below.


## Quick Start

We prepare a TickTockDB Demo in a docker image. With a single command to launch the docker, there will be a fully functional demo in the docker,
- A TickTockDB TSDB; 
- A Tcollector collecting OS metrics of the docker and sending to TickTockDB; 
- A Grafana providing metric dashboard to visualize the metrics.

### To run TickTockDB Demo:

1. You need to install [Docker Engine](https://docs.docker.com/engine/install/) first. 
2. Then simply run

       docker run -d --name ticktock -p 3000:3000 -p 6181-6182:6181-6182 -p 6181:6181/udp ytyou/ticktock:latest-grafana
    
   ![Docker command execution example](/docs/images/dockerDemoCmd.jpg)
    
3. To see the pre-built dashboard, point your browser to your docker host at port 3000 (e.g. http://localhost:3000).
   The initial username/password is admin/admin. 
   ![Docker Demo: Grafana login page](/docs/images/dockerDemoLogin1.jpg)

   You will be asked to change the password at the first login. Simply skip it if you don't want to.
   ![Docker Demo: Grafana login page](/docs/images/dockerDemoLogin2.jpg)
 
4. After login, go to "TickTockDB Demo" dashboard to see metric panels.
   ![Docker Demo: Grafana Dashboard](/docs/images/dockerDemoDashboard1.jpg)
   ![Docker Demo: Grafana Dashboard 2](/docs/images/dockerDemoDashboard2.jpg)
 
   The "TickTockDB Demo" dashboard is initialized with 4 metric panels, i.e., cpu load, memory usage, disk usage, and network usage.
   
   ![Docker Demo: Grafana Dashboard 3](/docs/images/dockerDemoDashboard3.jpg)

## Documentation

For more information, please refer to TickTockDB [documentation](https://github.com/ytyou/ticktock/wiki/User-Guide).
