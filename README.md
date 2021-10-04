# TickTock [![Docker Pulls](https://img.shields.io/docker/pulls/ytyou/ticktock)](https://hub.docker.com/r/ytyou/ticktock)

Ticktock is an open source Time Series DataBase (TSDB) for DevOps, Internet of Things (IoT), financial data. Based on many years of unsatisfied experience with TSDBs, we set our **design goals** of Ticktock as:

* **High performance:** 
  - At least 10X better than OpenTSDB, competitive to the best TSDBs like InfluxDB.
* **Easy to install and maintain:** 
  - Many TSDBs are built on top of other DBs, e.g., OpenTSDB on HBase, Clickhouse and Druid on relational DBs. This incurs complexity in installation and maintenance besides performance overhead. Ticktock is natively developed in C++ and has a single process only. It doesn't require additional expertise like HBase for OpenTSDB in maintence. It doesn't have painful Garbage Collection issues to deal with in high load scenarios.
* **Replication and Scalability supports:**
  - Ticktock provides replication and partition features in its open source versions.
* **Compatible with OpenTSDB APIs:** 
  - OpenTSDB is one of the most widely used TSDBs. Ticktock includes storing and querying APIs compatible with [OpenTSDB](http://opentsdb.net). You can use OpenTSDB's [TCollector](https://github.com/OpenTSDB/tcollector) to collect data and use [Grafana](https://grafana.com) to visualize data.

## Highlights

* **High Performance** 
  - High write throughput, low query latency. In our performance testing with some public available benchmarks, Ticktock is at least 50X better than Opentsdb, 4X than InfluxDB.
* **Replication** 
  - Write to any server in the cluster, get replicated to any other servers.
* **Scalability** 
  - Partition the database by metric names.
* **Compatibility** 
  - Compatible with OpenTSDB enough that you can use TCollector to send data to it; use Grafana to query it.
* **No Dependencies** 
  - No runtime dependencies.
* **Simplicity** 
  - One process per instance; Low maintenance.
* **Docker Ready** 
  - Start running in seconds; no installation required.
* **Open Source** 
  - You can redistribute it and/or modify it under the terms of the GNU General Public License. For details, see below.


## Quick Start

We prepare a TickTock Demo in a docker image. With a single command to launch the docker, there will be a fully functional demo in the docker,
- A Ticktock TSDB; 
- A Tcollector collecting OS metrics of the docker and sending to Ticktock; 
- A Grafana providing metric dashboard to visualize the metrics.

### Run Ticktock Demo steps:
1. You need to install [Docker Engine](https://docs.docker.com/engine/install/) first. 
2. Then simply run

       docker run -d --name ticktock -p 3000:3000 -p 6181-6182:6181-6182 -p 6181:6181/udp ytyou/ticktock:latest-grafana
    
   ![Docker command execution example](/docs/images/dockerDemoCmd.jpg)
    
3. To see the pre-built dashboard, point your browser to your docker host at port 3000 (e.g. http://localhost:3000).
   The initial username/password is admin/admin. 
   <img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoLogin1.jpg" width="650" ALIGN="middle"/>

   You will be asked to change the password at the first login. Simply skip it if you don't want to.
   <img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoLogin2.jpg" width="650" ALIGN="middle"/>
 
4. After login, go to "TickTock Demo" dashboard to see metric panels.
   <img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoDashboard1.jpg" width="650" ALIGN="middle"/>
   <img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoDashboard2.jpg" width="650" ALIGN="middle"/>
 
   The "TickTock Demo" dashboard is initialized with 4 metric panels, i.e., cpu load, memory usage, disk usage, and network usage.
   
   ![Docker command execution example](/docs/images/dockerDemoDashboard3.jpg)

## Documentation

For more information, please see Ticktock [documentation](https://github.com/ytyou/ticktock/wiki/User-Guide).
