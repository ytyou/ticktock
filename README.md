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
* Simplicity - One process per instance; Low maintenance.
* Docker Ready - Start running in seconds; no installation required.
* Open Source - You can redistribute it and/or modify it under the terms of the GNU General Public License. For details, see below.


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
   <center>img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoLogin1.jpg" width="650"/></center>

   You will be asked to change the password at the first login. Simply skip it if you don't want to.
   <center>img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoLogin2.jpg" width="650"/></center>
 
4. After login, go to "TickTock Demo" dashboard to see metric panels.
   <center>img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoDashboard1.jpg" width="650"/></center>

   <center>img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoDashboard2.jpg" width="650"/></center>
 
   The "TickTock Demo" dashboard is initialized with 4 metric panels, i.e., cpu load, memory usage, disk usage, and network usage.

   <center><img src="https://github.com/ytyou/ticktock/blob/feature/dockerReadme/docs/images/dockerDemoDashboard3.jpg" width="650"/></center>

## Documentation

For more information, please see Ticktock documentation in [User Guide](https://github.com/ytyou/ticktock/wiki/User-Guide).
