# TickTock
An OpenTSDB-like Time Series Database. It is not 100% compatible with OpenTSDB.
However, you can use TCollector to send data to it; you can use Grafana to query it
(select OpenTSDB as the data source type).

Highlights
----------
* High Performance - High write throughput, low query latency.
* Replication - Write to any server in the cluster, get replicated to any other servers.
* Scalability - Partition the database by metric names.
* Compatibility with OpenTSDB - Compatible enough so that you can use TCollector to send data to it; use Grafana to query it.
* Docker Ready - To run it: docker run -d -v ticktock:/var/lib/ticktock --network=host --name ticktock ytyou/ticktock:latest

Supported Platforms
-------------------
* CentOS 7
* Ubuntu 18 and up
* Docker Container

Other Linux platforms should also work, although we haven't tested them yet.

Build
-----
Install the following:
* zlib 1.2.11
* glibc libraries

On CentOS,
```
$ yum install zlib-devel
$ yum groupinstall "Development Tools"
```
On Ubuntu,
```
$ apt install zlib1g-dev
$ apt install build-essential
```
To build TickTock on CentOS, run
```
$ make
```
On Ubuntu, run
```
$ make -f Makefile.ubuntu
```

Running TickTock Server
-----------------------
```
$ tt -c <config> [-d]
```
Use '-d' option to run the server as Linux daemon. To run it in a Docker container,
```
$ docker run -d -v ticktock:/var/lib/ticktock --network=host --name ticktock ytyou/ticktock:latest
```

Collect Metrics
---------------
Use TCollector to send metrics to the TcpServer (default port number 6181).

Query Metrics
-------------
Ask Grafana to send queries to the HttpServer (default port number 6182).
Select OpenTSDB as the type of the data source.

Dependency Licenses
-------------------
* git@github.com:AlexeyAB/object_threadsafe.git - Apache License 2.0
* thread-safe queue by Joe Best-Rotheray - MIT License
* robin_map by Thibaut Goetghebuer-Planchon <tessil@gmx.com> - MIT License
* zlib - See zlib.net
