# TickTock
An OpenTSDB-like Time Series Database. It is not 100% compatible with OpenTSDB.
However, you can use TCollector to send data to it; you can use Grafana to query it
(select OpenTSDB as the data source).

Supported Platforms
-------------------
* CentOS 7
* Ubuntu 18

Other Linux platforms should also work, although we haven't tested them yet.

Build
-----
Install the following:
* zlib 1.2.11 static library (libz.a)
* glibc static libraries

On CentOS,
```
$ yum install zlib-devel
$ yum install glibc-static libstdc++-static
```
On Ubuntu,
```
$ apt install zlib
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
Use '-d' option to run the server as Linux daemon.

Collect Metrics
---------------
Use TCollector to send metrics to the TcpServer (default port number 6181).

Query Metrics
-------------
Ask Grafana to send queries to the HttpServer (default port number 6182).
Select OpenTSDB as the type of the data source.

Scaling
-------

Work in progress.

When finished, you can form a cluster of TickTock servers, each can hold part of
the metrics, and collectively they act just like a single TickTock server, with
the ability to scale horizontally.

Dependency Licenses
-------------------
* git@github.com:AlexeyAB/object_threadsafe.git - Apache License 2.0
* thread-safe queue by Joe Best-Rotheray - MIT License
* robin_map by Thibaut Goetghebuer-Planchon <tessil@gmx.com> - MIT License
* zlib - See zlib.net
