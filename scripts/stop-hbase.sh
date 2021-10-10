#!/bin/bash

# OpenTSDB config is loaded from /usr/local/share/opentsdb/etc/opentsdb/opentsdb.conf
# OpenTSDB log config is loaded from /usr/local/share/opentsdb/etc/opentsdb/logback.xml
# OpenTSDB binary is located at /usr/local/share/opentsdb/tsdb-2.4.0.jar
# Hbase/Hadoop/Zookeeper is located at /opt/hbase/...
# Hbase/Hadoop/Zookeeper data is stored at /tmp/hbase-yongtao/...

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JAVA=$JAVA_HOME/bin/java
export HBASE_HOME=/opt/hbase

# stop hbase if necessary
/opt/hbase/bin/stop-hbase.sh
sleep 1
/usr/bin/rm -rf /tmp/hbase/*
/usr/bin/rm -rf /tmp/hbase-$USER
/usr/bin/rm -rf /opt/hbase/logs/*
/usr/bin/rm -f QUERY_LOG_IS_UNDEFINED
