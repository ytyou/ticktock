#!/bin/bash

# OpenTSDB config is loaded from /etc/opentsdb.conf
# OpenTSDB log config is loaded from /home/$USER/src/opentsdb/src/logback.xml
# OpenTSDB binary is located at /usr/local/share/opentsdb/tsdb-2.4.0.jar
# Hbase/Hadoop/Zookeeper is located at /opt/hbase/...
# Hbase/Hadoop/Zookeeper data is stored at /tmp/hbase-$USER/...

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JAVA=$JAVA_HOME/bin/java
export HBASE_HOME=/opt/hbase

# stop hbase if necessary
/opt/hbase/bin/stop-hbase.sh
