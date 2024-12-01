#!/bin/bash

/usr/bin/mkdir -p /var/lib/ticktock/conf
/usr/bin/mkdir -p /var/lib/ticktock/data
/usr/bin/mkdir -p /var/lib/ticktock/log

if ! test -f "/var/lib/ticktock/conf/ticktock.conf"; then
    cp /opt/ticktock/conf/ticktock.conf /var/lib/ticktock/conf/
fi

# start tcollector, if present
if test -x "/opt/tcollector/tcollector"; then
    /opt/tcollector/tcollector start --port 6181 --pidfile /opt/tcollector/run/tcollector.pid --logfile /opt/tcollector/log/tcollector.log
fi

# start grafana, if present
if test -x "/opt/grafana/bin/grafana-server"; then
    nohup /opt/grafana/bin/grafana-server -homepath /opt/grafana server &
fi

# start TickTock
exec /opt/ticktock/bin/ticktock -c /var/lib/ticktock/conf/ticktock.conf -r $@

