#!/bin/bash

/usr/bin/mkdir -p /var/lib/ticktock/append
/usr/bin/mkdir -p /var/lib/ticktock/conf
/usr/bin/mkdir -p /var/lib/ticktock/data
/usr/bin/mkdir -p /var/lib/ticktock/log

if ! test -f "/var/lib/ticktock/conf/ticktock.conf"; then
    cp /opt/ticktock/conf/ticktock.conf /var/lib/ticktock/conf/
fi

# start tcollector, if present
if test -x "/opt/tcollector/tcollector"; then
    /opt/tcollector/tcollector start
fi

# start grafana, if present
if test -x "/opt/grafana/bin/grafana-server"; then
    nohup /opt/grafana/bin/grafana-server -homepath /opt/grafana web &
fi

# start TickTock
/opt/ticktock/bin/ticktock -c /var/lib/ticktock/conf/ticktock.conf -r $@

exit 0
