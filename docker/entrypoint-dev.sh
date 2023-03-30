#!/bin/bash

# start tcollector, if present
if test -x "/opt/tcollector/tcollector"; then
    /opt/tcollector/tcollector start
fi

# start grafana, if present
if test -x "/opt/grafana/bin/grafana-server"; then
    /opt/grafana/bin/grafana-server -homepath /opt/grafana web
fi

exit 0
