#!/bin/bash

/usr/bin/mkdir -p /var/lib/ticktock/append
/usr/bin/mkdir -p /var/lib/ticktock/data
/usr/bin/mkdir -p /var/log/ticktock

/opt/ticktock/bin/ticktock -c /opt/ticktock/conf/ticktock.conf -r

exit 0
