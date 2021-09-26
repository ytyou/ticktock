#!/bin/bash
#
# start ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -lt 1 ]; then
    CONF=$DIR/../conf/tt.conf
else
    CONF=$DIR/../conf/tt.${1}.conf
fi

if [ ! -f "$CONF" ]; then
    echo "Config file $CONF does not exist!"
    exit 1
fi

# make sure we can write to the default pid file
PID_FILE=/var/run/ticktock.pid
sudo touch $PID_FILE
sudo chown $USER:$USER $PID_FILE

$DIR/../bin/tt -c $CONF -d

exit 0
