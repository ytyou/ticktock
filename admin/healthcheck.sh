#!/bin/bash
#
# ping ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

HOST=192.168.1.42

RESPONSE=`$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=ping"`

# exit 0 means healthy; exit 1 means unhealthy;
if [[ $RESPONSE == "pong" ]]; then
    echo "TickTockDB running at $HOST:$PORT"
    exit 0
else
    echo "TickTockDB at $HOST:$PORT no response. Possibly down!"
    exit 1
fi
