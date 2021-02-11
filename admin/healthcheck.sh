#!/bin/bash
#
# ping ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

RESPONSE=`$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=ping"`

# exit 0 means healthy; exit 1 means unhealthy;
if [[ $RESPONSE == "pong" ]]; then
    exit 0
else
    exit 1
fi
