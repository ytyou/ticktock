#!/bin/bash
#
# shutdown ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=stop"
echo

exit 0
