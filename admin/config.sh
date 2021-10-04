#!/bin/bash
#
# retrieve configs of the ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL "http://$HOST:$PORT/api/config"
echo

exit 0
