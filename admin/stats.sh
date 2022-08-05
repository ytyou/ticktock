#!/bin/bash
#
# retrieve statistics of the ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL "http://$HOST:$PORT/api/stats"

exit 0
