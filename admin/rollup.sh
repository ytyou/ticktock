#!/bin/bash
#
# start rollup

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=rollup"
echo

exit 0
