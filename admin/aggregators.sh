#!/bin/bash
#
# retrieve list of supported aggregators

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL "http://$HOST:$PORT/api/aggregators"
echo

exit 0
