#!/bin/bash
#
# retrieve version of the ticktock server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL "http://$HOST:$PORT/api/version"
echo

exit 0
