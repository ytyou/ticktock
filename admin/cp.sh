#!/bin/bash
#
# start compaction

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=cp"
echo

exit 0
