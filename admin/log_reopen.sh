#!/bin/bash
#
# rotate log file

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=log&action=reopen"

exit 0
