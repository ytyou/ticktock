#!/bin/bash
#
# add/update config on the fly

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

if [ $# -eq 2 ]; then
    $CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=cfg&$1=$2"
else
    echo "Usage: $0 <key> <value>"
fi

exit 0
