#!/bin/bash
#
# change log level on the fly

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

if [ $# -ge 1 ]; then
    $CURL -XPOST "http://$HOST:$PORT/api/admin?cmd=log&level=$1"
else
    echo "Usage: $0 [debug|error|fatal|http|info|tcp|trace|warn]"
fi

exit 0
