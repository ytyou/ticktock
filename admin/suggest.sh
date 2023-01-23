#!/bin/bash
#
# retrieve suggestions of metrics names

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

PORT=4242
$CURL 'http://'$HOST':'$PORT'/api/suggest?type=metrics&q=cpu&max=1000'$@
echo

exit 0
