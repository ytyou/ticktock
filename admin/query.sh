#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL 'http://'$HOST':'$PORT'/api/query?start=1d-ago&m=avg:5m-avg:cpu.usr'
echo

exit 0
