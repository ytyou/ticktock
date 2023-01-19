#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL 'http://'$HOST':'$PORT'/api/query?start=1d-ago&m=avg:5m-avg:test.measurement%7Bhost=host1,_field=field2%7D'
echo

$CURL 'http://'$HOST':'$PORT'/api/query?start=1d-ago&m=avg:5m-avg:test.measurement2%7Bhost=host1,_field=field4%7D'
echo

exit 0
