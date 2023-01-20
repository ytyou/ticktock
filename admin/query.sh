#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL 'http://'$HOST':'$PORT'/api/query?start=1d-ago&m=avg:test.metric%7Bhost=host1%7D'
echo

exit 0
