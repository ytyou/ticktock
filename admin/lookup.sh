#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL 'http://'$HOST':'$PORT'/api/search/lookup?m=test.metric%7Bhost=*%7D&limit=10'
echo

exit 0
