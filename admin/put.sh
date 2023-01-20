#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

TS=`date +%s`
$CURL -XPOST 'http://'$HOST':'$PORT'/api/put' -d "put test.metric $TS 123 host=host1"
echo "Inserted: put test.metric $TS 123 host=host1"

exit 0
