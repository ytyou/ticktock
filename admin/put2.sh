#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

TS=`date +%s`
echo "put test.metric $TS 123 host=host1" | gzip | $CURL -XPOST 'http://'$HOST':'$PORT'/api/put' -H "Content-Encoding: gzip" --data-binary @-

exit 0
