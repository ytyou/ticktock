#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

$CURL 'http://'$HOST':'$PORT'/api/query' -d '{"start":1569859200000,"globalAnnotations":"true","end":1569859464774,"msResolution":"true","queries":[{"aggregator":"none","metric":"sr_metric_2","tags":{"tag1":"val1"}}]}'

echo

exit 0
