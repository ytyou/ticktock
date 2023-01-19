#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/common.sh

# InfluxDB's Line Protocol
$CURL -XPOST 'http://'$HOST':'$PORT'/api/write' -d $'test.measurement,host=host1,sensor=sensor1 field1=1,field2=2,field3=3\ntest.measurement2,host=host1,sensor=sensor2 field4=4,field5=5'

exit 0
