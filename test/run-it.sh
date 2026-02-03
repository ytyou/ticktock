#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PID=`pgrep -u $USER tt`

if [ ! -z "$PID" ]; then
    echo "TT (pid=$PID) still running. Abort!"
    exit 1
fi

rm -rf /tmp/tt_i /tmp/it.log

while : ; do
    curl http://dock:4242/api/stats &> /dev/null
    if [[ $? -eq 0 ]]; then
	break
    fi
    echo "Waiting for OpenTSDB..."
    sleep 5
done

# test python version
which python3 &> /dev/null

if [[ $? -eq 0 ]]; then
    source /home/yongtao/.env-mqtt/bin/activate
    $DIR/int_test3.py -o dock $@
    deactivate
else
    $DIR/int_test.py -o dock $@
fi

exit 0
