#!/bin/bash

# wait for TIME_WAIT connection to die
#while [ 1 ]
#do
#    netstat -a | grep 6182 | grep TIME_WAIT > /dev/null 2>&1
#    if [ $? -ne 0 ]; then
#        break
#    fi
#    sleep 1
#done

bin/tt -c conf/tt.conf -r $@

exit 0
