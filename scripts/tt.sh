#!/bin/bash

TS=$(date +%s)
TT=tt-prod0-h

# collect rss of bin/tt and send it to tt
STR=$(ps -aux --sort -rss | grep 'bin/tt' | grep -v grep)

if [ ! -z "$STR" ]
then
    declare -a ARR=($STR)
    CPU=${ARR[2]}
    VSZ=$(( ${ARR[4]} * 1024 ))
    RSS=$(( ${ARR[5]} * 1024 ))
    echo "put ticktock.cpu $TS $CPU host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
    echo "put ticktock.rss $TS $RSS host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
    echo "put ticktock.vsize $TS $VSZ host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
fi

# collect swap usage
STR=$(free -k | grep 'Swap:')

if [ ! -z "$STR" ]
then
    declare -a ARR=($STR)
    if [ "${#ARR[@]}" -eq 4 ]
    then
        TOTAL=$(( ${ARR[1]} * 1024 ))
        echo "put swap.total $TS $TOTAL host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
        USED=$(( ${ARR[2]} * 1024 ))
        echo "put swap.used $TS $USED host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
        FREE=$(( ${ARR[3]} * 1024 ))
        echo "put swap.free $TS $FREE host=$HOSTNAME\n" | /usr/bin/nc -q 0 $TT 6181
    fi
fi

FILES=$(ls /tmp/tt/log/stat.*.log 2>/dev/null)

for file in $FILES
do
    while read line; do
        echo "put $line" | /usr/bin/nc -q 0 $TT 6181
    done < $file
    rm -f $file
done

exit 0
