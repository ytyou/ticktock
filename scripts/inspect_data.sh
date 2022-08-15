#!/bin/bash

# This file is to calculate how many data points in a data dir.

if [ $# -lt 1 ]; then
    echo "Usage  : $0 <data file dir> [#thread]"
    echo "Example: $0 /home/usr1/ticktock/data 4"
    exit 1
fi

if [ $# -lt 2 ]; then
    THREAD=2
else
    THREAD=$2
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TMP_FILE="/tmp/tt_inspect_data"
totalDataPoints=0
STR=$(ls --ignore='*.cp' --ignore='*.meta' $1/)
declare -a FILES=($STR)

if [ "${#FILES[@]}" -eq 0 ]
then
    exit 2
fi

function count_dp()
{
    COUNT=$($DIR/../bin/inspect -a $1 | grep -v "dps\|Inspecting\|TSDB" | wc -l)
    FILE="$TMP_FILE$2"
    echo $COUNT > $FILE
}

for (( F=0; F<${#FILES[@]}; ));
do
    RES=()
    FIL=()
    for (( T=0; T<$THREAD; T++ ));
    do
        FIL[$T]=${FILES[$F]}
        count_dp $1/${FILES[$F]} $T &
        F=$(( F+1 ))
        if [ "$F" -ge ${#FILES[@]} ]
        then
            break
        fi
    done
    wait
    for (( T=0; T<${#FIL[@]}; T++ ));
    do
        FILE="$TMP_FILE$T"
        numDataPoints=$(cat $FILE)
        totalDataPoints=$(( totalDataPoints + numDataPoints ))
        echo "${FIL[$T]}: num=$numDataPoints, total=$totalDataPoints"
    done
done

exit 0
