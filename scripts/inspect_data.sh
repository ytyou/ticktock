#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage  : $0 <data file dir>"
    echo "Example: $0 /home/usr1/ticktock/data"
    exit 1
fi

inputDir=$1
totalDataPoints=0

for f in $1/*;
do
	numDataPoints=$(../bin/inspect -a $f | grep -v dps | wc -l)
	totalDataPoints=$(( $totalDataPoints + $numDataPoints ))
	echo "$f: num=$numDataPoints, total=$totalDataPoints"
done

exit 0
