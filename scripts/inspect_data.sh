#!/bin/bash

# This file is to calculate how many data points in a data dir.

if [ $# -ne 1 ]; then
    echo "Usage  : $0 <data file dir>"
    echo "Example: $0 /home/usr1/ticktock/data"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

inputDir=$1
totalDataPoints=$($DIR/../bin/inspect -d $1 | grep "^ts = " | wc -l)
echo "total=$totalDataPoints"

exit 0
