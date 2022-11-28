#!/bin/bash

# This file is to calculate how many data points in a data dir.

if [ $# -ne 1 ]; then
    echo "Usage  : $0 <data file dir>"
    echo "Example: $0 /home/usr1/ticktock/data"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/../bin/inspect -d $1

exit 0
