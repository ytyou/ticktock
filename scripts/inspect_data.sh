#!/bin/bash

# This file is to calculate how many data points in a data dir.

# default data directory
if [ ! -z "$TT_HOME" ]; then
    DATA_DIR=${TT_HOME}/data
fi

# cmdline args override default
if [ $# -eq 1 ]; then
    DATA_DIR=$1
fi

# make sure we have a valid data directory
if [ -z "$DATA_DIR" ]; then
    echo "Usage  : $0 <data file dir>"
    echo "Example: $0 /home/usr1/ticktock/data"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/../bin/inspect -d $DATA_DIR

exit 0
