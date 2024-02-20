#!/bin/bash

# This file is to migrate data from a dir to a TickTockDB.
if ! command -v nc &> /dev/null
then
    echo "nc (netcat) is requried but not found. Please install nc first (Ubuntu: sudo apt install netcat)"
    exit
fi

if [ $# -eq 3 ]; then
    HOST=$1
    PORT=$2
    DATA_DIR=$3
else
    echo "Usage  : $0 <host> <tcp port for put> <data file dir>"
    echo "Example: $0 localhost 6181 /home/usr1/backup/data"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "$DIR/../bin/inspect -d $DATA_DIR -r | nc -q 30 $HOST $PORT"
$DIR/../bin/inspect -d $DATA_DIR -r | nc -q 30 $HOST $PORT

exit 0
