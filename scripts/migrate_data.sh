#!/bin/bash

# This file is to migrate data from a dir to a TickTockDB.
if ! command -v nc &> /dev/null
then
    echo "nc (netcat) is requried but not found. Please install nc first (Ubuntu: sudo apt install netcat)"
    exit
fi

if [ $# -eq 4 ]; then
    HOST=$1
    PORT=$2
    INSPECT=$3
    DATA_DIR=$4
else
    echo "Usage  : $0 <host> <tcp port for put> <inspect binary> <data file dir>"
    echo "Note: Inspect binary must be version compatible to data files."
    echo "E.g., if the data files are generated with TT v0.12.1, then you'd better use the inspect binary in v0.12.1."
    echo "You can simply copy the inspect binary into a backup dir before you update your TT binary dir."
    echo "Example: $0 localhost 6181 /tmp/inspect.0.12.1 /home/usr1/backup/data"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "$INSPECT -d $DATA_DIR -r | nc -q 30 $HOST $PORT"
$INSPECT -d $DATA_DIR -r | nc -q 30 $HOST $PORT

exit 0
