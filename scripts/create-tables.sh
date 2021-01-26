#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

COMPRESSION=NONE
$DIR/create_table_no_ttl.sh
