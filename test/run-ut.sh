#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rm -rf /tmp/tt_u

$DIR/../bin/all_tests

exit 0
