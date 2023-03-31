#!/bin/bash
#
# find out glibc version installed on the host

ldd --version | grep ldd | grep -o '[^ ]*$'

exit 0
