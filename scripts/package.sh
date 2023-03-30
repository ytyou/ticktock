#!/bin/bash
#
# This script is used to create a tarball for TickTockDB.

echo "packaging..."

STAGE="beta"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# look for current version numbers
LINE=$(grep MAJOR $DIR/../include/global.h)
MAJOR=${LINE##* }
LINE=$(grep MINOR $DIR/../include/global.h)
MINOR=${LINE##* }
LINE=$(grep PATCH $DIR/../include/global.h)
PATCH=${LINE##* }
TT_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAGV=${TT_VERSION}-${STAGE}
GLIBC=`ldd --version | grep ldd | grep -o '[^ ]*$'`
ARCH=`uname -m`

pushd $DIR/..
mkdir -p pkgs
rm -f pkgs/ticktockdb-${TAGV}.tar.gz
rm -rf ticktockdb
mkdir -p ticktockdb/tools
cp -r admin ticktockdb
cp -r bin ticktockdb
cp -r conf ticktockdb
cp tools/prom_scraper.* ticktockdb/tools
/bin/tar cfz pkgs/ticktockdb-${TAGV}-glibc${GLIBC}-${ARCH}.tar.gz ticktockdb
popd

echo "package pkgs/ticktockdb-${TAGV}-glibc${GLIBC}-${ARCH}.tar.gz created"

exit 0
