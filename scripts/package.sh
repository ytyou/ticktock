#!/bin/bash
#
# This script is used to create a tarball for TickTockDB.

echo "packaging..."

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MQTT=""

for arg in "$@"; do
    if [[ "$arg" -eq "-DENABLE_MQTT" ]]; then
        MQTT="mqtt-"
    fi
done

# look for current version numbers
LINE=$(grep MAJOR $DIR/../include/global.h)
MAJOR=${LINE##* }
LINE=$(grep MINOR $DIR/../include/global.h)
MINOR=${LINE##* }
LINE=$(grep PATCH $DIR/../include/global.h)
PATCH=${LINE##* }
TT_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAGV="${MQTT}${TT_VERSION}"
GLIBC=`ldd --version | grep ldd | grep -o '[^ ]*$'`
ARCH=`uname -m`
ROOTD="ticktockdb-${TAGV}"

pushd $DIR/..
mkdir -p pkgs
rm -f pkgs/ticktockdb-${TAGV}.tar.gz
rm -rf $ROOTD
mkdir -p $ROOTD/scripts
mkdir -p $ROOTD/tools
cp docs/README.1st $ROOTD
cp -r admin $ROOTD
cp -r bin $ROOTD
cp -r conf $ROOTD
cp scripts/glibc-version.sh $ROOTD/scripts
cp tools/prom_scraper.* $ROOTD/tools
/bin/tar cfz pkgs/ticktockdb-${TAGV}-glibc${GLIBC}-${ARCH}.tar.gz $ROOTD
popd

echo "package pkgs/ticktockdb-${TAGV}-glibc${GLIBC}-${ARCH}.tar.gz created"

exit 0
