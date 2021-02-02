#!/bin/bash

PLATFORM=centos

if [ $# -eq 3 ]; then
    MAJOR=$1
    MINOR=$2
    PATCH=$3
else
    LINE=$(grep MAJOR include/global.h)
    MAJOR=${LINE##* }
    LINE=$(grep MINOR include/global.h)
    MINOR=${LINE##* }
    LINE=$(grep PATCH include/global.h)
    PATCH=${LINE##* }
fi

TT_PKG=ticktock-${MAJOR}.${MINOR}.${PATCH}-alpha-${PLATFORM}.tar.gz

echo "Creating ${TT_PKG}..."

make clean
make all

TT_ROOT=/tmp/tt_r

mkdir -p $TT_ROOT
rm -rf ${TT_ROOT}/*
mkdir -p $TT_ROOT/conf

cp -r admin $TT_ROOT
cp -r bin $TT_ROOT
rm ${TT_ROOT}/bin/all_tests
cp conf/tt.conf ${TT_ROOT}/conf/
cp LICENSE README.md $TT_ROOT

pushd $TT_ROOT
tar cfz $TT_PKG *
popd

cp $TT_ROOT/$TT_PKG releases/

exit 0
