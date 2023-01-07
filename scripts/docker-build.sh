#!/bin/bash

MAKE="/usr/bin/make -f Makefile.docker"
STAGE="beta"
TARGET_BRANCH="main"
TAGL="latest"
DOCKERFILE="../Dockerfile"
_TEST=0
_GRAFANA=0

# process command line arguments
while [[ $# -gt 0 ]]
do
    key=$1

    case $key in
        --grafana)
        _GRAFANA=1
        ;;

        --test)
        _TEST=1
        ;;
    esac
    shift
done

if [ $_TEST -eq 0 ]; then

    # make sure we are in main branch
    GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

    if [ "$GIT_BRANCH" != "$TARGET_BRANCH" ]; then
        echo "[ERROR] Not in $TARGET_BRANCH branch: $GIT_BRANCH"
        exit 1
    fi

    # make sure there are no local changes
    if [[ `git status --porcelain` ]]; then
        echo "[ERROR] Repo not clean"
        exit 1
    fi

    BUILD_OPT="--no-cache=true"
else
    BUILD_OPT="--no-cache=false"
fi

# make sure we are at the root of repo
if ! test -f "Makefile.docker"; then
    echo "[ERROR] Not at root of repo"
    exit 2
fi

if ! test -f "include/global.h"; then
    echo "[ERROR] Not at root of repo"
    exit 2
fi

# build binary
if ! $MAKE clean; then
    echo "[ERROR] '$MAKE clean' failed"
    exit 2
fi

if ! $MAKE all; then
    echo "[ERROR] '$MAKE all' failed"
    exit 2
fi

# look for current version numbers
LINE=$(grep MAJOR include/global.h)
MAJOR=${LINE##* }
LINE=$(grep MINOR include/global.h)
MINOR=${LINE##* }
LINE=$(grep PATCH include/global.h)
PATCH=${LINE##* }
TT_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAGV=${TT_VERSION}-${STAGE}

# create build directory
rm -rf docker/$TT_VERSION/*
mkdir -p docker/$TT_VERSION/opt/ticktock/bin
mkdir -p docker/$TT_VERSION/opt/ticktock/conf
mkdir -p docker/$TT_VERSION/opt/ticktock/scripts

# prepare for docker build
cp bin/tt docker/$TT_VERSION/opt/ticktock/bin/ticktock
cp bin/backfill docker/$TT_VERSION/opt/ticktock/bin/backfill
cp conf/tt.docker.conf docker/$TT_VERSION/opt/ticktock/conf/ticktock.conf
cp admin/* docker/$TT_VERSION/opt/ticktock/scripts/
cp docker/entrypoint.sh docker/$TT_VERSION/opt/ticktock/scripts/
cp docker/limits.conf docker/$TT_VERSION/

if [ $_GRAFANA -ne 0 ]; then
    TAGL="${TAGL}-grafana"
    TAGV="${TAGV}-grafana"
    DOCKERFILE=${DOCKERFILE}.grafana
    cp docker/tcollector docker/$TT_VERSION/
    cp -r /opt/tcollector.docker docker/$TT_VERSION/opt/tcollector
    cp -r /opt/grafana-9.3.2.docker docker/$TT_VERSION/opt/grafana-9.3.2
    pushd docker/$TT_VERSION/opt
    ln -s grafana-9.3.2 grafana
    popd
fi

if [ $_TEST -ne 0 ]; then
    TAGL="${TAGL}-test"
    TAGV="${TAGV}-test"
fi

# build
pushd docker/$TT_VERSION
docker build -f $DOCKERFILE --tag ytyou/ticktock:${TAGV} --tag ytyou/ticktock:${TAGL} \
    --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
    --build-arg GIT_COMMIT=$(git log -1 --pretty=format:%h) \
    --build-arg VERSION=${TT_VERSION}-${STAGE} --add-host=ticktock:127.0.0.1 \
    --rm $BUILD_OPT .
popd

exit 0
