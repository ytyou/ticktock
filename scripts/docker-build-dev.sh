#!/bin/bash

TARGET_BRANCH="main"
TAGL="latest"
TAGV="v0.3"
DOCKERFILE="../Dockerfile.dev"

# make sure we are at the root of repo
if ! test -f "Makefile.docker"; then
    echo "[ERROR] Not at root of repo"
    exit 2
fi

# make sure we are in main branch
GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

if [ "$GIT_BRANCH" != "$TARGET_BRANCH" ]; then
    echo "[ERROR] Not in $TARGET_BRANCH branch: $GIT_BRANCH"
    exit 1
fi

# make sure there are no local changes
#if [[ `git status --porcelain` ]]; then
#    echo "[ERROR] Repo not clean"
#    exit 1
#fi

# create build directory
rm -rf docker/dev-$TAGV
mkdir -p docker/dev-$TAGV/opt/ticktock/scripts

# prepare for docker build
cp docker/limits.conf docker/dev-$TAGV/
cp docker/tcollector docker/dev-$TAGV/
cp docker/entrypoint-dev.sh docker/dev-$TAGV/opt/ticktock/scripts/entrypoint.sh
cp -r /opt/tcollector.docker docker/dev-$TAGV/opt/tcollector
cp -r /opt/grafana-9.3.2.docker docker/dev-$TAGV/opt/grafana-9.3.2
pushd docker/dev-$TAGV/opt
ln -s grafana-9.3.2 grafana
popd

# build
pushd docker/dev-$TAGV
docker build -f $DOCKERFILE --tag ytyou/tt-dev:${TAGV} --tag ytyou/tt-dev:${TAGL} \
    --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
    --build-arg GIT_COMMIT=$(git log -1 --pretty=format:%h) \
    --build-arg VERSION=${TAGV} --add-host=tt-dev:127.0.0.1 \
    --rm --no-cache=true .
popd

exit 0
