#!/bin/bash

TARGET_BRANCH="main"
TAGV="glibc2.28-x86_64"
DOCKERFILE="../Dockerfile.build"

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
if [[ `git status --porcelain` ]]; then
    echo "[ERROR] Repo not clean"
    exit 1
fi

# create build directory
rm -rf docker/build-$TAGV
mkdir -p docker/build-$TAGV

# build
pushd docker/build-$TAGV
docker build -f $DOCKERFILE --tag ytyou/tt-build:${TAGV} \
    --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
    --build-arg GIT_COMMIT=$(git log -1 --pretty=format:%h) \
    --build-arg VERSION=${TAGV} --add-host=tt-build:127.0.0.1 \
    --rm --no-cache=true .
popd

exit 0
