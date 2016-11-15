#!/bin/sh

# This script is used by CI to build for a specific flavor.  It can be used
# locally: `./workspace/build.sh [linux|android]`
#
# For Android builds, you must set the ANDROID_NDK_PATH environment variable
# to point to your Android NDK installation.

flavor=${1:-linux}

nprocs=4
if [ "$(uname)" = "Linux" ]; then
  nprocs=$(grep -c ^processor /proc/cpuinfo)
fi

set -e

rm -rf ci.build
mkdir -p ci.build
cd ci.build

cmake_flags=""
if [ ${flavor} = "android" ]; then
  [ -z $ANDROID_NDK_PATH ] && (echo "ANDROID_NDK_PATH is not set!"; exit 1)
  cmake_flags="-DREALM_PLATFORM=Android -DANDROID_NDK=${ANDROID_NDK_PATH}"
fi

cmake ${cmake_flags} ..
make VERBOSE=1 -j${nprocs}
