#!/bin/bash

# This file is only used by the Seasoned Software fuzz system.

set -e

FLAGS="$CFLAGS -O2 -D REALM_DEBUG=1 -D REALM_ENABLE_ASSERTIONS=1"

mkdir build
cd build

cmake \
  -G Ninja \
  -D REALM_LIBFUZZER=ON \
  -D CMAKE_BUILD_TYPE=Debug \
  -D REALM_ENABLE_ASSERTIONS=ON \
  -D CMAKE_C_COMPILER=clang \
  -D CMAKE_CXX_COMPILER=clang++ \
  -D REALM_MAX_BPNODE_SIZE=1000 \
  -D REALM_ENABLE_ENCRYPTION=ON \
  -D CMAKE_CXX_FLAGS="${FLAGS}" \
  -D CMAKE_C_FLAGS="${FLAGS}" \
  ..

ninja realm-libfuzzer

upload-binary test/fuzzy/realm-libfuzzer
