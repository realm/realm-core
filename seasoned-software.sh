#!/bin/bash

# This file is only used by the Seasoned Software fuzz system.

set -e

mkdir build
cd build

cmake -D CMAKE_BUILD_TYPE=Debug -D REALM_ENABLE_ASSERTIONS=ON -D REALM_LIBFUZZER=ON -D REALM_MAX_BPNODE_SIZE=1000 -D REALM_ENABLE_ENCRYPTION=ON ..
make -j4 realm-libfuzzer
register-binary fuzzgroup test/fuzzy/realm-libfuzzer
