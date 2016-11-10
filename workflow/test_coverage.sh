#!/bin/sh

cmake -DCMAKE_BUILD_TYPE=Coverage .
# sh realm-sync/build.sh config -DREALM_SYNC_PREFIX=/source/realm-sync/
make VERBOSE=1 -j2 generate-coverage-cobertura
