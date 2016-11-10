#!/bin/sh

nprocs=4
if [ "$(uname)" = "Linux" ]; then
  nprocs=$(grep -c ^processor /proc/cpuinfo)
fi

cmake -DCMAKE_BUILD_TYPE=Coverage . && \
make VERBOSE=1 -j${nprocs} generate-coverage-cobertura
