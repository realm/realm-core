#!/bin/sh
# This script can be used to locally check and debug
# the linux build process outside of CI.
# This should be run from the root directory `packaging/manual_docker_build.sh`

set -e
rm -rf CMakeCache.txt
find . -name CMakeFiles -delete

docker build -t ci/realm-object-server:build .

docker run -it --rm -v $(pwd):/source -w /source ci/realm-object-server:build ./workflow/test_coverage.sh

