#!/bin/sh
# This script can be used to locally check and debug
# the linux build process outside of CI.
# This should be run from the root directory `packaging/local_docker_build.sh`

set -e

rm -rf CMakeCache.txt
find . -name CMakeFiles -print0 | xargs -0 rm -rf

docker build -t ci/realm-object-server:build .

docker run --rm \
  -u $(id -u) \
  -v "${HOME}:${HOME}" \
  -e HOME="${HOME}" \
  -v /etc/passwd:/etc/passwd:ro \
  -v $(pwd):/source \
  -w /source \
  ci/realm-object-server:build \
  ./workflow/test_coverage.sh
