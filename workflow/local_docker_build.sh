#!/bin/sh
# This script can be used to locally check and debug
# the linux build process outside of CI.
# This should be run from the root directory: `./workflow/local_docker_build.sh`

set -e

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
