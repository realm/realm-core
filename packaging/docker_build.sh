#!/bin/bash

set -e

script_path="$(pushd "$(dirname "$0")" >/dev/null; pwd)"
src_path="$(pushd "${script_path}/.." >/dev/null; pwd)"

. "${script_path}/functions.sh"

docker_build $@
