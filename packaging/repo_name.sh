#!/bin/bash

set -e

script_path="$(pushd "$(dirname $0)" >/dev/null; pwd)"
src_path="$(pushd "$script_path/.." >/dev/null; pwd)"
git_tag=$(git describe --exact-match --tags HEAD 2>/dev/null || true)

. ${src_path}/dependencies.list

if [[ $git_tag == "" ]]; then
  echo "sync-devel"
elif [[ $git_tag != "v${VERSION}" ]]; then
  echo "Git tag '$git_tag' does not match VERSION: '$VERSION'" 1>&2
else
  echo "sync"
fi
