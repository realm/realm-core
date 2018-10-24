#!/bin/bash

set -e

script_path="$(pushd "$(dirname "$0")" >/dev/null; pwd)"
src_path="$(pushd "${script_path}/.." >/dev/null; pwd)"

. "${script_path}/functions.sh"


git_tag=$(git describe --exact-match --tags HEAD 2>/dev/null || true)

. "${src_path}/dependencies.list"

if [ -z "$git_tag" ]; then
  info "No git tag exists.  Triggering -devel build"
  sha=$(git rev-parse HEAD | cut -b1-8)
  ITERATION="0.$sha"
elif [ "$git_tag" != "v${VERSION}" ]; then
  die "Git tag '$git_tag' does not match VERSION: '$VERSION'"
else
  info "Found git tag: '$git_tag'. Triggering release build"
  ITERATION="${BUILD_NUMBER:-1}"
fi

rm -rf "${src_path}/packaging/out"; mkdir -p "${src_path}/packaging/out"

cp "${src_path}/dependencies.list" "${src_path}/packaging/out/packaging.list"
cat <<-EOD >> "${src_path}/packaging/out/packaging.list"
ITERATION=$ITERATION
GIT_TAG=$git_tag
EOD

env_file="${src_path}/packaging/out/packaging.list"

distro_path="${src_path}/packaging/generic"
image_name="ci/${PACKAGE_NAME}:generic"

mkdir -p "${src_path}/packaging/out/generic"
rm -f "${src_path}/packaging/out/generic/*"

mkdir -p "${src_path}/packaging/test-logs/generic"
rm -f "${src_path}/packaging/test-logs/generic/*"

docker_build "${image_name}-base" "${distro_path}/base-image"

docker_build "${image_name}-build" "${distro_path}/build-image"

info "Running 'generic' build..."
docker run \
  --env-file "${env_file}" \
  --rm \
  -v "${src_path}/packaging/generic/files:/files:ro,z" \
  -v "${src_path}/packaging/generic/build-image/inside:/inside:ro,z" \
  -v "${src_path}:/source:ro,z" \
  -v "${src_path}/packaging/common:/common:ro,z" \
  -v "${src_path}/packaging/out/generic:/out:z" \
  -w /inside "${image_name}-build" \
  || die "Build phase for 'generic' failed."

info "Build phase for 'generic' succeeded."
