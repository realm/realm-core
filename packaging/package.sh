#!/bin/bash

set -e

die() { echo "$@" 1>&2 ; exit 1; }
info() { echo "===> $@"; }

script_path="$(pushd "$(dirname $0)" >/dev/null; pwd)"
src_path="$(pushd "$script_path/.." >/dev/null; pwd)"

default="generic centos-6 centos-7 ubuntu-1604"
distros=${@:-$default}

git_tag=$(git describe --exact-match --tags HEAD 2>/dev/null || true)

. ${src_path}/dependencies.list

PACKAGECLOUD_URL="https://${PACKAGECLOUD_MASTER_TOKEN}:@packagecloud.io/install/repositories/realm/sync-devel"

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

mkdir -p ${src_path}/packaging/out
cat <<-EOD > ${src_path}/packaging/out/packaging.list
VERSION=$VERSION
ITERATION=$ITERATION
EOD

env_file=${src_path}/packaging/out/packaging.list

for distro in $distros; do
  distro_path="${src_path}/packaging/${distro}"
  image_name="realm-core-${distro}"

  mkdir -p ${src_path}/packaging/out/$distro
  rm -f ${src_path}/packaging/out/$distro/*

  mkdir -p ${src_path}/packaging/test-logs/$distro
  rm -f ${src_path}/packaging/test-logs/$distro/*

  info "Building '$distro' base image..."
  docker build --pull \
    --build-arg "PACKAGECLOUD_URL=$PACKAGECLOUD_URL" \
    -t ${image_name}-base:latest ${distro_path}/base-image || \
      die "Building '$distro' base image failed"

  info "Building '$distro' build image..."
  docker build -t ${image_name}-build:latest ${distro_path}/build-image || \
    die "Building '$distro' build image failed."

  info "Running '$distro' build..."
  docker run \
    --env-file ${env_file} \
    --rm \
    -v ${src_path}/packaging/${distro}/build-image/inside:/inside:ro,z \
    -v ${src_path}:/source:ro,z \
    -v ${src_path}/packaging/out/${distro}:/out:z \
    -w /inside ${image_name}-build:latest \
    || die "Build phase for '$distro' failed."

  info "Building '$distro' test image..."
  docker build -t ${image_name}-test:latest ${distro_path}/test-image || \
    die "Building '$distro' test image failed."

  info "Running '$distro' tests..."
  docker run \
      --env-file ${env_file} \
      --rm \
      -v ${src_path}/packaging/${distro}/test-image/inside:/inside:ro,z \
      -v ${src_path}/packaging/out/${distro}:/out:z \
      -v ${src_path}/packaging/test-logs/${distro}:/test-logs:z \
      -w /inside ${image_name}-test:latest \
      || die "Test phase for '$distro' failed."

  info "Test phase for '$distro' succeeded."
done
