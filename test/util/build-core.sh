#!/usr/bin/env bash

set -euo pipefail

if [ $# -lt 1 ]; then
  REF=master
else
  REF=$1
fi

BRANCH="$(basename "${REF}")"
if [ "${BRANCH}" == "${REF}" ]; then
  REF="origin/${REF}"
fi

ORIGINDIR="${PWD}"
BASEDIR="$(realpath "core-builds/${BRANCH}")"
SRCDIR="${BASEDIR}/src"

mkdir -p "${BASEDIR}"

if [ ! -d "${SRCDIR}" ]; then
  git clone git@github.com:realm/realm-core.git "${SRCDIR}"
else
  git fetch
fi

cd "${SRCDIR}"

git checkout -B "${BRANCH}" "${REF}"
sh build.sh clean
sh build.sh config "${BASEDIR}"
sh build.sh build
sh build.sh install
