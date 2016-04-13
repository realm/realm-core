#!/usr/bin/env bash

set -euo pipefail

function showUsage () {
  cat <<EOF
Usage: $0 <branch>|<commit>|<tag>
Commit can be the 7-letter commit ID.
NB! A tag must begin with tags/.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h | --help )
      showUsage
      exit 0
      ;;
    * )
      break
      ;;
  esac
done

if [ $# -lt 1 ]; then
  showUsage
  exit 1
else
  REF=$1
fi

BASEDIR="core-builds/${REF}"
mkdir -p "${BASEDIR}"
BASEDIR="$(realpath "${BASEDIR}")"

SRCDIR="${BASEDIR}/src"

if [ ! -d "${SRCDIR}" ]; then
  git clone git@github.com:realm/realm-core.git "${SRCDIR}"
else
  git fetch
fi

cd "${SRCDIR}"

if [ `git branch -r | grep "^\\s*origin/${REF}$"` ]; then
  REMOTE="origin/${REF}"
else
  REMOTE="${REF}"
fi

git checkout -B "${REF}" "${REMOTE}"
sh build.sh clean
sh build.sh config "${BASEDIR}"
sh build.sh build
sh build.sh install
