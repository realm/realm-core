#!/usr/bin/env bash

# build-core.sh
#
# This script builds a given version of core in a dedicated directory of core
# builds. This enables, for instance, comparing the performance of various of
# versions of core on the same device.

set -euo pipefail

builddir=core-builds

showUsage () {
  cat <<EOF
Usage: $0 [-h|--help] [<branch>|<commit>|<tag>]
EOF
}

showHelp () {
  echo ""
  showUsage
  echo ""
  cat <<EOF
Builds core at given <branch>, <commit>, or <tag> under
${builddir} in the current working directory.

By default, the master branch gets built.
The commit can be the 7-character commit ID.
A tag must begin with tags/ (e.g., tags/v0.97.3).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h | --help )
      showHelp
      exit 0
      ;;
    * )
      break
      ;;
  esac
done

if [ $# -gt 1 ]; then
  showUsage
  exit 1
elif [ $# -eq 0 ]; then
  ref=master
else
  ref=$1
fi

basedir="${builddir}/${ref}"
mkdir -p "${basedir}"
basedir="$(realpath "${basedir}")"

srcdir="${basedir}/src"

function checkout () {

  # Check if given "ref" is a (remote) branch, and prepend origin/ if it is.
  # Otherwise, git-checkout will complain about updating paths and switching
  # branches at the same time.
  if [ `git branch -r | grep "^\\s*origin/${ref}$"` ]; then
    remoteref="origin/${ref}"
  else
    remoteref="${ref}"
  fi

  git checkout "${remoteref}"
}

if [ ! -d "${srcdir}" ]; then
  git clone git@github.com:realm/realm-core.git "${srcdir}"
  cd "${srcdir}"
  checkout
  sh build.sh clean
  sh build.sh config "${basedir}"
else
  cd "${srcdir}"
  git fetch
  checkout
fi

sh build.sh build
sh build.sh install
