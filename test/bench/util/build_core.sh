#!/bin/sh
#
# See ./util/build_core.sh --help for documentation.

builddir=./core-builds

showUsage () {
  cat <<EOF
Usage: $0 [-h|--help] [<branch>|<commit>|<tag>] [local-repo-reference]
EOF
}

showHelp () {
  echo ""
  showUsage
  echo ""
  cat <<EOF
./util/build_core.sh

This script builds the given version of core (branch, commit, or tag) in a
dedicated ${builddir} directory. This enables, for instance, comparing the
performance of various of versions of core on the same machine.

The optional third parameter [local-repo-reference] can be used to specify
an existing local checkout of the same repository. Using an already
existing repository as an alternate will require fewer objects to be
copied from the repository being cloned, reducing network and local
storage costs.

You can set the environment variable REALM_BENCH_CHECKOUT_ONLY if you 
just want the checkout and don't want to actually build all of core.

Examples:

$ ./util/build_core.sh master # master is assumed by default.
$ ./util/build_core.sh tags/v0.97.3 # Tags must be prefixed with "tags/".
$ ./util/build_core.sh ea310804 # Can be a short commit ID.
$ ./util/build_core.sh 32b3b79d2ab90e784ad5f14f201d682be9746781

This results in directories:

$ ./core-builds/master
$ ./core-builds/tags/v0.97.3
$ ./core-builds/ea310804
$ ./core-builds/32b3b79d2ab90e784ad5f14f201d682be9746781
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

if [ $# -gt 2 ]; then
  showUsage
  exit 1
elif [ $# -eq 0 ]; then
  ref=master
elif [ $# -eq 1 ]; then
  ref=$1
else
  ref=$1
  localrepo=$2
fi

basedir="${builddir}/${ref}"
mkdir -p "${basedir}"
basedir="$(cd "${basedir}" && pwd -P)"

srcdir="${basedir}/src"

checkout () {

  # Check if given "ref" is a (remote) branch, and prepend origin/ if it is.
  # Otherwise, git-checkout will complain about updating paths and switching
  # branches at the same time.
  if [ "$(git branch -r | grep -q "^\\s*origin/${ref}$")" ]; then
    remoteref="origin/${ref}"
  else
    remoteref="${ref}"
  fi

  git checkout "${remoteref}"
  build_system="shell"
  if [ -e "CMakeLists.txt" ] && [ ! -e "build.sh" ]; then
    build_system="cmake"
  fi
}

clean () {
  if [ "$build_system" == "cmake" ]; then
    mkdir -p build
    pushd build
    cmake ..
    make clean
    popd
  elif [ "$build_system" == "shell" ]; then
    sh build.sh clean
  else
    echo "Unknown build system!"
    exit 1
  fi
}

configure () {
  if [ "$build_system" == "cmake" ]; then
    : # this was taken care of by cmake
  elif [ "$build_system" == "shell" ]; then
    sh build.sh config "${basedir}"
  else
    echo "Unknown build system!"
    exit 1
  fi
}

build () {
  if [ "$build_system" == "cmake" ]; then
    mkdir -p build
    pushd build
    make
    popd
  elif [ "$build_system" == "shell" ]; then
    sh build.sh build
  else
    echo "Unknown build system!"
    exit 1
  fi
}

if [ ! -d "${srcdir}" ]; then
  if [ -z "$localrepo" ]; then
    git clone https://github.com/realm/realm-core.git "${srcdir}"
  else
    git clone https://github.com/realm/realm-core.git --reference "${localrepo}" "${srcdir}"
  fi
  cd "${srcdir}"
  checkout
  clean
  configure
else
  cd "${srcdir}"
  git fetch
  checkout
fi

if [ -z "$REALM_BENCH_CHECKOUT_ONLY" ]; then
  build
fi

