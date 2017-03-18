#!/bin/sh
#
# See ./util/build-benchmarks.sh --help for documentation.

showUsage () {
  cat <<EOF
Usage: $0 [-h|--help] [dir] [dest]
EOF
}

showHelp () {
  echo ""
  showUsage
  echo ""
  cat <<EOF
./util/build-benchmarks.sh

This script builds and runs benchmarks used in the performance scripts.
This is currently benchmark-common-tasks and benchmark-crud.
It assumes that the current directory is the top level to build from,
unless [dir] is specified.
It will copy the benchmark results into named folders in [dest] which
is set to "benchmark_results" by default.

Examples:

$ ./test/bench/util/build_benchmarks.sh  # builds from current directory
$ ./util/build_benchmarks.sh ../..       # builds benchmarks from top dir
$ ./util/build_benchmarks.sh ../.. res   # builds benchmkars from top dir into res

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
  directory=$(pwd)
  dest=$(pwd)"/benchmark_results"
elif [ $# -eq 1 ]; then
  directory=$1
  dest=$(pwd)"/benchmark_results"
elif [ $# -eq 2 ]; then
  directory=$1
  dest=$2
fi

pushd "${directory}"

build_system="shell"
if [ -e "CMakeLists.txt" ] && [ ! -e "build.sh" ]; then
  build_system="cmake"
fi

if [ "$build_system" == "cmake" ]; then
  # for cmake we operate in a separate build directory
  mkdir -p build
  pushd build
  cmake ..
  make realm-benchmark-common-tasks
  # -x flag checks if the file exists and is executable
  if [ -x test/benchmark-common-tasks/realm-benchmark-common-tasks ]; then
    pushd test/benchmark-common-tasks
    ./realm-benchmark-common-tasks
    popd
  else
    echo "Could not run benchmark-common-tasks!"
    exit 1
  fi
  make realm-benchmark-crud
  if [ -x test/benchmark-crud/realm-benchmark-crud ]; then
    pushd test/benchmark-crud
    ./realm-benchmark-crud
    popd
  else
    echo "Could not run benchmark-crud!"
    exit 1
  fi
  popd # build

  result_prefix="build"

elif [ "$build_system" == "shell" ]; then
  sh build.sh benchmark-common-tasks
  sh build.sh benchmark-crud
  result_prefix="."
else
  echo "Unknown build system!"
  exit 1
fi

popd # ${directory}

# copy result files to destination
mkdir -p "${dest}/benchmark-common-tasks"
mkdir -p "${dest}/benchmark-crud"
cp "${directory}/${result_prefix}"/test/benchmark-common-tasks/results* "${dest}/benchmark-common-tasks/"
cp "${directory}/${result_prefix}"/test/benchmark-crud/results* "${dest}/benchmark-crud/"

