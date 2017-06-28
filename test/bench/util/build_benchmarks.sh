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

topdir="$(pwd)"
cd "${directory}" || exit 1

build_system="shell"
if [ -e "CMakeLists.txt" ] && [ ! -e "build.sh" ]; then
  build_system="cmake"
fi

if [ "$build_system" = "cmake" ]; then
  # for cmake we operate in a separate build directory
  mkdir -p build
  cd build || exit 1
  # for consistency with pre cmake builds we don't benchmark with encryption
  cmake -D REALM_ENABLE_ENCRYPTION=OFF -D CMAKE_BUILD_TYPE=Release ..
  make realm-benchmark-common-tasks
  # -x flag checks if the file exists and is executable
  if [ -x ./test/benchmark-common-tasks/realm-benchmark-common-tasks ]; then
    cd test/benchmark-common-tasks || exit 1
    ./realm-benchmark-common-tasks
    cd ../../ || exit 1
  else
    echo "Could not run benchmark-common-tasks!"
    pwd
    exit 1
  fi
  make realm-benchmark-crud
  if [ -x ./test/benchmark-crud/realm-benchmark-crud ]; then
    cd test/benchmark-crud || exit 1
    ./realm-benchmark-crud
    cd ../.. || exit 1
  else
    echo "Could not run benchmark-crud!"
    exit 1
  fi
  make realm-stats
  if [ -x ./test/benchmark-common-tasks/realm-stats ]; then
    cd test/benchmark-common-tasks/ || exit 1
    python collect_stats.py --build-root-dir "../../" --source-root-dir "../../../"
    cd ../.. || exit 1
else
    echo "Could not run realm-stats!"
    exit 1
  fi
  cd .. || exit 1 # build

  result_prefix="build"

elif [ "$build_system" = "shell" ]; then

  sh build.sh build # we need to generate librealm.a for stats

  sh build.sh benchmark-common-tasks

  cd test/benchmark-common-tasks/ || exit 1
  python collect_stats.py --build-root-dir "../../" --source-root-dir "../../"
  cd ../.. || exit 1

  sh build.sh benchmark-crud
  result_prefix="."
else
  echo "Unknown build system!"
  exit 1
fi

cd "${topdir}" || exit 1

# copy result files to destination
mkdir -p "${dest}/benchmark-common-tasks"
mkdir -p "${dest}/benchmark-crud"
cp "${directory}/${result_prefix}"/test/benchmark-common-tasks/results* "${dest}/benchmark-common-tasks/"
cp "${directory}/${result_prefix}"/test/benchmark-common-tasks/stats.txt "${dest}/benchmark-common-tasks/"
cp "${directory}/${result_prefix}"/test/benchmark-crud/results* "${dest}/benchmark-crud/"

