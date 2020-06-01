#!/bin/sh
#
# See ./gen_bench.sh --help for documentation.


#The first line of the file "benchmark_version" holds the
#version number, see docs in that file.
BENCH_VERSION=$(head -n 1 benchmark_version)

show_usage () {
  cat <<EOF
Usage: $0 [-h|--help] [<branch>|<commit>|<tag>]
EOF
}

show_help () {
  echo ""
  show_usage
  echo ""
  cat <<EOF
gen_bench.sh

This script generates the benchmark results for the given version of core
(branch, commit, or tag) and places the results in the directory specified
by REALM_BENCH_DIR (defaults to "~/.realm/core/benchmarks/"). If the results
of the benchmarks on this machine already exist there, the benchmarks are not
run. If no version of core is specified, HEAD is assumed.

Examples:

$ ./gen_bench.sh # HEAD is assumed by default.
$ ./gen_bench.sh tags/v0.97.3 # Tags must be prefixed with "tags/".
$ ./gen_bench.sh ea310804 # Can be a short commit ID.
$ ./gen_bench.sh 32b3b79d2ab90e784ad5f14f201d682be9746781

EOF
}

get_machid () {
    if [ ! -z "${REALM_BENCH_MACHID}" ]; then
        machid="${REALM_BENCH_MACHID}"
    elif [ -f "/var/lib/dbus/machine-id" ]; then
        machid=$(cat /var/lib/dbus/machine-id)
    elif [ -f "/etc/machine-id" ]; then
        machid=$(cat /etc/machine-id)
    elif [ -f "/etc/hostname" ]; then
        machid=$(cat /etc/hostname)
    else
        machid=$(ifconfig en0 | awk '/ether/{print $2}')
    fi
    if [ -z "${machid}" ]; then
        machid="unknown"
    fi
    echo "using machine id: ${machid}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h | --help )
      show_help
      exit 0
      ;;
    * )
      break
      ;;
  esac
done

if [ $# -gt 1 ]; then
    show_usage
    exit 1
elif [ $# -eq 0 ]; then
    ref=$(git rev-parse HEAD)
else
    ref=$1
fi

#get the hash from nice names like tags/v2.0.0
remoteref=$(git rev-list -n 1 "${ref}")
ret=$?
if [ $ret -gt 0 ]; then
    echo "could not parse ref ${ref} exiting"
    exit 1
fi
unixtime=$(git show -s --format=%at "${remoteref}")

if [ -z "$REALM_BENCH_DIR" ]; then
    REALM_BENCH_DIR=~/.realm/core/benchmarks
fi

get_machid
basedir="${REALM_BENCH_DIR}/${BENCH_VERSION}/${machid}"
mkdir -p "${basedir}"
outputfile="${basedir}/${unixtime}_${remoteref}.csv"
statsfile="${basedir}/${unixtime}_${remoteref}.stats"

# if the file doesn't exist, create it and write the output dir as the first line
if [ ! -e "recent_results.txt" ] ; then
    echo "${basedir}" > recent_results.txt
fi
echo "${outputfile}" >> recent_results.txt
echo "${statsfile}" >> recent_results.txt

if [ -f "${outputfile}" ]; then
    echo "found results, skipping ${outputfile}"
else
    headref=$(git rev-parse HEAD)
    build_bench_script=$(pwd)/util/build_benchmarks.sh
    if [ "${headref}" = "${remoteref}" ]; then
        echo "building HEAD"
        cd ../.. || exit 1
    else
        rootdir=$(git rev-parse --show-toplevel)
        REALM_BENCH_CHECKOUT_ONLY=1 sh ./util/build_core.sh "${remoteref}" "${rootdir}"
        CHECKOUT_DIR="core-builds/${remoteref}"
        if [ ! -d "${CHECKOUT_DIR}" ]; then
            echo "fatal error: core checkout failed on ref: ${remoteref}"
            ls -lah
            exit 0
        fi
        REALM_VERSION_MAJOR=$(sed -n "s/^VERSION=\([0-9]*\).\([0-9]*\).\([0-9]*\).*$/\1/p" "${CHECKOUT_DIR}/src/dependencies.list")
        REALM_VERSION_MINOR=$(sed -n "s/^VERSION=\([0-9]*\).\([0-9]*\).\([0-9]*\).*$/\2/p" "${CHECKOUT_DIR}/src/dependencies.list")
        REALM_VERSION_PATCH=$(sed -n "s/^VERSION=\([0-9]*\).\([0-9]*\).\([0-9]*\).*$/\3/p" "${CHECKOUT_DIR}/src/dependencies.list")
        echo "Versions found: ${REALM_VERSION_MAJOR}.${REALM_VERSION_MINOR}.${REALM_VERSION_PATCH}"
        if [ -z "${REALM_VERSION_MAJOR}" ]; then
            echo "fatal error: could not parse version from dependencies.list: ${CHECKOUT_DIR}"
            ls -lah "${CHECKOUT_DIR}/src"
            exit 0
        fi

        cd ../benchmark-common-tasks || exit 1
        BCT_DIR="../bench/core-builds/${remoteref}/src/test/benchmark-common-tasks"
        cp main.cpp compatibility.hpp stats.cpp collect_stats.py "${BCT_DIR}"
        # we need to modify the build rules to build stats on old core versions
        # we will either need the makefile (with build.sh) or the CMakeLists.txt (with cmake)
        cp compatibility_makefile "${BCT_DIR}/Makefile"
        cp CMakeLists.txt "${BCT_DIR}/"
        if [ $(( REALM_VERSION_MAJOR < 6 || ( REALM_VERSION_MAJOR == 6 && REALM_VERSION_MINOR == 0 && REALM_VERSION_PATCH <= 1) )) -eq 1 ] ; then
            # in 6.0.1 the CMake target changed from "Core" to "Storage" so we need to change it back for older versions
            echo "compatibility pre 6.0.1 cmake targets Realm and not Storage"
            sed -i.bak -e "s/Storage/Core/g" "${BCT_DIR}/CMakeLists.txt"
        fi
        if [ $(( REALM_VERSION_MAJOR < 5 || ( REALM_VERSION_MAJOR == 5 && REALM_VERSION_MINOR < 4 ) || ( REALM_VERSION_VERSION_MAJOR == 5 && REALM_VERSION_MINOR == 4 && REALM_VERSION_PATCH <= 1 ) )) -eq 1 ]; then
            echo "compatibility pre 5.4.1"
            sed -i.bak -e "s/TestUtil/test-util/g" "${BCT_DIR}/CMakeLists.txt"
            sed -i.bak -e "s/Core/realm/g" "${BCT_DIR}/CMakeLists.txt"
        fi
        echo "unix timestamp of build is ${unixtime}"
        # The breaking change of SharedGroup construction syntax occured after tags/v2.0.0-rc2, we must use a legacy
        # adaptor for constructing SharedGroups in revisions of core before this time.
        if [ "${unixtime}" -lt "1473070980" ]; then
            echo "Using legacy compatibility of SharedGroup"
            cp compatibility_legacy.cpp "${BCT_DIR}/compatibility.cpp"
        else
            echo "Using normal compatibility of SharedGroup"
            cp compatibility.cpp "${BCT_DIR}/"
        fi
        cd ../benchmark-crud || exit 1
        cp main.cpp "../bench/core-builds/${remoteref}/src/test/benchmark-crud/"
        cd ../util || exit 1
        cp benchmark_results.hpp benchmark_results.cpp "../bench/core-builds/${remoteref}/src/test/util/"
        cd "../bench/core-builds/${remoteref}/src/" || exit 1
    fi
    # input 1: path to top level of checkout to build, input 2: destination for results
    sh "${build_bench_script}" . bench_results
    ret=$?
    if [ $ret -gt 0 ]; then
        echo "Error building benchmarks! Exiting."
        exit 1
    fi
    echo "writing results to ${outputfile}"
    # print common header
    head -n 1 "bench_results/benchmark-common-tasks/results.latest.csv" > "${outputfile}"
    # print contents, add _EncryptionOff tag to names without encryption (backwards compatibility)
    tail -n +2 "bench_results/benchmark-common-tasks/results.latest.csv" | perl -wpe "s/^\"(((?!EncryptionO[nf]+).)*)\"/\"\$1_EncryptionOff\"/" >> "${outputfile}"
    tail -n +2 "bench_results/benchmark-crud/results.latest.csv" | perl -wpe "s/^\"(((?!EncryptionO[nf]+).)*)\"/\"\$1_EncryptionOff\"/" >> "${outputfile}"

    # copy the statistics file to the results directory
    cp "bench_results/benchmark-common-tasks/stats.txt" "${statsfile}"

    if [ "${headref}" != "${remoteref}" ]; then
        cd ../.. || exit 1
        pwd
        echo "cleaning up: ${remoteref}"
        rm -rf "${remoteref}"
    else
        echo "done"
    fi
fi

