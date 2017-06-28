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
        if [ ! -d "core-builds/${remoteref}" ]; then
            echo "fatal error: core checkout failed on ref: ${remoteref}"
            ls -lah
            exit 0
        fi
        cd ../benchmark-common-tasks || exit 1
        cp main.cpp compatibility.hpp stats.cpp collect_stats.py "../bench/core-builds/${remoteref}/src/test/benchmark-common-tasks"
        cp compatibility_makefile "../bench/core-builds/${remoteref}/src/test/benchmark-common-tasks/Makefile"
        echo "unix timestamp of build is ${unixtime}"
        # The breaking change of SharedGroup construction syntax occured after tags/v2.0.0-rc2, we must use a legacy
        # adaptor for constructing SharedGroups in revisions of core before this time.
        if [ "${unixtime}" -lt "1473070980" ]; then
            echo "Using legacy compatibility of SharedGroup"
            cp compatibility_legacy.cpp "../bench/core-builds/${remoteref}/src/test/benchmark-common-tasks/compatibility.cpp"
        else
            echo "Using normal compatibility of SharedGroup"
            cp compatibility.cpp "../bench/core-builds/${remoteref}/src/test/benchmark-common-tasks/"
        fi
        cd ../benchmark-crud || exit 1
        cp main.cpp "../bench/core-builds/${remoteref}/src/test/benchmark-crud/"
        cp compatibility_makefile "../bench/core-builds/${remoteref}/src/test/benchmark-crud/Makefile"
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

