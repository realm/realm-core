#!/bin/sh
#
# See ./util/gen-bench.sh --help for documentation.

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
./util/build-core.sh

This script generates the benchmark results for the given version of core
(branch, commit, or tag) and places the results in the directory specified
by REALM_BENCH_DIR (defaults to "~/.realm/core/benchmarks/"). If the results
of the benchmarks on this machine already exist there, the benchmarks are not
run. If no version of core is specified, HEAD is assumed.

Examples:

$ ./util/build-core.sh # HEAD is assumed by default.
$ ./util/build-core.sh tags/v0.97.3 # Tags must be prefixed with "tags/".
$ ./util/build-core.sh ea310804 # Can be a short commit ID.
$ ./util/build-core.sh 32b3b79d2ab90e784ad5f14f201d682be9746781

EOF
}

get_machid () {
    if [ -f "/var/lib/dbus/machine-id" ]; then
        machid=$(cat /var/lib/dbus/machine-id)
    elif [ -f "/etc/machine-id" ]; then
        machid=$(cat /etc/machine-id)
    else
        machid=$(ifconfig en0 | awk '/ether/{print $2}')
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
unixtime=$(git show -s --format=%at ${remoteref})


if [ -z "$REALM_BENCH_DIR" ]; then
    REALM_BENCH_DIR=~/.realm/core/benchmarks
fi

get_machid
basedir="${REALM_BENCH_DIR}/${machid}"
mkdir -p "${basedir}"
outputfile="${basedir}/${unixtime}_${remoteref}.csv"

if [ -f "${outputfile}" ]; then
    echo "found results, skipping ${outputfile}"
else
    headref=$(git rev-parse HEAD)
    if [ "${headref}" == "${remoteref}" ]; then
        cd ../..
    else
        sh ./util/build-core.sh "${remoteref}"
        cd "./core-builds/${remoteref}/src/"
    fi
    sh build.sh benchmark-common-tasks
    sh build.sh benchmark-crud
    echo "writing results to ${outputfile}"
    # print common header
    head -n 1 "test/benchmark-common-tasks/results.latest.csv" > "${outputfile}"
    # print contents, add _EncryptionOff tag to names without encryption (backwards compatibility)
    tail -n +2 "test/benchmark-common-tasks/results.latest.csv" | perl -wpe "s/^\"(((?!EncryptionO[nf]+).)*)\"/\"\$1_EncryptionOff\"/" >> "${outputfile}"
    tail -n +2 "test/benchmark-crud/results.latest.csv" | perl -wpe "s/^\"(((?!EncryptionO[nf]+).)*)\"/\"\$1_EncryptionOff\"/" >> "${outputfile}"

    if [ "${headref}" != "${remoteref}" ]; then
        cd ../..
        pwd
        echo "cleaning up: ${remoteref}"
        rm -rf "${remoteref}"
    else
        echo "done"
    fi
fi

