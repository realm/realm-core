#!/usr/bin/env bash
#
# See ./util/gen-bench-hist.sh --help for documentation.

set -euo pipefail

showUsage () {
  cat <<EOF
Usage: $0 [-h|--help]
EOF
}

showHelp () {
  echo ""
  showUsage
  echo ""
  cat <<EOF
./gen-bench-hist.sh

This script runs the benchmarks on each version of core specified in the
file revs_to_benchmark.txt plus the current branch. The benchmarks of a
revision are not run if the benchmarks of that revision are already found in
the results folder. The results are then combined by function using a script
to generate a graph per benchmark function which shows performance across
revisions.

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

if [ $# -gt 0 ]; then
  showUsage
  exit 1
elif [ $# -eq 0 ]; then
  ref=master
else
  ref=$1
fi

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


while read p; do
  echo $p
  sh gen-bench.sh $p
done <revs_to_benchmark.txt

