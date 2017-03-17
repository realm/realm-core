#!/bin/sh
#
# See ./util/gen_bench_hist.sh --help for documentation.

show_usage () {
  cat <<EOF
Usage: $0 [-h|--help] [base branch]
EOF
}

show_help () {
  echo ""
  show_usage
  echo ""
  cat <<EOF
./gen_bench_hist.sh

This script runs the benchmarks on each version of core specified in the
file revs_to_benchmark.txt. The benchmarks of a revision are not run if
the benchmarks of that revision are already found in the results folder.
The results are then combined by function using a script to generate a
graph per benchmark function which shows performance across revisions.

If the optional [base branch] is specified this script will also try
to find the last common commit and benchmark that for comparison.
For example "master", "develop", or a PR that you intend to merge into.

EOF
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
elif [ $# -eq 1 ]; then
  destination=$1
  forked=$(git merge-base "${destination}" HEAD)
  ret=$?
  if [ $ret -gt 0 ]; then
      echo "Error: could not find where this branch forked from ${destination}. Continuing."
      forked=""
  fi
fi

# start fresh list of csv files written in this run. Each file
# written by gen_bench.sh will append a line to this file
rm recent_results.txt

grep -v -e "^ *#" -e "^ *$" revs_to_benchmark.txt | while read -r p; do
  echo "$p"
  sh gen_bench.sh "$p"
done

if [ -n "${forked}" ]; then
  echo "${forked}"
  sh gen_bench.sh "${forked}"
fi

echo "HEAD"
sh gen_bench.sh HEAD

# remove csv files not generated in this run, namely other
# HEAD versions. This means we only keep files noted in
# revs_to_benchmark. The git sha of HEAD is different per run.
results_dir=$(head -n 1 recent_results.txt)
for file in ${results_dir}/*.csv; do
  grep -q -F "${file}" recent_results.txt || (rm "${file}" && echo "removed old result: ${file}")
done

