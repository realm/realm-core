#!/usr/bin/env bash

set -euo pipefail

if [ $# -lt 1 ] || [ "$1" != "nobuild" ]; then
    cd ../datetime/ && ./cmake.sh
    cd ../timestamp/ && ./cmake.sh
fi

rm -rf results.*

cat <<EOF
##############
## Baseline ##
##############
EOF
../datetime/_build/benchmark

cat <<EOF
##############
## Datetime ##
##############
EOF
../datetime/_build/benchmark

cat <<EOF
###############
## Timestamp ##
###############
EOF
../timestamp/_build/benchmark
