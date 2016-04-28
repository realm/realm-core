#!/usr/bin/env bash

set -euo pipefail

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
