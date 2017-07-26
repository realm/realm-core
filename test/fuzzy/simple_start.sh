#!/bin/bash

# Start as many fuzzers as we have CPU logical processors. Use the default executable
DIR=$(dirname ${BASH_SOURCE[0]})
num_fuzzers="$(getconf _NPROCESSORS_ONLN)"
executable_path="fuzz-group"

${DIR}/start_parallel_fuzzer.sh "$num_fuzzers" "$executable_path"
