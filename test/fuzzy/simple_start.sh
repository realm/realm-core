#!/bin/sh

# Start as many fuzzers as we have CPU logical processors. Use the default executable

num_fuzzers="$(getconf _NPROCESSORS_ONLN)"
executable_path="./fuzz-group"

sh start_parallel_fuzzer.sh "$num_fuzzers" "$executable_path"
