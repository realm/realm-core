#!/bin/bash

# Stop the fuzzers and use the default executable to convert any found crashes and hangs to cpp unit tests

DIR=$(dirname ${BASH_SOURCE[0]})
executable_path="fuzz-group"
unit_tests_path="cpp_unit_tests/"

${DIR}/stop_parallel_fuzzer.sh "$executable_path" "$unit_tests_path"
