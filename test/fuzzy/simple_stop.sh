#!/bin/sh

# Stop the fuzzers and use the default executable to convert any found crashes and hangs to cpp unit tests

executable_path="./fuzz-group"
unit_tests_path="findings/cpp_unit_tests/"

bash stop_parallel_fuzzer.sh "$executable_path" "$unit_tests_path"
