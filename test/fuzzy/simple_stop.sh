#!/bin/sh

# Stop the fuzzers and use the default executable to convert any found crashes and hangs to cpp unit tests

EXECUTABLE_PATH="./fuzz-group-dbg"
UNIT_TESTS_PATH="findings/cpp_unit_tests/"

sh stop_parallel_fuzzer.sh $EXECUTABLE_PATH $UNIT_TESTS_PATH
