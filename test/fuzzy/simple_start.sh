#!/bin/sh

# Start as many fuzzers as we have CPU logical processors. Use the default executable

NUM_FUZZERS=`getconf _NPROCESSORS_ONLN`
EXECUTABLE_PATH="./fuzz-group-dbg"

sh start_parallel_fuzzer.sh $NUM_FUZZERS $EXECUTABLE_PATH
