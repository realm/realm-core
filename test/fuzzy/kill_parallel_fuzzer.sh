#!/bin/sh
if [ "$#" -ne 1 ]; then
    echo "Usage sh $0 executable_path (e.g. ./fuzz-group-dbg)"
    exit 1
fi
EXECUTABLE_PATH=$1
UNIT_TESTS_PATH="findings/generated_unit_tests/"

# Kill all running fuzzers
pkill afl-fuzz

# Clean up the queues (many files, inodes)
rm -rf findings/*/queue

mkdir -p $UNIT_TESTS_PATH

# Find all interesting cases
files=`find findings \( -path "*hang/id:s*" -or -path "*crashes/id:*" \)`

# Run executable for each and capture the output
for file in $files
do
    cppfile=$UNIT_TESTS_PATH$(basename $file).cpp
    $EXECUTABLE_PATH $file --log > $cppfile
done
