#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 executable_path (e.g. ./fuzz-group-dbg) output_directory"
    exit 1
fi
EXECUTABLE_PATH=$1
UNIT_TESTS_PATH=$2

echo "Killing all running fuzzers"
pkill afl-fuzz

echo "Cleaning up the queues to free disk space, inodes"
rm -rf findings/*/queue

echo "Removing any leftover Realm files"
rm -rf fuzzer*.realm*

# Find all interesting cases
FILES=(`find findings \( -path "*hang/id:s*" -or -path "*crashes/id:*" \)`)

NUM_FILES=${#FILES[@]}

if [ $NUM_FILES -eq 0 ]; then
    echo "No crashes or hangs found."
    exit 0
fi

echo "Converting ${NUM_FILES} found crashes and hangs into .cpp unit tests in ${UNIT_TESTS_PATH}"

mkdir -p $UNIT_TESTS_PATH

# Run executable for each and capture the output
for FILE in $FILES
do
    CPP_FILE=$UNIT_TESTS_PATH$(basename $FILE).cpp
    $EXECUTABLE_PATH $FILE --log > $CPP_FILE
done
