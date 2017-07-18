#!/usr/bin/env bash

SCRIPT=$(basename "${BASH_SOURCE[0]}")
cd "$(dirname ${BASH_SOURCE[0]})"
FUZZPREFIX=$(git rev-parse --show-prefix)
ROOT_DIR=$(git rev-parse --show-toplevel)
BUILD_DIR="build.afl"
FINDINGS_DIR=${ROOT_DIR}/${BUILD_DIR}/findings

if [ "$#" -ne 2 ]; then
    echo "Usage: sh ${SCRIPT} executable_path (e.g. group) output_directory"
    exit 1
fi
fuzz_test="$1"
unit_tests_path="$2"

echo "Killing all running fuzzers"
pkill afl-fuzz

echo "Cleaning up the queues to free disk space, inodes (this may take a while)"
rm -rf "${FINDINGS_DIR}/*/queue"

# Remove any previously minimized cases
rm -rf "${FINDINGS_DIR}/*/*/*.minimized"

echo "Removing any leftover Realm files"
find "${ROOT_DIR}/${BUILD_DIR}" -type f -name "fuzzer*.realm*" -delete

# Find all interesting cases
files=($(find ${FINDINGS_DIR} -path "*/hangs/id:*" -or -path "*/crashes/id:*"))

num_files=${#files[@]}

if [[ $num_files -eq 0 ]]; then
    echo "No crashes or hangs found."
    exit 0
fi

# see also start_parallel_fuzzer.sh
time_out="10000" # ms, 10x
memory="300" # MB

mkdir -p ${FINDINGS_DIR}/${unit_tests_path}
EXEC=$(find ${ROOT_DIR}/${BUILD_DIR} -name ${fuzz_test})

# This loop was deliberately changed into doing two things in order to get
# cpp files as early as possible
echo "Minimizing and converting $num_files found crashes and hangs"
for file in "${files[@]}"; do
	# Let AFL try to minimize each input before converting to .cpp
	minimized_file="$file.minimized"
    afl-tmin -t "$time_out" -m "$memory" -i "$file" -o "$minimized_file" "${EXEC}" @@
    test $? -eq 1 && exit 1 # terminate if afl-tmin is being terminated

    # Convert into cpp file
    cpp_file=${FINDINGS_DIR}/${unit_tests_path}$(basename $file).cpp
    ${EXEC} ${minimized_file} --log > ${cpp_file}
    test $? -eq 1 && exit 1 # terminate if afl-tmin is being terminated
done
