#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 executable_path (e.g. ./fuzz-group-dbg) output_directory"
    exit 1
fi
executable_path="$1"
unit_tests_path="$2"

echo "Killing all running fuzzers"
pkill afl-fuzz

echo "Cleaning up the queues to free disk space, inodes (this may take a while)"
rm -rf findings/*/queue

# Remove any previously minimized cases
rm -rf findings/*/*/*.minimized

# Find all interesting cases
files=($(find findings \( -path "*/hangs/id:*" -or -path "*/crashes/id:*" \)))

num_files=${#files[@]}

if [ $num_files -eq 0 ]; then
    echo "No crashes or hangs found."
    exit 0
fi

# see also start_parallel_fuzzer.sh
time_out="10000" # ms, 10x
memory="100" # MB

mkdir -p "$unit_tests_path"

# This loop was deliberately changed into doing two things in order to get
# cpp files as early as possible
echo "Minimizing and converting $num_files found crashes and hangs"
for file in ${files[@]}
do
	# Let AFL try to minimize each input before converting to .cpp
	minimized_file="$file.minimized"
    afl-tmin -t "$time_out" -m "$memory" -i "$file" -o "$minimized_file" "$executable_path" @@ 1
    test $? -eq 1 && exit 1 # terminate if afl-tmin is being terminated

    # FIXME: Support generating unit tests using the "trace" facility in test_fuzzer
done
