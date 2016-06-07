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

echo "Removing any leftover Realm files"
rm -rf fuzzer*.realm*

# Find all interesting cases
files=($(find findings \( -path "*hang/id:s*" -or -path "*crashes/id:*" \)))

num_files=${#files[@]}

if [ $num_files -eq 0 ]; then
    echo "No crashes or hangs found."
    exit 0
fi

echo "Minimizing $num_files found crashes and hangs"

for file in ${files[@]}
do
	afl-tmin -i "$file" -o "$file.minimized" "$executable_path" @@
done

echo "Converting $num_files found crashes and hangs into .cpp unit tests in \"$unit_tests_path\""

mkdir -p "$unit_tests_path"

# Run executable for each and capture the output
for file in ${files[@]}
do
    cpp_file="$unit_tests_path$(basename $file).cpp"
    "$executable_path" "$file.minimized" --log > "$cpp_file"
done
