#!/bin/sh
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 num_fuzzers executable_path (e.g. ./fuzz-group-dbg)"
    exit 1
fi
num_fuzzers="$1"
executable_path="$2"

compiler="afl-g++"
flags="COMPILER_IS_GCC_LIKE=yes"

if [ "$(uname)" = "Darwin" ]; then
    compiler="afl-clang++"

    # FIXME: Consider detecting if ReportCrash was already unloaded and skip this message
    #        or print and don't try to run AFL.
    echo "----------------------------------------------------------------------------------------"
    echo "Make sure you have unloaded the OS X crash reporter:"
    echo
    echo "launchctl unload -w /System/Library/LaunchAgents/com.apple.ReportCrash.plist"
    echo "sudo launchctl unload -w /System/Library/LaunchDaemons/com.apple.ReportCrash.Root.plist"
    echo "----------------------------------------------------------------------------------------"
else
    # FIXME: Check if AFL works if the core pattern is different, but does not start with | and test for that
    if [ "$(cat /proc/sys/kernel/core_pattern)" != "core" ]; then
        echo "----------------------------------------------------------------------------------------"
        echo "AFL might mistake crashes with hangs if the core is outputed to an external process"
        echo "Please run:"
        echo
        echo "sudo sh -c 'echo core > /proc/sys/kernel/core_pattern'"
        echo "----------------------------------------------------------------------------------------"
        exit 1
    fi
fi

echo "Building core"

cd ../../
REALM_MAX_BPNODE_SIZE_DEBUG=4 REALM_ENABLE_ENCRYPTION=yes sh build.sh config
CXX="$compiler" REALM_HAVE_CONFIG=yes make -j check-debug-norun "$flags"

echo "Building fuzz target"

cd -
CXX="$compiler" make -j check-debug-norun "$flags"

echo "Cleaning up the findings directory"

pkill afl-fuzz
rm -rf findings/*

# see also stop_parallel_fuzzer.sh
time_out="1000" # ms
memory="100" # MB

echo "Starting $num_fuzzers fuzzers in parallel"

# if we have only one fuzzer
if [ $num_fuzzers -eq 1 ]; then
    afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings "$executable_path" @@
    exit 0
fi

# start the fuzzers in parallel
afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings -M "fuzzer1" "$executable_path" @@ --name "fuzzer1" >/dev/null 2>&1 &

for i in $(seq 2 $num_fuzzers);
do
    afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings -S "fuzzer$i" "$executable_path" @@ --name "fuzzer$i" >/dev/null 2>&1 &
done

echo
echo "Use afl-whatsup findings/ to check progress"
echo
