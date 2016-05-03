#!/bin/sh
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 num_cores ./executable_path (e.g. ./fuzz-group-dbg)"
    exit 1
fi
NUM_CORES=$1
EXECUTABLE_PATH=$2

COMPILER="afl-g++"
FLAGS="COMPILER_IS_GCC_LIKE=yes"

if [ "`uname`" = "Darwin" ]; then
    COMPILER="afl-clang++"

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
    if [ "`cat /proc/sys/kernel/core_pattern`" != "core" ]; then
        echo "----------------------------------------------------------------------------------------"
        echo "AFL might mistake crashes with hangs if the core is outputed to an external process"
        echo "Please run:"
        echo
        echo "sudo sh -c 'echo core > /proc/sys/kernel/core_pattern'"
        echo "----------------------------------------------------------------------------------------"
        exit 1
    fi
fi

# build core
cd ../../
CXX=$COMPILER make -j check-debug-norun $FLAGS

# build fuzz target
cd -
CXX=$COMPILER make -j check-debug-norun $FLAGS

# clean the findings directory
echo "Cleaning up the findings directory"
pkill afl-fuzz
rm -rf findings/* &> /dev/null

TIME_OUT="100" # ms
MEMORY="100" # MB

# if we have only one fuzzer
if [ $NUM_CORES -eq 1 ]; then
    afl-fuzz  -t $TIME_OUT -m $MEMORY -i testcases -o findings $EXECUTABLE_PATH @@
    exit 0
fi

# start the fuzzers in parallel
afl-fuzz -t $TIME_OUT -m $MEMORY -i testcases -o findings -M fuzzer1 $EXECUTABLE_PATH @@ --name fuzzer1 &> /dev/null &

for i in `seq 2 $NUM_CORES`;
do
    afl-fuzz -t $TIME_OUT -m $MEMORY -i testcases -o findings -S fuzzer$i $EXECUTABLE_PATH @@ --name fuzzer$i &> /dev/null &
done

echo
echo "Use afl-whatsup findings/ to check progress"
echo "Use pkill afl-fuzz to kill the fuzzers"
echo
