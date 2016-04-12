#!/bin/sh
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 num_cores ./executable_path (e.g. ./fuzz-group-dbg)"
    exit 1
fi
NUM_CORES=$1
EXECUTABLE_PATH=$2

COMPILER="afl-g++"
FLAGS=""
if [ "`uname`" = "Darwin" ]; then
    COMPILER="afl-clang++"
    FLAGS="COMPILER_IS_GCC_LIKE=yes"

    # FIXME: Consider detecting if ReportCrash was already unloaded and skip this message
    #        or print and don't try to run AFL.
    echo "----------------------------------------------------------------------------------------"
    echo "Make sure you have unloaded the OS X crash reporter:"
    echo
    echo "launchctl unload -w /System/Library/LaunchAgents/com.apple.ReportCrash.plist"
    echo "sudo launchctl unload -w /System/Library/LaunchDaemons/com.apple.ReportCrash.Root.plist"
    echo "----------------------------------------------------------------------------------------"
fi

# build core
cd ../../
CXX=$COMPILER make -j check-debug-norun $FLAGS

# build fuzz target
cd -
CXX=$COMPILER make -j check-debug-norun $FLAGS

# clean the findings directory
echo "Cleaning up the findings directory"
killall afl-fuzz &> /dev/null
rm -rf findings/* &> /dev/null

# start the fuzzers
TIME_OUT="100" # ms
MEMORY="100" # MB
afl-fuzz -t $TIME_OUT -m $MEMORY -i testcases -o findings -M fuzzer1 $EXECUTABLE_PATH @@ > /dev/null 2>&1 &

for i in `seq 2 $NUM_CORES`;
do
    afl-fuzz -t $TIME_OUT -m $MEMORY -i testcases -o findings -S fuzzer$i $EXECUTABLE_PATH @@ > /dev/null 2>&1 &
done

echo
echo "Use afl-whatsup findings/ to check progress"
echo "Use killall afl-fuzz to kill the fuzzers"
echo
