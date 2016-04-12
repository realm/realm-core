#!/bin/sh
if [ "$#" -ne 2 ]; then
	echo "Usage sh $0 num_cores ./executable_path"
	exit 1
fi
NUM_CORES=$1
EXECUTABLE_PATH=$2

COMPILER="afl-g++"
FLAGS=""
if [ "`uname`" = Darwin ]; then
	COMPILER=afl-clang++
	FLAGS="COMPILER_IS_GCC_LIKE=yes"

	#Warning
	echo "Make sure you have run:"
	echo
	echo "SL=/System/Library; PL=com.apple.ReportCrash"
	echo "launchctl unload -w ${SL}/LaunchAgents/${PL}.plist"
	echo "sudo launchctl unload -w ${SL}/LaunchDaemons/${PL}.Root.plist"
	echo
fi

#build core
cd ../../
CXX=$COMPILER make -j check-debug-norun $FLAGS
# build fuzz target
cd -
CXX=$COMPILER make -j check-debug-norun $FLAGS

#clean the findings directory

echo "Cleaning up the findings directory"
killall afl-fuzz &>/dev/null
rm -rf findings/* &>/dev/null

# start the fuzzers (timeout 100ms)
afl-fuzz  -i testcases -o findings -M fuzzer01 $EXECUTABLE_PATH @@ > /dev/null 2>&1 &

for i in `seq 2 $NUM_CORES`;
do
	sleep 1
	afl-fuzz -t 100 -i testcases -o findings -S fuzzer$i $EXECUTABLE_PATH @@ > /dev/null 2>&1 &
done

echo
echo "Use afl-whatsup findings/ to check progress"
echo "Use killall afl-fuzz" to kill the fuzzers
echo
