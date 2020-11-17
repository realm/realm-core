#!/bin/sh
if [ "$#" -ne 2 ]; then
    echo "Usage sh $0 num_fuzzers executable_path (e.g. ./fuzz-transform)"
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

    if [ -z ${AFL_SKIP_CPUFREQ+"1"} ]; then
        # AFL_SKIP_CPUFREQ env variable is not set -- see if we can check for the presence
        # of the performance CPU governor before AFL starts complaining about missing it
        if [ -e /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
            wrong_govs=`grep -L performance /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null`

            if [ "" != "${wrong_govs}" ]; then
                echo "----------------------------------------------------------------------------------------"
                echo "AFL requires you to either set the AFL_SKIP_CPUFREQ environment variable or set CPU"
                echo "performance governors to 'performance'"
                echo "For best performance, please run:"
                echo
                echo "cd /sys/devices/system/cpu"
                echo "sudo sh -c 'echo performance | tee cpu*/cpufreq/scaling_governor'"
                echo "----------------------------------------------------------------------------------------"
                exit 1
            fi
        fi
    fi
fi

if [ ! -f "${executable_path}" ]; then
    sh ./build_parallel_fuzzer.sh
fi

echo "Cleaning up the findings directory"

pkill afl-fuzz
rm -rf findings/*

# see also stop_parallel_fuzzer.sh
time_out="100+" # ms, ignore outliers
memory="100" # MB

echo "Starting $num_fuzzers fuzzers in parallel"

# if we have only one fuzzer
if [ $num_fuzzers -eq 1 ]; then
    afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings "$executable_path" 1 @@
    exit 0
fi

# start the fuzzers in parallel
afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings -M "fuzzer1" "$executable_path" @@ 1 >/dev/null 2>&1 &

for i in $(seq 2 $num_fuzzers);
do
    afl-fuzz -t "$time_out" -m "$memory" -i testcases -o findings -S "fuzzer$i" "$executable_path" @@ "$i" >/dev/null 2>&1 &
done

echo
echo "Use afl-whatsup findings/ to check progress"
echo
