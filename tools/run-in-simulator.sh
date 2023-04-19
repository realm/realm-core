#!/bin/bash

set -xeo pipefail

appPath="$1"
bundleId="$2"
outputFile="$3"

id="$(echo $RANDOM | shasum | cut -f 1 -d ' ')"
# Sample output: "iOS 15.5 (15.5 - 19F70) - com.apple.CoreSimulator.SimRuntime.iOS-15-5"
# We're extracting the part starting with com.apple, looking for a runtime that's iOS 13 or newer
runtimeId="$(xcrun simctl list runtimes | grep -o 'com.apple.CoreSimulator.SimRuntime.iOS-1[3-9][0-9-]*' | head -n 1)"

xcrun simctl create "$id" com.apple.CoreSimulator.SimDeviceType.iPhone-11 "$runtimeId"
xcrun simctl boot "$id"

xcrun simctl install "$id" "$appPath"
xcrun simctl launch --console-pty "$id" "$bundleId"

# When running inside a VM, simulators don't flush disk writes immediately even
# with F_FULLFSYNC, and any unflushed writes are lost entirely if the simulator
# is shut down. As a result, we need to wait for the test results file to
# actually appear on disk before continuing.
# Wait 5 seconds before checking the first time
sleep 5
while [ ! -s "$outputFile" ] && [ $((retries++)) -lt 100 ]; do
    sleep 5
done

xcrun simctl shutdown "$id"
xcrun simctl delete "$id"
