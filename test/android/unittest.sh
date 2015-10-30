#!/bin/bash

adb logcat -c
adb shell am start -a android.intent.action.MAIN -n io.realm.test/android.app.NativeActivity
while [ true ]; do
    sleep 10
    report=$(adb logcat -d | grep "The XML file" | cut -d/ -f3- | tr -d "\r")
    if [ "$report" != "" ]; then

        # adb pull <full-path-for-report-file>
        adb shell am force-stop io.realm.test
        break
    fi
done
