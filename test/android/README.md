# On-Target Unit Testing [Android]

## Assumptions

This project assumes the building for ARM architecture.

If you want to build it for Intel or MIPS then you will have to edit jni/Android.mk.
Precisely, the library name at the end of this line:

```
[...]
LOCAL_LDLIBS     := -llog -landroid -lrealm-android-arm
[...]
```

## Prerequisites

 * Android SDK
 * Android NDK
 * The core static library created with:

 ```
 $ sh build.sh build-android
 ```

## Setting up the project

From the test/android folder:

```
$ android update project -p .
```

This will setup the build.xml and local.properties files

## Building the native code

From the test/android/jni folder:

```
$ ndk-build
```

It is possible to see the actual commands being issued with:

```
$ ndk-build V=1
```

This will create the shared library.

## Building the 

From the test/android folder:

```
$ ant debug
```

## Installing the app on the device

From the test/android folder:

```
$ ant installd
```

Of course make sure the device is set to developer mode and connected.

## Launch the app

You can of course tap the app icon. Alternatively you can do it from the command line:

```
$ adb shell am start -a android.intent.action.MAIN -n com.tightdb.test/android.app.NativeActivity
```

## See the logs

This is as easy as:

```
$ adb logcat
```

## Retrieving the XML file containing the test results:

```
$ adb pull /storage/sdcard0/Android/data/com.tightdb.test/files/unit-test-report.xml .
```

