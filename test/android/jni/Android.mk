LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)

# Copy core to here - ARM only (see APP_ABI variable in Application.mk)
LOCAL_MODULE     := realm-android
LOCAL_SRC_FILES  := ../../../android-lib/librealm-android-arm.a
include $(PREBUILT_STATIC_LIBRARY)

include $(CLEAR_VARS)

# build unit test and native activity - and link with core
TESTS := $(wildcard ../../test_*.cpp ../../large_tests/*.cpp ../../util/*.cpp)

LOCAL_MODULE     := native-activity
LOCAL_SRC_FILES  := $(TESTS) main.cpp
LOCAL_C_INCLUDES := $(LOCAL_PATH)/../../ ../../../src
LOCAL_CFLAGS     := -std=c++11 -DREALM_HAVE_CONFIG
LOCAL_CPPFLAGS   := -std=c++11 -DREALM_HAVE_CONFIG
LOCAL_LDLIBS     := -llog -landroid
LOCAL_LDFLAGS    += -L../../android-lib
LOCAL_STATIC_LIBRARIES += android_native_app_glue realm-android

include $(BUILD_SHARED_LIBRARY)

$(call import-module,android/native_app_glue)
