LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)

TESTS := $(wildcard ../../test_*.cpp ../../large_tests/*.cpp ../../util/*.cpp)

LOCAL_MODULE     := native-activity
LOCAL_SRC_FILES  := $(TESTS) main.cpp
LOCAL_C_INCLUDES := $(LOCAL_PATH)/../../ ../../../src
LOCAL_LDLIBS     := -llog -landroid -lrealm-android-arm
LOCAL_LDFLAGS    += -L../../../android-lib
LOCAL_STATIC_LIBRARIES += android_native_app_glue

include $(BUILD_SHARED_LIBRARY)

$(call import-module,android/native_app_glue)
