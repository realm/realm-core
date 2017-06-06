APP_PLATFORM := android-10

APP_STL := gnustl_static
APP_CPPFLAGS += -fexceptions
APP_CPPFLAGS += -frtti
APP_ABI := arm64-v8a

# Must match the toolchain version used by core
NDK_TOOLCHAIN_VERSION := 4.9
