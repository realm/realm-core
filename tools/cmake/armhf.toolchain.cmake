set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)

set(CMAKE_CXX_FLAGS_INIT "-Wno-psabi")

set(CMAKE_LIBRARY_ARCHITECTURE arm-linux-gnueabihf)

set(CMAKE_FIND_ROOT_PATH "/usr/${CMAKE_LIBRARY_ARCHITECTURE}")

set(ENV{PKG_CONFIG_SYSROOT_DIR} "/usr/lib/arm-linux-gnueabihf/")

set(THREADS_PTHREAD_ARG -pthread)

set(REALM_USE_SYSTEM_OPENSSL ON)
