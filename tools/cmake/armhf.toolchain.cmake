set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)

set(CMAKE_LIBRARY_ARCHITECTURE arm-linux-gnueabihf)

set(ENV{PKG_CONFIG_SYSROOT_DIR} "/usr/lib/arm-linux-gnueabihf/")

set(THREADS_PTHREAD_ARG -pthread)