if(NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64")
    message(FATAL_ERROR "This toolchain can only be used on x86_64 Linux.")
endif()

set(_TRIPLET "x86_64-unknown-linux-gnu")
file(DOWNLOAD https://static.realm.io/toolchains/v3/${_TRIPLET}.tar.zst ${CMAKE_BINARY_DIR}/${_TRIPLET}.tar.zst
     EXPECTED_HASH MD5=aa99fc16d85adf1bb2f8b63b83542044 STATUS _DOWNLOAD_STATUS)
list(GET _DOWNLOAD_STATUS 0 _DOWNLOAD_STATUS_CODE)
if(NOT ${_DOWNLOAD_STATUS_CODE} EQUAL 0)
    message(FATAL_ERROR "Error downloading ${_TRIPLET}.tar.zst: ${_DOWNLOAD_STATUS}")
endif()

file(ARCHIVE_EXTRACT INPUT ${CMAKE_BINARY_DIR}/${_TRIPLET}.tar.zst DESTINATION ${CMAKE_BINARY_DIR})
include("${CMAKE_BINARY_DIR}/${_TRIPLET}/toolchain.cmake")
unset(CMAKE_CROSSCOMPILING_EMULATOR)

set(CMAKE_EXE_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")
