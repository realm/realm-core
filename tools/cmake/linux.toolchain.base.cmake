if(NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64")
    message(FATAL_ERROR "This toolchain can only be used on x86_64 Linux.")
endif()

file(DOWNLOAD https://static.realm.io/toolchains/v3/${_TRIPLET}.tar.zst ${CMAKE_BINARY_DIR}/${_TRIPLET}.tar.zst
     EXPECTED_HASH MD5=${_TOOLCHAIN_MD5} STATUS _DOWNLOAD_STATUS)
list(GET _DOWNLOAD_STATUS 0 _DOWNLOAD_STATUS_CODE)
if(NOT ${_DOWNLOAD_STATUS_CODE} EQUAL 0)
    message(FATAL_ERROR "Error downloading ${_TRIPLET}.tar.zst: ${_DOWNLOAD_STATUS}")
endif()

file(ARCHIVE_EXTRACT INPUT ${CMAKE_BINARY_DIR}/${_TRIPLET}.tar.zst DESTINATION ${CMAKE_BINARY_DIR})
include("${CMAKE_BINARY_DIR}/${_TRIPLET}/toolchain.cmake")

set(CMAKE_EXE_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-Xlinker --exclude-libs=libgcc.a,libstdc++.a -static-libgcc -static-libstdc++")

set(REALM_LINUX_TOOLCHAIN ON)
