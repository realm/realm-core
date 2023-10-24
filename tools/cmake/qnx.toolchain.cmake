set(CMAKE_SYSTEM_NAME QNX)
set(CMAKE_SYSTEM_VERSION 7.1)
set(CMAKE_CROSSCOMPILING TRUE)
set(QNX TRUE)

set(CMAKE_SYSTEM_PROCESSOR x86_64 CACHE STRING "Target architecture")

#set(QNX_BASE "${CMAKE_CURRENT_LIST_DIR}" CACHE STRING "Path to the QNX installation")
set(QNX_TARGET "${QNX_BASE}/target/qnx7")
if(CMAKE_HOST_SYSTEM_NAME STREQUAL Darwin)
  set(QNX_HOST "${QNX_BASE}/host/darwin/x86_64")
elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL Linux)
  set(QNX_HOST "${QNX_BASE/host/linux}/x86_64")
elseif(CMAKE_HOST_SYSTEM_NAME STREQUAL Windows)
  set(QNX_HOST "${QNX_BASE}/host/win64/x86_64")
else()
  message(FATAL_ERROR "Unknown QNX host")
endif()

set(CMAKE_C_COMPILER ${QNX_HOST}/usr/bin/qcc)
set(CMAKE_C_COMPILER_TARGET gcc_nto${CMAKE_SYSTEM_PROCESSOR})

set(CMAKE_CXX_COMPILER ${QNX_HOST}/usr/bin/q++)
set(CMAKE_CXX_COMPILER_TARGET gcc_nto${CMAKE_SYSTEM_PROCESSOR})

# set(CMAKE_AR ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}ar)
# set(CMAKE_ASM_COMPILER ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}gcc)
# set(CMAKE_LINKER ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}ld)
# set(CMAKE_NM ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}nm)
# set(CMAKE_OBJCOPY ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}objcopy)
# set(CMAKE_OBJDUMP ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}objdump)
# set(CMAKE_RANLIB ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}ranlib)
# set(CMAKE_READELF ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}readelf)
# set(CMAKE_SIZE ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}size)
# set(CMAKE_STRIP ${QNX_HOST}/usr/bin/nto${CMAKE_SYSTEM_PROCESSOR}strip)

# file(GLOB_RECURSE libgcc_a 
#   "${QNX_HOST}/usr/lib/gcc/${CMAKE_SYSTEM_PROCESSOR}*/*/pic/libgcc.a")

# set(CMAKE_C_STANDARD_LIBRARIES_INIT
#   "${libgcc_a} -lc -Bstatic -lcS ${libgcc_a}")
# set(CMAKE_CXX_STANDARD_LIBRARIES_INIT
#   "-lc++ -lm ${CMAKE_C_STANDARD_LIBRARIES_INIT}")

# set(CMAKE_EXE_LINKER_FLAGS_INIT "-nodefaultlibs")
# set(CMAKE_SHARED_LINKER_FLAGS_INIT "-nodefaultlibs")
# set(CMAKE_MODULE_LINKER_FLAGS_INIT "-nodefaultlibs")

set(CMAKE_SYSROOT "${QNX_TARGET}")

# Set the target directories
list(APPEND CMAKE_FIND_ROOT_PATH
  "${QNX_TARGET}/${CMAKE_SYSTEM_PROCESSOR}"
  "${QNX_TARGET}"
)

# Search for programs in the build host directories.
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# Search for libraries and headers in the target directories.
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)

set(CMAKE_C_FLAGS_INIT -D_QNX_SOURCE)
set(CMAKE_CXX_FLAGS_INIT -D_QNX_SOURCE)
