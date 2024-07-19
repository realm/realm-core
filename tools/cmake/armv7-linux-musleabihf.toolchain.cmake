set(_TRIPLET "armv7-unknown-linux-musleabihf")
set(_TOOLCHAIN_MD5 e5df7301cf9138ef5cae6160878cab15)
include("${CMAKE_CURRENT_LIST_DIR}/linux.toolchain.base.cmake")
set(REALM_LINUX_LIBC musl)

# Explicitly opt-in to the slower bfd linker over gold, because gold in GCC 11.2 doesn't play nice with R_ARM_REL32 relocations
set(CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_EXE_LINKER_FLAGS_INIT}")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_SHARED_LINKER_FLAGS_INIT}")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_MODULE_LINKER_FLAGS_INIT}")
