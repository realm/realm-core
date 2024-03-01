set(_TRIPLET "armv7-unknown-linux-gnueabihf")
set(_TOOLCHAIN_MD5 fbf817b1428bb35c93be8e6c15f73d7d)
include("${CMAKE_CURRENT_LIST_DIR}/linux.toolchain.base.cmake")

# Explicitly opt-in to the slower bfd linker over gold, because gold in GCC 11.2 doesn't play nice with R_ARM_REL32 relocations
set(CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_EXE_LINKER_FLAGS_INIT}")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_SHARED_LINKER_FLAGS_INIT}")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-fuse-ld=bfd ${CMAKE_MODULE_LINKER_FLAGS_INIT}")
