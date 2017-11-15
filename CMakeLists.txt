cmake_minimum_required(VERSION 3.8)

project(RealmCore)

list(APPEND CMAKE_MODULE_PATH "${RealmCore_SOURCE_DIR}/tools/cmake")

# Include general CMake modules
include(GNUInstallDirs)
include(CheckIncludeFiles)

# Include custom CMake modules
include(GetGitRevisionDescription)
include(Utilities)
include(SpecialtyBuilds)
include(GetVersion)

# Get accurate git-describe version
git_describe(REALM_VERSION)

# Set global compilation options (no definitions)
if(MSVC AND NOT WINDOWS_STORE)
    # Statically link the run-time library
    # https://docs.microsoft.com/bg-bg/cpp/build/reference/md-mt-ld-use-run-time-library
    # https://cmake.org/Wiki/CMake_FAQ#How_can_I_build_my_MSVC_application_with_a_static_runtime.3F
    set(CMAKE_C_FLAGS_DEBUG_INIT "/D_DEBUG /MTd /Zi /Ob0 /Od /RTC1")
    set(CMAKE_C_FLAGS_MINSIZEREL_INIT     "/MT /O1 /Ob1 /D NDEBUG")
    set(CMAKE_C_FLAGS_RELEASE_INIT        "/MT /O2 /Ob2 /D NDEBUG")
    set(CMAKE_C_FLAGS_RELWITHDEBINFO_INIT "/MT /Zi /O2 /Ob1 /D NDEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG_INIT "/D_DEBUG /MTd /Zi /Ob0 /Od /RTC1")
    set(CMAKE_CXX_FLAGS_MINSIZEREL_INIT     "/MT /O1 /Ob1 /D NDEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE_INIT        "/MT /O2 /Ob2 /D NDEBUG")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT "/MT /Zi /O2 /Ob1 /D NDEBUG")
endif()

add_compile_options(
    $<$<CXX_COMPILER_ID:MSVC>:/W3>
    $<$<AND:$<CXX_COMPILER_ID:MSVC>,$<CONFIG:Debug>>:/Z7>
    $<$<AND:$<CXX_COMPILER_ID:MSVC>,$<CONFIG:RelWithDebInfo>>:/Z7>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wextra>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wpedantic>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wundef>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wshadow>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wunreachable-code>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wshorten-64-to-32>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wold-style-cast>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wconditional-uninitialized>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wextra-semi>
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-Wno-nested-anon-types>
    $<$<BOOL:${ANDROID}>:-Wno-maybe-uninitialized>
    $<$<BOOL:${ANDROID}>:-Wno-uninitialized>
)

# Set global compile definitions
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:REALM_DEBUG=1>
    $<$<CONFIG:MinSizeDebug>:REALM_DEBUG=1>
    $<$<CXX_COMPILER_ID:MSVC>:_SCL_SECURE_NO_WARNINGS>
    $<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>
    $<$<CXX_COMPILER_ID:MSVC>:WIN32_LEAN_AND_MEAN>
    $<$<BOOL:${ANDROID}>:ANDROID>
)

set(CMAKE_DEBUG_POSTFIX "-dbg")
set(CMAKE_MINSIZEDEBUG_POSTFIX "-dbg")

# Which version of OpenSSL should Android use
if(ANDROID)
    set(ANDROID_OPENSSL_VERSION "1.0.2k")
    set(ANDROID_OPENSSL_BUILD_NUMBER "1")
endif()

# Options (passed to CMake)
option(REALM_ENABLE_ASSERTIONS "Enable assertions in release mode." OFF)
option(REALM_ENABLE_ALLOC_SET_ZERO "Zero all allocations." OFF)
option(REALM_ENABLE_ENCRYPTION "Enable encryption." ON)
option(REALM_ENABLE_MEMDEBUG "Add additional memory checks" OFF)
option(REALM_VALGRIND "Tell the test suite we are running with valgrind" OFF)
option(REALM_METRICS "Enable various metric tracking" ON)
set(REALM_MAX_BPNODE_SIZE "1000" CACHE STRING "Max B+ tree node size.")

check_include_files(malloc.h HAVE_MALLOC_H)

# Store configuration in header file
configure_file(src/realm/util/config.h.in src/realm/util/config.h)

# Configure source code to use right version number
configure_file(src/realm/version.hpp.in src/realm/version.hpp)

set(DEPRECATED_CONFIG_FILE "${RealmCore_SOURCE_DIR}/src/realm/util/config.h")
if(EXISTS "${DEPRECATED_CONFIG_FILE}")
    message(FATAL_ERROR "${DEPRECATED_CONFIG_FILE} exists in the source directory, and will take precedence over the generated configuration in the build directory. Please remove this file before continuing. Alternatively, you can also clean your realm-core to remove this and other stale files: git clean -xfd")
endif()

add_subdirectory(src)

enable_testing()
add_subdirectory(test)

# Install the licence and changelog files
install(FILES LICENSE CHANGELOG.md DESTINATION "doc/realm")

include(CMakePackageConfigHelpers)
write_basic_package_version_file(realmConfigVersion.cmake
    VERSION ${REALM_VERSION}
    COMPATIBILITY SameMajorVersion
)

# Make the project importable from the build directory
export(EXPORT realm NAMESPACE Realm:: FILE realmTargets.cmake)
file(
    COPY "${RealmCore_SOURCE_DIR}/tools/cmake/realmConfig.cmake"
    DESTINATION "${RealmCore_BINARY_DIR}"
)

# Make the project importable from the install directory
install(EXPORT realm FILE realmTargets.cmake NAMESPACE Realm:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/realm)
install(
    FILES
        tools/cmake/realmConfig.cmake
        "${RealmCore_BINARY_DIR}/realmConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/realm
)

# CPack
get_package_file_name(realm-core ${REALM_VERSION})
set(CPACK_GENERATOR TGZ)
set(CPACK_PACKAGE_FILE_NAME ${realm-core_FILE_NAME})
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
if(CMAKE_BUILD_TYPE STREQUAL Release)
    set(CPACK_STRIP_FILES TRUE)
endif()
include(CPack)

