if (CMAKE_BUILD_TYPE MATCHES "RelAssert")
    set(REALM_ENABLE_ASSERTIONS ON CACHE BOOL "Build with assertions")
    set(CMAKE_CXX_FLAGS_RELASSERT ${CMAKE_CXX_FLAGS_RELWITHDEBINFO})
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelASAN")
    set(REALM_ASAN ON CACHE BOOL "Build with address sanitizer")
    set(CMAKE_CXX_FLAGS_RELASAN "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O1")
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelTSAN")
    set(REALM_TSAN ON CACHE BOOL "Build with address sanitizer")
    set(CMAKE_CXX_FLAGS_RELTSAN "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O1")
endif()

# -------------
# Coverage
# -------------
option(REALM_COVERAGE "Compile with coverage support." OFF)
if(REALM_COVERAGE)
    if(MSVC)
        message(FATAL_ERROR
                "Code coverage is not yet supported on Visual Studio builds")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g --coverage -fprofile-arcs -ftest-coverage -fno-elide-constructors")
        if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline -fno-inline-small-functions -fno-default-inline")
        endif()
    endif()
endif()

# -------------
# AFL
# -------------
option(REALM_AFL "Compile for fuzz testing." OFF)
if(REALM_AFL)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        set(FUZZ_COMPILER_NAME "afl-clang++")
    elseif(${CMAKE_CXX_COMPILER_ID} MATCHES "GNU")
        set(FUZZ_COMPILER_NAME "afl-g++")
    else()
        message(FATAL_ERROR
                "Running AFL with your compiler (${CMAKE_CXX_COMPILER_ID}) is not supported")
    endif()
    find_program(AFL ${FUZZ_COMPILER_NAME})
    if(NOT AFL)
        message(FATAL_ERROR "AFL not found!")
    endif()
    set(CMAKE_CXX_COMPILER "${AFL}")
endif()

# -------------
# libfuzzer
# -------------
option(REALM_LIBFUZZER "Compile with llvm libfuzzer" OFF)
if(REALM_LIBFUZZER)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        # todo: add the undefined sanitizer here after blacklisting false positives
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer,address -fsanitize-coverage=trace-pc-guard")
    else()
        message(FATAL_ERROR
                "Compiling for libfuzzer is only supported with clang")
    endif()
endif()

# -------------
# Address Sanitizer
# -------------
option(REALM_ASAN "Compile with address sanitizer support" OFF)
if(REALM_ASAN)
    if(MSVC)
        message(FATAL_ERROR
                "The Address Sanitizer is not yet supported on Visual Studio builds")
    else()
        list(APPEND REALM_SANITIZER_FLAGS -fsanitize=address)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -g")
    endif()
endif()

# -------------
# Thread Sanitizer
# -------------
option(REALM_TSAN "Compile with thread sanitizer support" OFF)
if(REALM_TSAN)
    if(MSVC)
        message(FATAL_ERROR
                "The Thread Sanitizer is not yet supported on Visual Studio builds")
    else()
        list(APPEND REALM_SANITIZER_FLAGS -fsanitize=thread)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -fPIE")
        # According to the clang docs, if -fsanitize=thread is specified then compiling
        # and linking with PIE is turned on automatically.
        if (CMAKE_COMPILER_IS_GNUXX)
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pie")
        endif()
    endif()
endif()

# -------------
# Memory Sanitizer
# -------------
option(REALM_MSAN "Compile with memory sanitizer support" OFF)
if(REALM_MSAN)
    if(MSVC)
        message(FATAL_ERROR
                "The Memory Sanitizer is not yet supported on Visual Studio builds")
    else()
        list(APPEND REALM_SANITIZER_FLAGS -fsanitize=memory -fsanitize-memory-track-origins)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -O2 -g -fPIE -pie")
    endif()
endif()

# -------------
# Undefined Sanitizer
# -------------
option(REALM_USAN "Compile with undefined sanitizer support" OFF)
if(REALM_USAN)
    if(MSVC)
        message(FATAL_ERROR
                "The Undefined Sanitizer is not yet supported on Visual Studio builds")
    else()
        list(APPEND REALM_SANITIZER_FLAGS -fsanitize=undefined)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -O2 -g -fPIE -pie")
    endif()
endif()
