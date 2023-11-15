if (CMAKE_BUILD_TYPE MATCHES "RelAssert")
    set(REALM_ENABLE_ASSERTIONS ON CACHE BOOL "Build with assertions")
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelASAN")
    set(REALM_ASAN ON CACHE BOOL "Build with address sanitizer")
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelTSAN")
    set(REALM_TSAN ON CACHE BOOL "Build with address sanitizer")
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelMSAN")
    set(REALM_MSAN ON CACHE BOOL "Build with memory sanitizer")
endif()

if (CMAKE_BUILD_TYPE MATCHES "RelUSAN")
    set(REALM_USAN ON CACHE BOOL "Build with undefined sanitizer")
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

option(REALM_LLVM_COVERAGE "Compile with llvm's code coverage support." OFF)
if (REALM_LLVM_COVERAGE)
    if(${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_LINK_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    else()
        message(FATAL_ERROR "Code coverage is only supported with clang")
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
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fsanitize=fuzzer,address")
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
    if (MSVC)
        set(REALM_SANITIZER_FLAGS /fsanitize=address)
        set(REALM_SANITIZER_LINK_FLAGS /INCREMENTAL:NO)

        if ("${CMAKE_BUILD_TYPE}" STREQUAL "RelASAN")
            list(APPEND REALM_SANITIZER_FLAGS /Ox /Zi)
            list(APPEND REALM_SANITIZER_LINK_FLAGS /DEBUG)
        elseif (NOT "${CMAKE_BUILD_TYPE}" MATCHES ".*Deb.*")
            # disable warning for better stacktrace for asan
            # pdbs can be activated with RelWithDebInfo or RelASAN
            list(APPEND REALM_SANITIZER_FLAGS /wd5072)
        endif()
    else()
        set(REALM_SANITIZER_FLAGS -fsanitize=address -fno-sanitize-recover=all -fsanitize-address-use-after-scope -fno-omit-frame-pointer)
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
        set(REALM_SANITIZER_FLAGS -fsanitize=thread -fno-sanitize-recover=all -fno-omit-frame-pointer)
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
        set(REALM_SANITIZER_FLAGS -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer)
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
        set(REALM_SANITIZER_FLAGS -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
    endif()
endif()

if (REALM_SANITIZER_FLAGS)
    if (NOT MSVC)
        if ("${CMAKE_BUILD_TYPE}" MATCHES "Rel[ATMU]SAN")
            list(APPEND REALM_SANITIZER_FLAGS -O1 -g)
        endif()

        set(REALM_SANITIZER_LINK_FLAGS ${REALM_SANITIZER_FLAGS})

        if (CMAKE_COMPILER_IS_GNUXX) # activated for clang automatically according to docs
            list(APPEND REALM_SANITIZER_LINK_FLAGS -pie)
        endif()
    endif()

    add_compile_options(${REALM_SANITIZER_FLAGS})
    if (MSVC)
        add_compile_definitions(_DISABLE_STRING_ANNOTATION _DISABLE_VECTOR_ANNOTATION)
    endif()

    if (REALM_SANITIZER_LINK_FLAGS)
        add_link_options(${REALM_SANITIZER_LINK_FLAGS})
    endif()
endif()
