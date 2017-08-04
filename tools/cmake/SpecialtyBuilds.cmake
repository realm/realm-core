# -------------
# Coverage
# -------------
option(REALM_COVERAGE "Compile with coverage support." OFF)
if(REALM_COVERAGE)
    if(MSVC)
        message(FATAL_ERROR
                "Code coverage is not yet supported on Visual Studio builds")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage -fno-elide-constructors")
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
# Address Sanitizer
# -------------
option(REALM_ASAN "Compile with address sanitizer support" OFF)
if(REALM_ASAN)
    if(MSVC)
        message(FATAL_ERROR
                "The Address Sanitizer is not yet supported on Visual Studio builds")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -g")
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
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -O2 -g -fPIE")
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
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -O2 -g -fPIE -pie")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
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
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer -O2 -g -fPIE -pie")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
    endif()
endif()
