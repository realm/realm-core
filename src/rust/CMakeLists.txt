# https://cmake.org/cmake/help/latest/policy/CMP0116.html
cmake_policy(SET CMP0116 NEW)

find_program(
    CXXBRIDGE_COMMAND cxxbridge
    DOC "cxxbridge generator for Rust bindings"
)

if (NOT CXXBRIDGE_COMMAND)
    message(FATAL_ERROR "could not find `cxxbridge`; install with `cargo install cxxbridge-cmd`")
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_FLAVOR "debug")
    set(TARGET_FLAGS_ARG "")
    set(LIB_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
else()
    set(CARGO_CMD cargo build --release)
    set(TARGET_FLAVOR "release-with-debug")
    set(TARGET_FLAVOR_ARG "--profile=release-with-debug")
    set(LIB_POSTFIX "")
endif()

# TODO: Handle LTO
set(RUST_FLAGS "")

if(NOT CARGO_TARGET_DIR)
    set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/target")
endif()

# TODO: Windows lib names
set(BUNDLE_LIB_NAME "librealm_rust_bundle.a")
set(BUNDLE_LIB_PATH "${CARGO_TARGET_DIR}/${TARGET_FLAVOR}/${BUNDLE_LIB_NAME}")

# Cargo generates this file, and CMake can use it to detect if the build is
# outdated.
set(BUNDLE_DEP_NAME "librealm_rust_bundle.d")
set(BUNDLE_DEP_PATH "${CARGO_TARGET_DIR}/${TARGET_FLAVOR}/${BUNDLE_DEP_NAME}")


# Generate `rust/cxx.h` once. All bridge modules can use it.
set(RUSTCXX_DIR "${CMAKE_CURRENT_BINARY_DIR}/rust/cxx.h")
add_custom_command(
    OUTPUT "${RUSTCXX_DIR}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_CURRENT_BINARY_DIR}/rust"
    COMMAND "${CXXBRIDGE_COMMAND}" --header --output "${RUSTCXX_DIR}"
    COMMENT "Generating rust/cxx.h..."
    VERBATIM
)

# Generate the `realm-sys` glue code. This is used when calling from Rust into
# Core's C++ API.
set(REALM_SYS_LIB_RS "${CMAKE_CURRENT_SOURCE_DIR}/sys/src/lib.rs")
set(REALM_SYS_HPP "${CMAKE_CURRENT_BINARY_DIR}/realm-sys.hpp")
set(REALM_SYS_CPP "${CMAKE_CURRENT_BINARY_DIR}/realm-sys.cpp")
add_custom_command(
    OUTPUT
        "${REALM_SYS_HPP}"
        "${REALM_SYS_CPP}"
    MAIN_DEPENDENCY "${REALM_SYS_LIB_RS}"
    DEPENDS "${RUSTCXX_DIR}"
    COMMAND "${CXXBRIDGE_COMMAND}" "${REALM_SYS_LIB_RS}" --header --include "rust/cxx.h" --output "${REALM_SYS_HPP}"
    COMMAND "${CXXBRIDGE_COMMAND}" "${REALM_SYS_LIB_RS}" --include "${REALM_SYS_HPP}" --output "${REALM_SYS_CPP}"
    COMMENT "Generating realm-sys C++ interface..."
    VERBATIM
)

# Generate the `realm-query` glue code. This is used when calling from C++ into
# Rust.
set(REALM_QUERY_LIB_RS "${CMAKE_CURRENT_SOURCE_DIR}/query/src/lib.rs")
set(REALM_QUERY_HPP "${CMAKE_CURRENT_BINARY_DIR}/realm-query.hpp")
set(REALM_QUERY_CPP "${CMAKE_CURRENT_BINARY_DIR}/realm-query.cpp")
add_custom_command(
    OUTPUT
        "${REALM_QUERY_HPP}"
        "${REALM_QUERY_CPP}"
    MAIN_DEPENDENCY "${REALM_QUERY_LIB_RS}"
    DEPENDS "${RUSTCXX_DIR}"
    COMMAND "${CXXBRIDGE_COMMAND}" "${REALM_QUERY_LIB_RS}" --header --include "rust/cxx.h" --output "${REALM_QUERY_HPP}"
    COMMAND "${CXXBRIDGE_COMMAND}" "${REALM_QUERY_LIB_RS}" --include "${REALM_QUERY_HPP}" --output "${REALM_QUERY_CPP}"
    COMMENT "Generating realm-query C++ interface..."
    VERBATIM
)

# Build the C++ parts of the Rust bridge in CMake. This could also have been
# built by Cargo (in `sys/build.rs`), but it's better to have a single build
# system for all C++ code. The drawback is that we need to tell `sys/build.rs`
# about the library via environment variables.
#
# The `RustBridge` library is a dependency of the Rust code (`RustBundle`). It
# depends on Core, so it is the way that the `Storage` target becomes available
# to Rust code.
add_library(RustBridge STATIC
    # Generated glue code
    "${RUSTCXX_DIR}"
    "${REALM_SYS_HPP}"
    "${REALM_SYS_CPP}"
    "${REALM_QUERY_HPP}"
    "${REALM_QUERY_CPP}"

    # Manual glue
    "sys/bridge.cpp"
    "sys/bridge.hpp"
)
set_target_properties(RustBridge PROPERTIES
    OUTPUT_NAME "realm-rust-bridge"
)
target_include_directories(RustBridge
    PRIVATE
        "sys"
        "${CMAKE_CURRENT_BINARY_DIR}"
)
target_link_libraries(RustBridge PUBLIC Storage)

install(TARGETS RustBridge EXPORT realm ARCHIVE DESTINATION lib COMPONENT devel)

if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
    set(RUSTFLAGS
        "-Clinker=clang"
        # "-Clinker-plugin-lto"
        # "-Clink-arg=-flto"
        # "-Clink-arg=-fuse-ld=lld"
    )
endif()

set(CMAKE_ENV_CMD
    ${CMAKE_COMMAND} -E env REALM_CMAKE_BUILD=1 REALM_CMAKE_BINARY_DIR=${CMAKE_BINARY_DIR} RUSTFLAGS=${RUSTFLAGS}
)

set(CARGO_ENV_CMD
    ${CMAKE_ENV_CMD} -- cargo
)

if (NOT REALM_CARGO_BUILD)
    # CMake is not being invoked by Cargo, so we can add targets that invoke
    # Cargo. This is just a precaution to avoid recursive calls to Cargo.

    # Invoke Cargo to build a static library (the `realm-bundle` crate)
    # containing all Rust code, including any Rust dependencies (and including
    # the Rust standard library).
    add_custom_command(
        OUTPUT
            "${BUNDLE_LIB_PATH}"
        DEPFILE
            "${BUNDLE_DEP_PATH}"
        COMMAND
            ${CARGO_ENV_CMD} build ${TARGET_FLAVOR_ARG} --target-dir "${CARGO_TARGET_DIR}" -p realm-rust-bundle
        COMMENT "Building Rust bundle with Cargo..."
        DEPENDS
            RustBridge
            "${REALM_QUERY_HPP}"
            "${REALM_QUERY_CPP}"
        COMMAND_EXPAND_LISTS
        VERBATIM
    )

    # Virtual target. This is just a way to name the static library at
    # BUNDLE_LIB_PATH (produced by Cargo).
    add_custom_target(RustBundle DEPENDS "${BUNDLE_LIB_PATH}")

    # Interface target for the Rust code. Use this to depend on the Rust code
    # from a C++ target.
    #
    # TODO: Differentiate between install/build interfaces.
    add_library(RustInterface INTERFACE)
    add_dependencies(RustInterface RustBundle)
    target_link_libraries(RustInterface INTERFACE RustBridge "${BUNDLE_LIB_PATH}" Storage)
    target_include_directories(RustInterface INTERFACE "${CMAKE_CURRENT_BINARY_DIR}")

    install(FILES "${BUNDLE_LIB_PATH}" DESTINATION lib COMPONENT devel)
    install(FILES "${REALM_QUERY_HPP}" "${RUSTCXX_DIR}" DESTINATION include/rust COMPONENT devel)

    enable_testing() # TODO: Not appropriate here.
    add_test(
        NAME RustTests
        COMMAND
            ${CARGO_ENV_CMD} test ${TARGET_FLAVOR_ARG} --target-dir "${CARGO_TARGET_DIR}" --workspace
        COMMAND_EXPAND_LISTS
    )
endif()
