if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_FLAVOR "debug")
    set(TARGET_FLAGS_ARG "")
    set(LIB_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
else()
    set(CARGO_CMD cargo build --release)
    set(TARGET_FLAVOR "release")
    set(TARGET_FLAVOR_ARG "--release")
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

set(CMAKE_ENV_CMD
    ${CMAKE_COMMAND} -E env
        "REALM_RUST_CORE_INCLUDE_DIRECTORIES=$<JOIN:$<TARGET_PROPERTY:Storage,INCLUDE_DIRECTORIES>,:>"
        "REALM_RUST_CORE_BINARY_DIR=$<TARGET_PROPERTY:Storage,BINARY_DIR>"
        "REALM_RUST_CORE_BINARY_NAME=$<TARGET_PROPERTY:Storage,OUTPUT_NAME>${LIB_POSTFIX}"
        "REALM_RUST_BRIDGE_INCLUDE_DIRECTORIES=$<JOIN:$<TARGET_PROPERTY:RustBridge,INCLUDE_DIRECTORIES>,:>"
        "REALM_RUST_BRIDGE_BINARY_DIR=$<TARGET_PROPERTY:RustBridge,BINARY_DIR>"
        "REALM_RUST_BRIDGE_BINARY_NAME=$<TARGET_PROPERTY:RustBridge,OUTPUT_NAME>${LIB_POSTFIX}"
)

set(CARGO_ENV_CMD
    ${CMAKE_ENV_CMD} -- cargo
)

# Build Rust code, including headers used by the bridge code (the `RustBridge`
# target).
add_custom_target(RustBundle
    COMMAND
        "${CARGO_ENV_CMD}" build
            "${TARGET_FLAVOR_ARG}"
            --target-dir "${CARGO_TARGET_DIR}"
            -p realm-rust-bundle
    BYPRODUCTS "${BUNDLE_LIB_PATH}"
    VERBATIM
    COMMAND_EXPAND_LISTS
)

add_library(RustBridge STATIC
    sys/bridge.cpp
    sys/bridge.hpp
    "${CARGO_TARGET_DIR}/cxxbridge/realm-sys/src/lib.rs.cc"
)
set_target_properties(RustBridge PROPERTIES OUTPUT_NAME "realm-rust-bridge")
target_include_directories(RustBridge PRIVATE
    "${CARGO_TARGET_DIR}/cxxbridge"
    "sys"
)
target_link_libraries(RustBridge PUBLIC Storage)
install(TARGETS RustBridge EXPORT realm
    ARCHIVE DESTINATION lib
    COMPONENT devel)

if(NOT REALM_CARGO_BUILD)
    # If building through cargo, the bridge components produced by `RustBundle`
    # have already been generated, so we don't need this dependency. Also, we
    # cannot have it, since it would invoke cargo recursively.
    add_dependencies(RustBridge RustBundle)

    install(FILES "${CARGO_TARGET_DIR}/${TARGET_FLAVOR}/librealm_rust_bundle.a"
        DESTINATION lib
        COMPONENT devel
    )
endif()

enable_testing()
add_test(
    NAME RustTests
    COMMAND
        "${CARGO_ENV_CMD}" test
            "${TARGET_FLAVOR_ARG}"
            --target-dir "${CARGO_TARGET_DIR}"
            --workspace
    COMMAND_EXPAND_LISTS
)
