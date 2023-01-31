set(REALM_FFI_SOURCES
    ../../../realm.h

    config.cpp
    dictionary.cpp
    error.cpp
    notifications.cpp
    object.cpp
    list.cpp
    query.cpp
    realm.cpp
    scheduler.cpp
    schema.cpp
    set.cpp
    util.cpp

    conversion.hpp
    types.hpp
    util.hpp
    # realm/object-store/c_api/realm.c
)

if(REALM_ENABLE_SYNC)
    list(APPEND REALM_FFI_SOURCES
        app.cpp
        http.cpp
        logging.cpp
        sync.cpp
        socket_provider.cpp
    )
endif()

add_library(RealmFFIStatic STATIC ${REALM_FFI_SOURCES})
if(EMSCRIPTEN)
    add_library(RealmFFI ALIAS RealmFFIStatic)
else()
    add_library(RealmFFI SHARED ${REALM_FFI_SOURCES})
endif()

if (NOT EMSCRIPTEN)
    target_compile_definitions(RealmFFI PRIVATE -DRealm_EXPORTS)
endif()
target_compile_definitions(RealmFFIStatic PUBLIC -DRLM_NO_DLLIMPORT)

if (NOT EMSCRIPTEN)
    target_link_libraries(RealmFFI PRIVATE Storage ObjectStore QueryParser)
endif()
target_link_libraries(RealmFFIStatic PRIVATE Storage ObjectStore QueryParser)

if (${REALM_ENABLE_SYNC})
    if (NOT EMSCRIPTEN)
        target_link_libraries(RealmFFI PRIVATE Sync)
    endif()
    target_link_libraries(RealmFFIStatic PRIVATE Sync)
endif()

if (ANDROID AND NOT EMSCRIPTEN)
    target_link_libraries(RealmFFI PUBLIC android log)
endif()

# TODO: Including the pegtl header is only necessary because the Query Parser
# throws raw pegtl exceptions on failure.
if (NOT EMSCRIPTEN)
    target_include_directories(RealmFFI PRIVATE ${PEGTL_INCLUDE_DIR})
endif()
target_include_directories(RealmFFIStatic PRIVATE ${PEGTL_INCLUDE_DIR})

target_include_directories(RealmFFIStatic INTERFACE
    $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

if(NOT EMSCRIPTEN)
    target_include_directories(RealmFFI INTERFACE
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/src>
        $<INSTALL_INTERFACE:include>
    )

    set_target_properties(RealmFFI PROPERTIES
        OUTPUT_NAME "realm-ffi"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON

        # FIXME: Not building by default because of the link-time dependency on
        # libuv. This should be fixed by refactoring Scheduler to be a dynamic
        # interface with an implementation selected at runtime.
        EXCLUDE_FROM_ALL ON
    )
endif()

set_target_properties(RealmFFIStatic PROPERTIES
    OUTPUT_NAME "realm-ffi-static"
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
)

# install(TARGETS RealmFFI DESTINATION lib COMPONENT devel)

install(TARGETS RealmFFIStatic DESTINATION lib COMPONENT devel)
install(FILES ../../../realm.h DESTINATION include COMPONENT devel)
