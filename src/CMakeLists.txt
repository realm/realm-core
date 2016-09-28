set(SOURCES
    collection_notifications.cpp
    index_set.cpp
    list.cpp
    object_schema.cpp
    object_store.cpp
    results.cpp
    schema.cpp
    shared_realm.cpp
    thread_confined.cpp
    impl/collection_change_builder.cpp
    impl/collection_notifier.cpp
    impl/handover.cpp
    impl/list_notifier.cpp
    impl/realm_coordinator.cpp
    impl/results_notifier.cpp
    impl/transact_log_handler.cpp
    impl/weak_realm_notifier.cpp
    parser/parser.cpp
    parser/query_builder.cpp
    util/format.cpp)

set(HEADERS
    collection_notifications.hpp
    index_set.hpp
    list.hpp
    object_schema.hpp
    object_store.hpp
    property.hpp
    results.hpp
    schema.hpp
    shared_realm.hpp
    thread_confined.hpp

    impl/android/external_commit_helper.hpp
    impl/apple/external_commit_helper.hpp
    impl/apple/keychain_helper.hpp
    impl/generic/external_commit_helper.hpp

    impl/collection_change_builder.hpp
    impl/collection_notifier.hpp
    impl/external_commit_helper.hpp
    impl/list_notifier.hpp
    impl/handover.hpp
    impl/realm_coordinator.hpp
    impl/results_notifier.hpp
    impl/transact_log_handler.hpp
    impl/weak_realm_notifier.hpp

    parser/parser.hpp
    parser/query_builder.hpp

    util/android/event_loop_signal.hpp
    util/apple/event_loop_signal.hpp
    util/generic/event_loop_signal.hpp
    util/node/event_loop_signal.hpp

    util/atomic_shared_ptr.hpp
    util/compiler.hpp
    util/event_loop_signal.hpp
    util/format.hpp)

if(APPLE)
    list(APPEND SOURCES impl/apple/external_commit_helper.cpp impl/apple/keychain_helper.cpp)
elseif(REALM_PLATFORM STREQUAL "Android")
    list(APPEND SOURCES impl/android/external_commit_helper.cpp)
else()
    list(APPEND SOURCES impl/generic/external_commit_helper.cpp)
endif()

set(INCLUDE_DIRS
    ${REALM_CORE_INCLUDE_DIR}
    ${PEGTL_INCLUDE_DIR}
    ${UV_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR})

if(REALM_ENABLE_SYNC)
    # Add the sync files separately to reduce merge conflicts.
    list(APPEND HEADERS sync_config.hpp sync_manager.hpp sync_metadata.hpp sync_session.hpp impl/sync_client.hpp)
    list(APPEND SOURCES sync_manager.cpp sync_metadata.cpp sync_session.cpp)
    find_package(ZLIB REQUIRED)
    list(APPEND INCLUDE_DIRS ${REALM_SYNC_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS})
endif()

add_library(realm-object-store STATIC ${SOURCES} ${HEADERS})
set_target_properties(realm-object-store PROPERTIES POSITION_INDEPENDENT_CODE 1)
target_compile_definitions(realm-object-store PRIVATE ${PLATFORM_DEFINES})
target_include_directories(realm-object-store PUBLIC ${INCLUDE_DIRS})
target_link_libraries(realm-object-store PUBLIC realm ${PLATFORM_LIBRARIES})

if(REALM_ENABLE_SYNC)
    # Add the sync libraries separately to reduce merge conflicts.
    target_link_libraries(realm-object-store PUBLIC realm-sync ${ZLIB_LIBRARIES})
endif()
