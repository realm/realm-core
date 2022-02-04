add_subdirectory(c_api)

set(SOURCES
    binding_callback_thread_observer.cpp
    collection_notifications.cpp
    index_set.cpp
    collection.cpp
    dictionary.cpp
    list.cpp
    object.cpp
    object_changeset.cpp
    object_schema.cpp
    object_store.cpp
    results.cpp
    set.cpp
    schema.cpp
    shared_realm.cpp
    thread_safe_reference.cpp

    impl/collection_change_builder.cpp
    impl/collection_notifier.cpp
    impl/deep_change_checker.cpp
    impl/list_notifier.cpp
    impl/object_notifier.cpp
    impl/realm_coordinator.cpp
    impl/results_notifier.cpp
    impl/transact_log_handler.cpp
    impl/weak_realm_notifier.cpp
    util/scheduler.cpp
    util/uuid.cpp

    util/android/scheduler.hpp
    util/apple/scheduler.hpp
    util/generic/scheduler.hpp
    util/uv/scheduler.hpp)

set(HEADERS
    binding_callback_thread_observer.hpp
    binding_context.hpp
    collection_notifications.hpp
    feature_checks.hpp
    index_set.hpp
    keypath_helpers.hpp
    collection.hpp
    dictionary.hpp
    list.hpp
    object.hpp
    object_accessor.hpp
    object_changeset.hpp
    object_schema.hpp
    object_store.hpp
    property.hpp
    results.hpp
    set.hpp
    schema.hpp
    shared_realm.hpp
    thread_safe_reference.hpp

    impl/apple/external_commit_helper.hpp
    impl/apple/keychain_helper.hpp
    impl/epoll/external_commit_helper.hpp
    impl/generic/external_commit_helper.hpp

    impl/collection_change_builder.hpp
    impl/collection_notifier.hpp
    impl/deep_change_checker.hpp
    impl/external_commit_helper.hpp
    impl/list_notifier.hpp
    impl/notification_wrapper.hpp
    impl/object_accessor_impl.hpp
    impl/object_notifier.hpp
    impl/realm_coordinator.hpp
    impl/results_notifier.hpp
    impl/transact_log_handler.hpp
    impl/weak_realm_notifier.hpp

    util/aligned_union.hpp
    util/atomic_shared_ptr.hpp
    util/copyable_atomic.hpp
    util/event_loop_dispatcher.hpp
    util/scheduler.hpp
    util/tagged_bool.hpp
    util/tagged_string.hpp
    util/uuid.hpp

    c_api/conversion.hpp
)

if(REALM_ENABLE_SYNC)
    list(APPEND HEADERS
        sync/app.hpp
        sync/app_utils.hpp
        sync/app_credentials.hpp
        sync/generic_network_transport.hpp
        sync/async_open_task.hpp
        sync/sync_manager.hpp
        sync/sync_session.hpp
        sync/sync_user.hpp
        sync/app_service_client.hpp
        sync/auth_request_client.hpp
        sync/mongo_client.hpp
        sync/mongo_collection.hpp
        sync/mongo_database.hpp
        sync/push_client.hpp
        sync/subscribable.hpp

        sync/impl/sync_client.hpp
        sync/impl/sync_file.hpp
        sync/impl/sync_metadata.hpp
        sync/impl/network_reachability.hpp

        util/bson/bson.hpp
        util/bson/min_key.hpp
        util/bson/max_key.hpp
        util/bson/regular_expression.hpp
        util/bson/indexed_map.hpp
        util/bson/mongo_timestamp.hpp)

    list(APPEND SOURCES
        sync/app.cpp
        sync/app_utils.cpp
        sync/app_credentials.cpp
        sync/generic_network_transport.cpp
        sync/async_open_task.cpp
        sync/sync_manager.cpp
        sync/sync_session.cpp
        sync/sync_user.cpp
        sync/mongo_client.cpp
        sync/mongo_collection.cpp
        sync/mongo_database.cpp
        sync/push_client.cpp
        sync/impl/sync_file.cpp
        sync/impl/sync_metadata.cpp
        util/bson/bson.cpp
        util/bson/regular_expression.cpp)
    if(APPLE)

        list(APPEND HEADERS
            sync/impl/apple/network_reachability_observer.hpp
            sync/impl/apple/system_configuration.hpp)

        list(APPEND SOURCES
            sync/impl/apple/network_reachability_observer.cpp
            sync/impl/apple/system_configuration.cpp)
    endif()
endif()

add_library(ObjectStore STATIC ${SOURCES} ${HEADERS})
add_library(Realm::ObjectStore ALIAS ObjectStore)

set_target_properties(ObjectStore PROPERTIES
    OUTPUT_NAME realm-object-store
)

check_symbol_exists(epoll_create sys/epoll.h REALM_HAVE_EPOLL)

if(APPLE)
    target_sources(ObjectStore PRIVATE impl/apple/external_commit_helper.cpp impl/apple/keychain_helper.cpp)
elseif(REALM_HAVE_EPOLL)
    target_compile_definitions(ObjectStore PUBLIC REALM_HAVE_EPOLL=1)
    target_sources(ObjectStore PRIVATE impl/epoll/external_commit_helper.cpp)
elseif(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    target_sources(ObjectStore PRIVATE impl/windows/external_commit_helper.cpp)
else()
    target_sources(ObjectStore PRIVATE impl/generic/external_commit_helper.cpp)
endif()

if(REALM_ENABLE_SYNC OR REALM_ENABLE_SERVER)
    # needed to disable assertions in external/json
    target_compile_definitions(ObjectStore PUBLIC
        $<$<CONFIG:Release>:NDEBUG>
        $<$<CONFIG:RelWithDebInfo>:NDEBUG>
    )
    target_include_directories(ObjectStore PRIVATE ${JSON_INCLUDE_DIR})
endif()

target_link_libraries(ObjectStore PUBLIC Storage QueryParser)

if(REALM_ENABLE_SYNC)
    target_link_libraries(ObjectStore PUBLIC Sync)
    target_compile_definitions(ObjectStore PUBLIC REALM_ENABLE_SYNC=1)
endif()

set_target_properties(ObjectStore PROPERTIES OUTPUT_NAME "realm-object-store")

foreach(FILE ${HEADERS})
    get_filename_component(DIR ${FILE} DIRECTORY)
    INSTALL(FILES ${FILE}
            DESTINATION include/realm/object-store/${DIR}
            COMPONENT devel)
endforeach()
install(TARGETS ObjectStore EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)
