add_subdirectory(c_api)

set(SOURCES
    collection.cpp
    collection_notifications.cpp
    dictionary.cpp
    index_set.cpp
    list.cpp
    class.cpp
    object.cpp
    object_changeset.cpp
    object_schema.cpp
    object_store.cpp
    results.cpp
    schema.cpp
    sectioned_results.cpp
    set.cpp
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
    audit.hpp
    audit_serializer.hpp
    binding_context.hpp
    collection.hpp
    collection_notifications.hpp
    dictionary.hpp
    feature_checks.hpp
    index_set.hpp
    keypath_helpers.hpp
    list.hpp
    class.hpp
    object.hpp
    object_accessor.hpp
    object_changeset.hpp
    object_schema.hpp
    object_store.hpp
    property.hpp
    results.hpp
    schema.hpp
    sectioned_results.hpp
    set.hpp
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
    util/tagged_string.hpp
    util/uuid.hpp

    c_api/conversion.hpp
)

add_library(ObjectStore STATIC ${SOURCES} ${HEADERS})
add_library(Realm::ObjectStore ALIAS ObjectStore)

set_target_properties(ObjectStore PROPERTIES
    OUTPUT_NAME realm-object-store
)

check_symbol_exists(epoll_create sys/epoll.h REALM_HAVE_EPOLL)

if(APPLE)
    target_sources(ObjectStore PRIVATE impl/apple/external_commit_helper.cpp impl/apple/keychain_helper.cpp)
    target_link_options(ObjectStore INTERFACE "SHELL:-framework Security")
elseif(REALM_HAVE_EPOLL)
    target_compile_definitions(ObjectStore PUBLIC REALM_HAVE_EPOLL=1)
    target_sources(ObjectStore PRIVATE impl/epoll/external_commit_helper.cpp)
elseif(CMAKE_SYSTEM_NAME MATCHES "^Windows")
    target_sources(ObjectStore PRIVATE impl/windows/external_commit_helper.cpp)
elseif(EMSCRIPTEN)
    target_sources(ObjectStore PRIVATE impl/emscripten/external_commit_helper.cpp)
else()
    target_sources(ObjectStore PRIVATE impl/generic/external_commit_helper.cpp)
endif()

target_link_libraries(ObjectStore PUBLIC Storage QueryParser)
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
