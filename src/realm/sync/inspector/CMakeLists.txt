cmake_minimum_required(VERSION 3.6)

set(INSPECTOR_HEADERS
    util.hpp
)

set(INSPECTOR_SOURCES
    util.cpp
)

add_library(Inspector ${INSPECTOR_HEADERS} ${INSPECTOR_SOURCES})
target_link_libraries(Inspector Sync SyncServer)

add_executable(InspectorInspectClientRealm "inspect_client_realm.cpp")
set_target_properties(InspectorInspectClientRealm PROPERTIES OUTPUT_NAME "realm-inspect-client-realm")
target_link_libraries(InspectorInspectClientRealm Inspector RealmCore::Storage)

add_executable(InspectorPrintChangeset "print_changeset.cpp")
set_target_properties(InspectorPrintChangeset PROPERTIES OUTPUT_NAME "realm-print-changeset")
target_link_libraries(InspectorPrintChangeset Inspector RealmCore::Storage)

add_executable(InspectorInspectServerRealm "inspect_server_realm.cpp")
set_target_properties(InspectorInspectServerRealm PROPERTIES OUTPUT_NAME "realm-inspect-server-realm")
target_link_libraries(InspectorInspectServerRealm Inspector RealmCore::Storage)

add_executable(InspectorMergeChangesetIntoRealm "merge_changeset_into_realm.cpp")
set_target_properties(InspectorMergeChangesetIntoRealm PROPERTIES OUTPUT_NAME "realm-merge-changeset-into-realm")
target_link_libraries(InspectorMergeChangesetIntoRealm Inspector RealmCore::Storage)

add_executable(InspectorPerformPartialSync "perform_partial_sync.cpp")
set_target_properties(InspectorPerformPartialSync PROPERTIES OUTPUT_NAME "realm-perform-partial-sync")
target_link_libraries(InspectorPerformPartialSync Inspector RealmCore::Storage)
