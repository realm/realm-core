
add_executable(InspectorInspectClientRealm "inspect_client_realm.cpp")
set_target_properties(InspectorInspectClientRealm PROPERTIES
                      OUTPUT_NAME "realm-inspect-client-realm"
                      DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
target_link_libraries(InspectorInspectClientRealm Sync Storage)

add_executable(InspectorPrintChangeset "print_changeset.cpp")
set_target_properties(InspectorPrintChangeset PROPERTIES
                      OUTPUT_NAME "realm-print-changeset"
                      DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
target_link_libraries(InspectorPrintChangeset Sync Storage)

add_executable(StatCommand stat_command.cpp)
set_target_properties(StatCommand PROPERTIES
                      OUTPUT_NAME "realm-stat"
                      DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
target_link_libraries(StatCommand Sync Storage)

add_executable(ApplyToStateCommand apply_to_state_command.cpp)
set_target_properties(ApplyToStateCommand PROPERTIES
                      OUTPUT_NAME "realm-apply-to-state"
                      DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
target_link_libraries(ApplyToStateCommand Sync Storage)

add_executable(HistCommand hist_command.cpp)
set_target_properties(HistCommand PROPERTIES
                      OUTPUT_NAME "realm-hist"
                      DEBUG_POSTFIX "${CMAKE_DEBUG_POSTFIX}")
target_link_libraries(HistCommand Sync Storage)

install(TARGETS
    StatCommand
    HistCommand
    ApplyToStateCommand
    InspectorInspectClientRealm
    InspectorPrintChangeset
    COMPONENT runtime
    DESTINATION bin
)
