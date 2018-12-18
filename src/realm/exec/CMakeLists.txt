
if(NOT APPLE AND NOT ANDROID AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows" AND NOT REALM_BUILD_LIB_ONLY)
    add_executable(RealmConfig config_tool.cpp)
    set_target_properties(RealmConfig PROPERTIES
        OUTPUT_NAME "realm-config"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

    add_executable(RealmImporter importer_tool.cpp importer.cpp importer.hpp)
    set_target_properties(RealmImporter PROPERTIES
        OUTPUT_NAME "realm-importer"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(RealmImporter realm)

    install(TARGETS RealmConfig RealmImporter
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_BINDIR})

    add_executable(realmd realmd.cpp)
    set_target_properties(realmd PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(realmd Core)
    install(TARGETS realmd
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})

    add_executable(RealmTrawler realm_trawler.cpp )
    set_target_properties(RealmTrawler PROPERTIES
        OUTPUT_NAME "realm-trawler"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(RealmTrawler realm)
endif()
