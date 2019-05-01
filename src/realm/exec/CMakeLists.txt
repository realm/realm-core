
if(NOT APPLE AND NOT ANDROID AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows" AND NOT REALM_BUILD_LIB_ONLY)
    add_executable(RealmConfig config_tool.cpp)
    set_target_properties(RealmConfig PROPERTIES
        OUTPUT_NAME "realm-config"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})

    # FIXME add_executable(RealmImporter importer_tool.cpp importer.cpp importer.hpp)
    # set_target_properties(RealmImporter PROPERTIES
    #     OUTPUT_NAME "realm-importer"
    #     DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    # target_link_libraries(RealmImporter realm)

    install(TARGETS RealmConfig # RealmImporter
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_BINDIR})

    add_executable(realmd realmd.cpp)
    set_target_properties(realmd PROPERTIES DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(realmd Core)
    install(TARGETS realmd
            COMPONENT runtime
            DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
endif()

add_executable(RealmTrawler EXCLUDE_FROM_ALL realm_trawler.cpp )
set_target_properties(RealmTrawler PROPERTIES
    OUTPUT_NAME "realm-trawler"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmTrawler Core)

add_executable(RealmBrowser EXCLUDE_FROM_ALL realm_browser.cpp )
set_target_properties(RealmBrowser PROPERTIES
    OUTPUT_NAME "realm-browser-6"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmBrowser Core)

add_executable(RealmDump EXCLUDE_FROM_ALL realm_dump.c)
set_target_properties(RealmDump PROPERTIES
    OUTPUT_NAME "realm-dump"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_compile_definitions(RealmDump PRIVATE
    "$<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>"
)
