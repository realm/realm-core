add_executable(RealmImporter EXCLUDE_FROM_ALL importer_tool.cpp importer.cpp importer.hpp)
set_target_properties(RealmImporter PROPERTIES
    OUTPUT_NAME "realm-importer"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmImporter Storage)

if(NOT APPLE AND NOT ANDROID AND NOT CMAKE_SYSTEM_NAME MATCHES "^Windows")
    add_executable(RealmDaemon realmd.cpp)
    set_target_properties(RealmDaemon PROPERTIES
        OUTPUT_NAME "realmd"
        DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX})
    target_link_libraries(RealmDaemon Storage)
    list(APPEND ExecTargetsToInstall RealmDaemon)
endif()

add_executable(RealmTrawler EXCLUDE_FROM_ALL realm_trawler.cpp)
set_target_properties(RealmTrawler PROPERTIES
    OUTPUT_NAME "realm-trawler"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmTrawler Storage)

add_executable(RealmEnumerate EXCLUDE_FROM_ALL realm_enumerate.cpp)
set_target_properties(RealmEnumerate PROPERTIES
    OUTPUT_NAME "realm-enumerate"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmEnumerate ObjectStore)

add_executable(RealmDecrypt EXCLUDE_FROM_ALL realm_decrypt.cpp)
set_target_properties(RealmDecrypt PROPERTIES
    OUTPUT_NAME "realm-decrypt"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmDecrypt Storage)

add_executable(RealmEncrypt EXCLUDE_FROM_ALL realm_encrypt.cpp)
set_target_properties(RealmEncrypt PROPERTIES
    OUTPUT_NAME "realm-encrypt"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmEncrypt Storage)

add_executable(RealmBrowser EXCLUDE_FROM_ALL realm_browser.cpp)
set_target_properties(RealmBrowser PROPERTIES
    OUTPUT_NAME "realm-browser-10"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(RealmBrowser Storage)

if(REALM_ENABLE_SYNC)
add_executable(Realm2JSON realm2json.cpp )
set_target_properties(Realm2JSON PROPERTIES
    OUTPUT_NAME "realm2json"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)
target_link_libraries(Realm2JSON Storage QueryParser Sync)
list(APPEND ExecTargetsToInstall Realm2JSON)
endif()

add_executable(RealmDump EXCLUDE_FROM_ALL realm_dump.c)
set_target_properties(RealmDump PROPERTIES
    OUTPUT_NAME "realm-dump"
    DEBUG_POSTFIX ${CMAKE_DEBUG_POSTFIX}
)

install(TARGETS ${ExecTargetsToInstall}
        COMPONENT runtime
        DESTINATION bin)

add_custom_target(build_all DEPENDS
    RealmImporter RealmTrawler RealmEnumerate RealmDecrypt
    RealmEncrypt RealmBrowser RealmDump)
add_dependencies(build_all ${ExecTargetsToInstall})
