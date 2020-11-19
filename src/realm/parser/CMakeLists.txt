set(REALM_PARSER_SOURCES
    driver.cpp
    keypath_mapping.cpp
    query_flex.cpp
    query_bison.cpp
) # REALM_PARSER_SOURCES

add_library(QueryParser STATIC ${REALM_PARSER_SOURCES})
add_library(realm-parser ALIAS QueryParser)
add_library(Realm::QueryParser ALIAS QueryParser)
target_link_libraries(QueryParser PUBLIC Storage)
set_target_properties(QueryParser PROPERTIES OUTPUT_NAME "realm-parser")

install(TARGETS QueryParser EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)
