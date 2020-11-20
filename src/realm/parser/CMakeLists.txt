set(REALM_PARSER_SOURCES
    driver.cpp
    keypath_mapping.cpp
    query_flex.cpp
    query_bison.cpp
) # REALM_PARSER_SOURCES

set(REALM_PARSER_HEADERS
    driver.hpp
    query_bison.hpp
    query_flex.hpp
    query_parser.hpp
)

# Add these files so that they appear in cmake generated projects for IDE
set(REALM_PARSER_EXTRAS
    query_bison.yy
    query_flex.ll
)

if(NOT MSVC)
    set_source_files_properties(query_flex.cpp PROPERTIES COMPILE_OPTIONS "-Wno-unreachable-code")
endif()

add_library(QueryParser STATIC ${REALM_PARSER_SOURCES} ${REALM_PARSER_HEADERS} ${REALM_PARSER_EXTRAS})
add_library(realm-parser ALIAS QueryParser)
add_library(Realm::QueryParser ALIAS QueryParser)
target_link_libraries(QueryParser PUBLIC Storage)
set_target_properties(QueryParser PROPERTIES OUTPUT_NAME "realm-parser")

install(TARGETS QueryParser EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)
