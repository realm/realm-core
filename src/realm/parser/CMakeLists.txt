if(BISON_FOUND)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/generated/query_bison.cpp ${CMAKE_CURRENT_SOURCE_DIR}/generated/query_bison.hpp
        MAIN_DEPENDENCY query_bison.yy
        COMMAND ${BISON_EXECUTABLE} --defines=generated/query_bison.hpp -o generated/query_bison.cpp query_bison.yy
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
if(FLEX_FOUND)
    add_custom_command(
        OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/generated/query_flex.cpp ${CMAKE_CURRENT_SOURCE_DIR}/generated/query_flex.hpp
        MAIN_DEPENDENCY query_flex.ll
        COMMAND ${FLEX_EXECUTABLE} --noline --header-file=generated/query_flex.hpp -ogenerated/query_flex.cpp query_flex.ll
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()

if(BISON_FOUND AND FLEX_FOUND)
    add_custom_target(GenerateParserSources
        COMMAND ${FLEX_EXECUTABLE} --noline --header-file=generated/query_flex.hpp -ogenerated/query_flex.cpp query_flex.ll
        COMMAND ${BISON_EXECUTABLE} --defines=generated/query_bison.hpp -o generated/query_bison.cpp query_bison.yy
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DEPENDS query_bison.yy query_flex.ll
    )
endif()

set(REALM_PARSER_SOURCES
    driver.cpp
    keypath_mapping.cpp
    generated/query_flex.cpp
    generated/query_bison.cpp
) # REALM_PARSER_SOURCES

set(REALM_PARSER_HEADERS
    driver.hpp
    keypath_mapping.hpp
    query_parser.hpp
    generated/query_bison.hpp
    generated/query_flex.hpp
)

set(REALM_PARSER_INSTALL_HEADERS
    keypath_mapping.hpp
    query_parser.hpp
)

# Add these files so that they appear in cmake generated projects for IDE
set(REALM_PARSER_EXTRAS
    query_bison.yy
    query_flex.ll
)

if(NOT MSVC)
    set_source_files_properties(generated/query_flex.cpp PROPERTIES COMPILE_OPTIONS "-Wno-unreachable-code")
endif()

add_library(QueryParser STATIC ${REALM_PARSER_SOURCES} ${REALM_PARSER_HEADERS} ${REALM_PARSER_EXTRAS})
add_library(realm-parser ALIAS QueryParser)
add_library(Realm::QueryParser ALIAS QueryParser)
target_link_libraries(QueryParser PUBLIC Storage)
set_target_properties(QueryParser PROPERTIES OUTPUT_NAME "realm-parser")

install(TARGETS QueryParser EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)

INSTALL(FILES ${REALM_PARSER_INSTALL_HEADERS}
        DESTINATION include/realm/parser
        COMPONENT devel)
