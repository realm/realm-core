set(REALM_PARSER_SOURCES
    expression_container.cpp
    keypath_mapping.cpp
    parser.cpp
    parser_utils.cpp
    primitive_list_expression.cpp
    property_expression.cpp
    query_builder.cpp
    subquery_expression.cpp
    value_expression.cpp
) # REALM_PARSER_SOURCES

set(REALM_PARSER_HEADERS
    collection_operator_expression.hpp
    expression_container.hpp
    keypath_mapping.hpp
    parser.hpp
    parser_utils.hpp
    primitive_list_expression.hpp
    property_expression.hpp
    query_builder.hpp
    subquery_expression.hpp
    value_expression.hpp
) # REALM_PARSER_HEADERS

if (MSVC)
    set_source_files_properties(query_builder.cpp PROPERTIES COMPILE_FLAGS /bigobj)
endif()

add_library(QueryParser STATIC ${REALM_PARSER_SOURCES} ${REALM_PARSER_HEADERS})
add_library(realm-parser ALIAS QueryParser)
add_library(Realm::QueryParser ALIAS QueryParser)
target_link_libraries(QueryParser PUBLIC Storage)
target_include_directories(QueryParser PRIVATE ${PEGTL_INCLUDE_DIR})
set_target_properties(QueryParser PROPERTIES OUTPUT_NAME "realm-parser")
install(FILES ${REALM_PARSER_HEADERS}
        DESTINATION include/realm/parser
        COMPONENT devel)
install(TARGETS QueryParser EXPORT realm
        ARCHIVE DESTINATION lib
        COMPONENT devel)
