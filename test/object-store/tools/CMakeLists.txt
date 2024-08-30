if(REALM_ENABLE_SYNC)
    add_executable(redir_server
                   redir_server.cpp
                   ../util/sync/redirect_server.hpp
    )
    target_compile_definitions(redir_server PRIVATE
        REALM_ENABLE_AUTH_TESTS=1
    )
    target_include_directories(redir_server PRIVATE
                               ${JSON_INCLUDE_DIR}
                               ${CMAKE_CURRENT_SOURCE_DIR}/..
                               ${CMAKE_CURRENT_SOURCE_DIR}/../..
    )
    target_link_libraries(redir_server PUBLIC ObjectStore TestUtil)
endif()
