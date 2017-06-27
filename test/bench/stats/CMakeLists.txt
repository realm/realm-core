set(REALM_STATS_SOURCES
    main.cpp
) # REALM_STATS_SOURCES

add_executable(realm-stats ${REALM_STATS_SOURCES})

target_link_libraries(realm-stats realm)

file(COPY "collect_stats.py"
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})


