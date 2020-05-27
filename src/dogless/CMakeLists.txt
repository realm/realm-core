
set(DOGLESS_VERSION_MAJOR 0)
set(DOGLESS_VERSION_MINOR 2)
set(DOGLESS_VERSION_PATCH 0)

set(DOGLESS_HEADERS
    dogstatsd.hpp
    hostname.hpp
    statsd.hpp
    stats_collector.hpp
) # DOGLESS_HEADERS
set(DOGLESS_SOURCES
    dogstatsd.cpp
    hostname.cpp
    statsd.cpp
) # DOGLESS_SOURCES
set(UTILS_HEADERS
    utils/io.hpp
    utils/misc.hpp
    utils/random.hpp
    utils/sockets.hpp
) # UTILS_HEADERS
set(UTILS_SOURCES
    utils/io.cpp
    utils/sockets.cpp
) # UTILS_SOURCES

find_path(STANDALONE_ASIO_INCLUDE asio.hpp HINTS "/usr/include" "/usr/local/include")
if(STANDALONE_ASIO_INCLUDE)
  include_directories(${STANDALONE_ASIO_INCLUDE})
  set(ASIO_LIBRARIES "-lpthread")
  set(HAVE_STANDALONE_ASIO true)
  message(STATUS "${Green}Found standalone ASIO include at: "
                 "${STANDALONE_ASIO_INCLUDE}${Reset}")
else()
  message(FATAL_ERROR "${Red}Failed to locate standalone ASIO dependency.${Reset}")
endif()

add_library(
  Dogless STATIC
  ${DOGLESS_SOURCES} ${DOGLESS_HEADERS} ${UTILS_SOURCES} ${UTILS_HEADERS})

set_target_properties(
  Dogless PROPERTIES
  OUTPUT_NAME dogless
  VERSION ${PROJECT_VERSION}
  POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(
    Dogless ${ASIO_LIBRARIES} Storage
)
