get_filename_component(POSTFIX ${CMAKE_CURRENT_LIST_DIR} NAME)
set(BIN benchmark-${POSTFIX})
set(CORE_BUILD core-build-${POSTFIX})

################
## Core Build ##
################

set(TAG v0.97.3)
set(REF "tags/${TAG}")

add_custom_target(
  ${CORE_BUILD}
  cd ../../.. && ./util/build-core.sh "${REF}"
)

include_directories("../../core-builds/${REF}/include")
link_directories("../../core-builds/${REF}/lib")

################
## The Binary ##
################

add_executable(${BIN} main.cpp)

add_dependencies(${BIN} ${CORE_BUILD})
target_link_libraries(${BIN} realm)
target_link_libraries(${BIN} util)
