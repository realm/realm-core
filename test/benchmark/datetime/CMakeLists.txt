set(BIN benchmark-datetime)

################
## Core Build ##
################

set(TAG v0.97.3)
set(CORE_BUILD "core-build-tags-${TAG}")
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
