# - try to find procps directories and libraries
#
# Once done this will define:
#
#  PROCPS_FOUND
#  PROCPS_INCLUDE_DIR
#  PROCPS_LIBRARY
#

include (FindPackageHandleStandardArgs)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_path(PROCPS_INCLUDE_DIR proc/readproc.h)
  find_library(PROCPS_LIBRARY NAMES proc procps)
  find_package_handle_standard_args(procps DEFAULT_MSG PROCPS_LIBRARY PROCPS_INCLUDE_DIR)

  add_library(ProcPs SHARED IMPORTED)
  set_target_properties(ProcPs PROPERTIES
                      IMPORTED_LOCATION "${PROCPS_LIBRARY}"
                      INTERFACE_INCLUDE_DIRECTORIES "${PROCPS_INCLUDE_DIR}")
endif()
