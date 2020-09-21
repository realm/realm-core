# taken from https://github.com/cvmfs/cvmfs/commit/8ee3a8d7764ae25efdbdadf76a77ad5c23291deb

# --------------------------------------------------------------------
# - FindProcPS
#
# This module looks for the PROCPS software and defines
#    PROCPS_FOUND        , set to TRUE if the PROCPS is found
#    PROCPS_INCLUDE_DIR  , include directories for PROCPS
#    PROCPS_LIBRARY_DIR  , link directories for PROCPS libraries
#    PROCPS_LIBRARY      , the PROCPS libraries
# --------------------------------------------------------------------

# Include directory finding process
find_path(
    PROCPS_INCLUDE_DIR
    NAMES          procps.h readproc.h
    PATH_SUFFIXES  proc
    PATHS          /usr/local/include /usr/include
    DOC            "ProcPS library include path"
)

# Library finding process
find_library(
    PROCPS_LIBRARY
    NAMES  proc
    PATHS  /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
    DOC    "ProcPS library location"
)

if(PROCPS_LIBRARY)
    if(NOT TARGET PROCPS::PROCPS)
        add_library(PROCPS::PROCPS UNKNOWN IMPORTED)
        set_target_properties(PROCPS::PROCPS PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${PROCPS_INCLUDE_DIR}"
            IMPORTED_LOCATION "${PROCPS_LIBRARY}"
        )
    endif()
endif()

# Handle the QUIETLY and REQUIRED arguments and set PROCPS_FOUND to TRUE
# if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PROCPS DEFAULT_MSG
    PROCPS_INCLUDE_DIR
    PROCPS_LIBRARY
)

mark_as_advanced(PROCPS_INCLUDE_DIR PROCPS_LIBRARY)