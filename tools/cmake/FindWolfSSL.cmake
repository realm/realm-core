#[=======================================================================[.rst:
FindWolfSSL
---------

Find WolfSSL includes and library. For now this only works when pointed to the output directory
of a wolfSSL build that was configured using the following configure line:
./configure --enable-static --enable-certgen --enable-opensslall --enable-opensslextra \
    --enable-context-extra-user-data --prefix=/path/to/output-dir


Imported Targets
^^^^^^^^^^^^^^^^

An :ref:`imported target <Imported targets>` named
``WolfSSL`` is provided if WolfSSL has been found.

Result Variables
^^^^^^^^^^^^^^^^

This module defines the following variables:

``WolfSSL_FOUND``
  True if WolfSSL was found, false otherwise.
``WolfSSL_INCLUDE_DIRS``
  Include directories needed to include WolfSSL headers.
``WolfSSL_LIBRARIES``
  Libraries needed to link to WolfSSL.


Hints
^^^^^

The following variables may be set to control search behavior:

``WOLFSSL_ROOT_DIR``
  Set to the root directory of a wolfSSL installation.

``WOLFSSL_USE_STATIC_LIBS``
  Set to ``TRUE`` to look for static libraries.


#]=======================================================================]

#-----------------------------------------------------------------------------
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "FindWolfSSL.cmake only supports Linux, for now...")
endif()

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if(WOLFSSL_USE_STATIC_LIBS)
  # save the original find library ordering so we can restore it later
  set(WOLFSSL_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  # replace with our static lib suffix
  set(CMAKE_FIND_LIBRARY_SUFFIXES .a )
endif()

if(EXISTS ${WOLFSSL_ROOT_DIR})
    set(CMAKE_PREFIX_PATH ${WOLFSSL_ROOT_DIR})
endif()

find_library(WolfSSL_LIBRARY NAMES wolfssl libwolfssl HINTS ${WOLFSSL_ROOT_DIR})
mark_as_advanced(WolfSSL_LIBRARY)

find_path(WolfSSL_INCLUDE_DIR NAMES wolfssl/ssl.h HINTS ${WOLFSSL_ROOT_DIR})
mark_as_advanced(WolfSSL_INCLUDE_DIR)

# we need to add one additional include for the openssl compatibility layer
find_path(WolfSSL_OpenSSL_Compat_INCLUDE_DIR NAMES openssl/ssl.h HINTS ${WOLFSSL_ROOT_DIR} PATH_SUFFIXES "wolfssl")
list(APPEND WolfSSL_INCLUDE_DIR ${WolfSSL_OpenSSL_Compat_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(WolfSSL
    FOUND_VAR WolfSSL_FOUND
    REQUIRED_VARS WolfSSL_LIBRARY WolfSSL_INCLUDE_DIR
    )
set(WOLFSSL_FOUND ${WolfSSL_FOUND})

#-----------------------------------------------------------------------------
# Provide documented result variables and targets.
if(WolfSSL_FOUND)
    set(WolfSSL_INCLUDE_DIRS ${WolfSSL_INCLUDE_DIR})
    set(WolfSSL_LIBRARIES ${WolfSSL_LIBRARY})
    if(NOT TARGET WolfSSL)
        add_library(WolfSSL UNKNOWN IMPORTED)
        set_target_properties(WolfSSL PROPERTIES
            IMPORTED_LOCATION "${WolfSSL_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${WolfSSL_INCLUDE_DIRS}"
            )
    endif()
endif()


if(WOLFSSL_USE_STATIC_LIBS)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${WOLFSSL_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()
unset(CMAKE_PREFIX_PATH)
