add_subdirectory(realm)
add_subdirectory(external/IntelRDFPMathLib20U2)
if (NOT REALM_BUILD_LIB_ONLY)
    add_subdirectory(external/bson EXCLUDE_FROM_ALL)
endif()

if (REALM_ENABLE_GEOSPATIAL)
    add_subdirectory(external/s2)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
  add_subdirectory(win32)
endif()

install(FILES realm.hpp
        DESTINATION include
        COMPONENT devel)
