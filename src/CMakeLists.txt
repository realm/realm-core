add_subdirectory(realm)
add_subdirectory(external/IntelRDFPMathLib20U2)
add_subdirectory(external/s2)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
  add_subdirectory(win32)
endif()

install(FILES realm.hpp
        DESTINATION include
        COMPONENT devel)
