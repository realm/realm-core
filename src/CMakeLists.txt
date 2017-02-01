add_subdirectory(realm)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
  add_subdirectory(win32)
endif()

install(FILES realm.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT devel)
