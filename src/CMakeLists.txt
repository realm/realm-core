add_subdirectory(realm)

if(WIN32)
  add_subdirectory(win32)
endif()

install(FILES realm.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
