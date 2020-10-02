add_subdirectory(realm)
add_subdirectory(external/IntelRDFPMathLib20U2)

if(CMAKE_SYSTEM_NAME MATCHES "^Windows")
  add_subdirectory(win32)
endif()

install(FILES realm.hpp
        DESTINATION include
        COMPONENT devel)

  
set(REALM_FFI_SOURCES
  realm/object-store/c_api.cpp
  realm/object-store/c_api/realm.c
  realm/realm.h
)

add_library(RealmFFI SHARED ${REALM_FFI_SOURCES})
add_library(RealmFFIStatic STATIC ${REALM_FFI_SOURCES})
target_link_libraries(RealmFFI PRIVATE Storage Sync ObjectStore QueryParser)
target_link_libraries(RealmFFI PRIVATE Storage Sync ObjectStore QueryParser)

target_compile_definitions(RealmFFI PRIVATE -DRealm_EXPORTS)
target_compile_definitions(RealmFFIStatic INTERFACE -DRLM_NO_DLLIMPORT)

set_target_properties(RealmFFI PROPERTIES
  OUTPUT_NAME "realm-ffi"
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

set_target_properties(RealmFFIStatic PROPERTIES
  OUTPUT_NAME "realm-ffi-static"
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

install(TARGETS RealmFFI DESTINATION lib COMPONENT devel)
install(FILES realm/realm.h DESTINATION include/realm COMPONENT devel)