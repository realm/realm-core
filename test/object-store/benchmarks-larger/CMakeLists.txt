add_executable(object-store-benchmark-larger main.cpp)
target_link_libraries(object-store-benchmark-larger TestUtil ObjectStore RealmFFIStatic ${libuv_target})
