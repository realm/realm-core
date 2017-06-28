add_library(sha_win32 sha224k.cpp sha256k.cpp sha224.hpp)

install(TARGETS sha_win32 ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT devel)
install(FILES sha224.hpp sha256.hpp sha384.hpp sha512.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/win32 COMPONENT devel)
