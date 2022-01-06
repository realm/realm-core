add_library(ShaWin32 sha224k.cpp sha256k.cpp)
add_library(Realm::ShaWin32 ALIAS ShaWin32)

target_include_directories(ShaWin32
	INTERFACE
		$<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/win32/kalven-sha2>
		$<INSTALL_INTERFACE:include/win32/kalven-sha2>
)

install(
	TARGETS ShaWin32
	EXPORT realm
	ARCHIVE DESTINATION lib
	LIBRARY DESTINATION lib
	RUNTIME DESTINATION bin
	INCLUDES DESTINATION include
)

install(FILES sha224.hpp DESTINATION include/win32/kalven-sha2)
