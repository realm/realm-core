add_library(GetOptWin32 INTERFACE)
add_library(Realm::GetOptWin32 ALIAS GetOptWin32)

target_include_directories(GetOptWin32
	INTERFACE
		$<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/win32>
		$<INSTALL_INTERFACE:include/win32>
)

install(TARGETS GetOptWin32 EXPORT realm INCLUDES DESTINATION include/win32)
install(FILES getopt.h DESTINATION include/win32)

add_subdirectory(kalven-sha2)
