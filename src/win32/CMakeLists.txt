add_library(getopt_win32 INTERFACE)
target_include_directories(getopt_win32
    INTERFACE
        $<BUILD_INTERFACE:${RealmCore_SOURCE_DIR}/win32>
        $<INSTALL_INTERFACE:include/win32>
)

install(FILES getopt.h
        DESTINATION include/win32
        COMPONENT devel)

add_subdirectory(kalven-sha2)
