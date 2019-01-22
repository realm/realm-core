include(CMakeFindDependencyMacro)

if(APPLE)
    find_library(Foundation Foundation)
    target_link_libraries(RealmCore::Core ${Foundation})
elseif(ANDROID)
    string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
    set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${ANDROID_ABI}/openssl.tgz")

    message(STATUS "Downloading OpenSSL...")
    file(DOWNLOAD "${OPENSSL_URL}" "${CMAKE_BINARY_DIR}/openssl/openssl.tgz" STATUS download_status)

    list(GET download_status 0 status_code)
    if (NOT "${status_code}" STREQUAL "0")
        message(FATAL_ERROR "Downloading ${url}... Failed. Status: ${download_status}")
    endif()

    message(STATUS "Uncompressing OpenSSL...")
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E tar xfz "openssl.tgz"
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/openssl"
    )

    message(STATUS "Importing OpenSSL...")
    set(OpenSSL_DIR "${CMAKE_BINARY_DIR}/openssl/lib/cmake/OpenSSL")
    find_dependency(OpenSSL REQUIRED CONFIG)
else() # Unix systems and Windows
    find_dependency(OpenSSL REQUIRED)
endif()

include("${CMAKE_CURRENT_LIST_DIR}/RealmCoreTargets.cmake")
