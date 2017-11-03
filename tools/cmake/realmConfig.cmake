include(CMakeFindDependencyMacro)

if(UNIX AND NOT APPLE)
	if(ANDROID)
        string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE)
        set(OPENSSL_FILENAME "openssl-${BUILD_TYPE}-${ANDROID_OPENSSL_VERSION}-${ANDROID_OPENSSL_BUILD_NUMBER}-Android-${ANDROID_ABI}")
        set(OPENSSL_URL "http://static.realm.io/downloads/openssl/${ANDROID_OPENSSL_VERSION}/Android/${ANDROID_ABI}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Downloading OpenSSL...")
        file(DOWNLOAD "${OPENSSL_URL}" "${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}.tar.gz")

        message(STATUS "Uncompressing OpenSSL...")
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar xfz "${OPENSSL_FILENAME}.tar.gz")

        set(CMAKE_FIND_ROOT_PATH "${CMAKE_BINARY_DIR}/${OPENSSL_FILENAME}")
	endif()
	find_dependency(OpenSSL)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_dependency(Threads)

include("${CMAKE_CURRENT_LIST_DIR}/realmTargets.cmake")