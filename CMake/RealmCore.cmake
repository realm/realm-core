###########################################################################
#
# Copyright 2016 Realm Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
###########################################################################

include(ExternalProject)
include(ProcessorCount)

if(${CMAKE_GENERATOR} STREQUAL "Unix Makefiles")
    set(MAKE_EQUAL_MAKE "MAKE=$(MAKE)")
endif()

set(MAKE_FLAGS "REALM_HAVE_CONFIG=1")

if(SANITIZER_FLAGS)
  set(MAKE_FLAGS ${MAKE_FLAGS} "EXTRA_CFLAGS=${SANITIZER_FLAGS}" "EXTRA_LDFLAGS=${SANITIZER_FLAGS}")
endif()

ProcessorCount(NUM_JOBS)
if(NOT NUM_JOBS EQUAL 0)
    set(MAKE_FLAGS ${MAKE_FLAGS} "-j${NUM_JOBS}")
endif()

if (${CMAKE_VERSION} VERSION_GREATER "3.4.0")
    set(USES_TERMINAL_BUILD USES_TERMINAL_BUILD 1)
endif()

function(use_realm_core version_or_path_to_source)
    if("${version_or_path_to_source}" MATCHES "^[0-9]+(\\.[0-9])+")
        if(APPLE OR REALM_PLATFORM STREQUAL "Android")
            download_realm_core(${version_or_path_to_source})
        else()
            clone_and_build_realm_core("v${version_or_path_to_source}")
        endif()
    else()
        build_existing_realm_core(${version_or_path_to_source})
    endif()
    set(REALM_CORE_INCLUDE_DIR ${REALM_CORE_INCLUDE_DIR} PARENT_SCOPE)
endfunction()

function(download_realm_core core_version)
    if(APPLE)
        set(core_basename "realm-core")
        set(core_compression "xz")
        set(core_platform "")
    elseif(REALM_PLATFORM STREQUAL "Android")
        set(core_basename "realm-core-android")
        set(core_compression "gz")
        set(core_platform "-android-x86_64")
    endif()
    set(core_tarball_name "${core_basename}-${core_version}.tar.${core_compression}")
    set(core_url "https://static.realm.io/downloads/core/${core_tarball_name}")
    set(core_temp_tarball "/tmp/${core_tarball_name}")
    set(core_directory_parent "${CMAKE_CURRENT_SOURCE_DIR}${CMAKE_FILES_DIRECTORY}")
    set(core_directory "${core_directory_parent}/${core_basename}-${core_version}")
    set(core_tarball "${core_directory_parent}/${core_tarball_name}")

    if (NOT EXISTS ${core_tarball})
        if (NOT EXISTS ${core_temp_tarball})
            message("Downloading core ${core_version} from ${core_url}.")
            file(DOWNLOAD ${core_url} ${core_temp_tarball}.tmp SHOW_PROGRESS)
            file(RENAME ${core_temp_tarball}.tmp ${core_temp_tarball})
        endif()
        file(COPY ${core_temp_tarball} DESTINATION ${core_directory_parent})
    endif()

    set(core_library_debug ${core_directory}/librealm${core_platform}-dbg.a)
    set(core_library_release ${core_directory}/librealm${core_platform}.a)
    set(core_libraries ${core_library_debug} ${core_library_release})

    if(APPLE)
        add_custom_command(
            COMMENT "Extracting ${core_tarball_name}"
            OUTPUT ${core_libraries}
            DEPENDS ${core_tarball}
            COMMAND ${CMAKE_COMMAND} -E tar xf ${core_tarball}
            COMMAND ${CMAKE_COMMAND} -E remove_directory ${core_directory}
            COMMAND ${CMAKE_COMMAND} -E rename core ${core_directory}
            COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${core_libraries})
    elseif(REALM_PLATFORM STREQUAL "Android")
        add_custom_command(
            COMMENT "Extracting ${core_tarball_name}"
            OUTPUT ${core_libraries}
            DEPENDS ${core_tarball}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${core_directory}
            COMMAND ${CMAKE_COMMAND} -E chdir ${core_directory} tar xf ${core_tarball}
            COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${core_libraries})
    endif()

    add_custom_target(realm-core DEPENDS ${core_libraries})

    add_library(realm STATIC IMPORTED)
    add_dependencies(realm realm-core)
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_DEBUG ${core_library_debug})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_COVERAGE ${core_library_debug})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_RELEASE ${core_library_release})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION ${core_library_release})

    set(REALM_CORE_INCLUDE_DIR ${core_directory}/include PARENT_SCOPE)
endfunction()

macro(define_built_realm_core_target core_directory)
    set(core_library_debug ${core_directory}/src/realm/librealm-dbg.a)
    set(core_library_release ${core_directory}/src/realm/librealm.a)
    set(core_libraries ${core_library_debug} ${core_library_release})

    ExternalProject_Add_Step(realm-core ensure-libraries
        COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${core_libraries}
        OUTPUT ${core_libraries}
        DEPENDEES build
        )

    add_library(realm STATIC IMPORTED)
    add_dependencies(realm realm-core)

    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_DEBUG ${core_library_debug})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_COVERAGE ${core_library_debug})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION_RELEASE ${core_library_release})
    set_property(TARGET realm PROPERTY IMPORTED_LOCATION ${core_library_release})

    set(REALM_CORE_INCLUDE_DIR ${core_directory}/src PARENT_SCOPE)
endmacro()

function(clone_and_build_realm_core branch)
    set(core_prefix_directory "${CMAKE_CURRENT_SOURCE_DIR}${CMAKE_FILES_DIRECTORY}/realm-core")
    ExternalProject_Add(realm-core
        GIT_REPOSITORY "git@github.com:realm/realm-core.git"
        GIT_TAG ${branch}
        PREFIX ${core_prefix_directory}
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make -C src/realm librealm.a librealm-dbg.a ${MAKE_FLAGS}
        INSTALL_COMMAND ""
        ${USES_TERMINAL_BUILD}
        )

    ExternalProject_Get_Property(realm-core SOURCE_DIR)
    define_built_realm_core_target(${SOURCE_DIR})
endfunction()

function(build_existing_realm_core core_directory)
    get_filename_component(core_directory ${core_directory} ABSOLUTE)
    ExternalProject_Add(realm-core
        URL ""
        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}${CMAKE_FILES_DIRECTORY}/realm-core
        SOURCE_DIR ${core_directory}
        BUILD_IN_SOURCE 1
        BUILD_ALWAYS 1
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make -C src/realm librealm.a librealm-dbg.a ${MAKE_FLAGS}
        INSTALL_COMMAND ""
        ${USES_TERMINAL_BUILD}
        )

    define_built_realm_core_target(${core_directory})
endfunction()

function(build_realm_sync sync_directory)
    get_filename_component(sync_directory ${sync_directory} ABSOLUTE)
    if(APPLE)
        find_library(FOUNDATION Foundation)
        find_library(SECURITY Security)
    endif()
    ExternalProject_Add(realm-sync-lib
        DEPENDS realm-core
        URL ""
        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}${CMAKE_FILES_DIRECTORY}/realm-sync
        SOURCE_DIR ${sync_directory}
        BUILD_IN_SOURCE 1
        BUILD_ALWAYS 1
        CONFIGURE_COMMAND ""
        BUILD_COMMAND make -C src/realm librealm-sync.a librealm-sync-dbg.a librealm-server.a librealm-server-dbg.a ${MAKE_FLAGS}
        INSTALL_COMMAND ""
        ${USES_TERMINAL_BUILD}
        )
    set(sync_library_debug ${sync_directory}/src/realm/librealm-sync-dbg.a)
    set(sync_library_release ${sync_directory}/src/realm/librealm-sync.a)
    set(sync_libraries ${sync_library_debug} ${sync_library_release})

    ExternalProject_Add_Step(realm-sync-lib ensure-libraries
        COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${sync_libraries}
        OUTPUT ${sync_libraries}
        DEPENDEES build
        )

    add_library(realm-sync STATIC IMPORTED)
    add_dependencies(realm-sync realm-sync-lib)

    set_property(TARGET realm-sync PROPERTY IMPORTED_LOCATION_DEBUG ${sync_library_debug})
    set_property(TARGET realm-sync PROPERTY IMPORTED_LOCATION_COVERAGE ${sync_library_debug})
    set_property(TARGET realm-sync PROPERTY IMPORTED_LOCATION_RELEASE ${sync_library_release})
    set_property(TARGET realm-sync PROPERTY IMPORTED_LOCATION ${sync_library_release})
    if(APPLE)
        set_property(TARGET realm-sync PROPERTY INTERFACE_LINK_LIBRARIES ${FOUNDATION} ${SECURITY})
    else()
        set_property(TARGET realm-sync PROPERTY INTERFACE_LINK_LIBRARIES -lcrypto -lssl)
    endif()

    # Sync server library is built as part of the sync library build
    ExternalProject_Add(realm-server-lib
        DEPENDS realm-core
        DOWNLOAD_COMMAND ""
        PREFIX ${CMAKE_CURRENT_SOURCE_DIR}${CMAKE_FILES_DIRECTORY}/realm-sync
        SOURCE_DIR ${sync_directory}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        )
    set(sync_server_library_debug ${sync_directory}/src/realm/librealm-server-dbg.a)
    set(sync_server_library_release ${sync_directory}/src/realm/librealm-server.a)
    set(sync_server_libraries ${sync_server_library_debug} ${sync_server_library_release})

    ExternalProject_Add_Step(realm-server-lib ensure-server-libraries
        COMMAND ${CMAKE_COMMAND} -E touch_nocreate ${sync_server_libraries}
        OUTPUT ${sync_server_libraries}
        DEPENDEES build
        )

    add_library(realm-sync-server STATIC IMPORTED)
    add_dependencies(realm-sync-server realm-server-lib)

    set_property(TARGET realm-sync-server PROPERTY IMPORTED_LOCATION_DEBUG ${sync_server_library_debug})
    set_property(TARGET realm-sync-server PROPERTY IMPORTED_LOCATION_COVERAGE ${sync_server_library_debug})
    set_property(TARGET realm-sync-server PROPERTY IMPORTED_LOCATION_RELEASE ${sync_server_library_release})
    set_property(TARGET realm-sync-server PROPERTY IMPORTED_LOCATION ${sync_server_library_release})
    if(APPLE)
        set_property(TARGET realm-sync-server PROPERTY INTERFACE_LINK_LIBRARIES ${FOUNDATION} ${SECURITY})
    else()
        set_property(TARGET realm-sync-server PROPERTY INTERFACE_LINK_LIBRARIES -lcrypto -lssl) 
    endif()

    set(REALM_SYNC_INCLUDE_DIR ${sync_directory}/src PARENT_SCOPE)
endfunction()
