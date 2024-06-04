include(CheckFunctionExists)
include(CheckStructHasMember)

set(BSON_SRC
  bcon.c
  bson-clock.c
  bson-decimal128.c
  bson-error.c
  bson-iso8601.c
  bson-iter.c
  bson-json.c
  bson-keys.c
  bson-memory.c
  bson-oid.c
  bson-reader.c
  bson-string.c
  bson-timegm.c
  bson-utf8.c
  bson-value.c
  bson-version-functions.c
  bson-writer.c
  bson.c
  common-b64.c
  ../jsonsl/jsonsl.c
)

if(WIN32)
	set(BSON_OS 2)
else()
	set(BSON_OS 1)
endif()

check_symbol_exists (snprintf stdio.h BSON_HAVE_SNPRINTF)
CHECK_STRUCT_HAS_MEMBER ("struct timespec" tv_sec time.h BSON_HAVE_TIMESPEC)
check_symbol_exists (gmtime_r time.h BSON_HAVE_GMTIME_R)
check_function_exists (rand_r BSON_HAVE_RAND_R)
check_include_file (strings.h BSON_HAVE_STRINGS_H)
if (NOT APPLE)
    check_symbol_exists (strlcpy string.h BSON_HAVE_STRLCPY)
else()
    set(BSON_HAVE_STRLCPY Off)
endif()
check_include_file (stdbool.h BSON_HAVE_STDBOOL_H)
check_symbol_exists (clock_gettime time.h BSON_HAVE_CLOCK_GETTIME)
check_symbol_exists (strnlen string.h BSON_HAVE_STRNLEN)

configure_file (
   "${PROJECT_SOURCE_DIR}/src/external/bson/bson-config.h.in"
   "${PROJECT_BINARY_DIR}/src/external/bson/bson-config.h"
)

add_library(Bson STATIC ${BSON_SRC})
add_library(Realm::Bson ALIAS Bson)
target_compile_definitions(Bson PRIVATE _GNU_SOURCE _XOPEN_SOURCE=700 BSON_COMPILATION)
if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
  target_compile_definitions(Bson PRIVATE _DARWIN_C_SOURCE)
endif()
target_include_directories(Bson PUBLIC .. ${PROJECT_BINARY_DIR}/src/external)
target_compile_definitions(Bson INTERFACE BSON_STATIC)

set_target_properties(Bson PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    COMPILE_WARNING_AS_ERROR Off)
