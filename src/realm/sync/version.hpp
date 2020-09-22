#ifndef REALM_SYNC_VERSION_HPP
#define REALM_SYNC_VERSION_HPP

#include <realm/util/features.h>

// clang-format off
#define REALM_SYNC_VER_MAJOR 10
#define REALM_SYNC_VER_MINOR 0
#define REALM_SYNC_VER_PATCH 0-beta.11
#define REALM_SYNC_PRODUCT_NAME "realm-sync"
// clang-format on

#define REALM_SYNC_VER_STRING                                                                                        \
    REALM_QUOTE(REALM_SYNC_VER_MAJOR) "." REALM_QUOTE(REALM_SYNC_VER_MINOR) "." REALM_QUOTE(REALM_SYNC_VER_PATCH)
#define REALM_SYNC_VER_CHUNK "[" REALM_SYNC_PRODUCT_NAME "-" REALM_SYNC_VER_STRING "]"

#endif // REALM_SYNC_VERSION_HPP
