#include <realm/util/features.h>

#include "crypt_key.hpp"


namespace {

bool g_always_encrypt = false;

} // unnamed namespace


namespace realm {
namespace test_util {

const char* crypt_key(bool always)
{
#if REALM_ENABLE_ENCRYPTION
    if (always || g_always_encrypt)
        return "1234567890123456789012345678901123456789012345678901234567890123";
#else
    static_cast<void>(always);
#endif
    return nullptr;
}


bool is_always_encrypt_enabled()
{
    return g_always_encrypt;
}


void enable_always_encrypt()
{
    g_always_encrypt = true;
}

} // namespace test_util
} // namespace realm
