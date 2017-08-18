/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/features.h>

#include "crypt_key.hpp"


namespace {

bool g_always_encrypt = false;

} // unnamed namespace


namespace realm {
namespace test_util {

const char* crypt_key(bool always)
{
    return 0;

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
