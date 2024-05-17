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
#include <cstring>

#include "crypt_key.hpp"


namespace {

bool g_always_encrypt = false;

} // unnamed namespace


namespace realm {
namespace test_util {

std::optional<util::EncryptionKey> crypt_key(const char* raw_value, bool always)
{
#if REALM_ENABLE_ENCRYPTION
    if (raw_value != nullptr && (always || g_always_encrypt)) {
        std::array<uint8_t, 64> raw_key;
        REALM_ASSERT(strlen(raw_value) == raw_key.size());
        std::copy(raw_value, raw_value + 64, raw_key.begin());
        return util::EncryptionKey(raw_key);
    }
#else
    static_cast<void>(raw_value);
    static_cast<void>(always);
#endif
    return std::nullopt;
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
