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

#ifndef REALM_TEST_UTIL_CRYPT_KEY_HPP
#define REALM_TEST_UTIL_CRYPT_KEY_HPP

#include <string>

namespace realm {
namespace test_util {

/// Returns a non-null encryption key if encryption is enabled at compile-time
/// (REALM_ENABLE_ENCRYPTION), and either \a always is true or global mode
/// "always encrypt" is enabled. Otherwise it returns null. The global mode
/// "always encrypt" can be enabled by calling always_encrypt(), but pay
/// attention to the rules governing its use.
///
/// The returned string, if not null, is null terminated.
///
/// This function is thread-safe as long as there are no concurrent invocations
/// of always_encrypt().
const char* crypt_key(bool always = false);

/// Returns the empty string when, and only when crypt_key() returns null.
std::string crypt_key_2(bool always = false);

/// Returns true if global mode "always encrypt" is enabled.
///
/// This function is thread-safe as long as there are no concurrent invocations
/// of always_encrypt().
bool is_always_encrypt_enabled();

/// Enable global mode "always encrypt".
///
/// This function is **not** thread-safe. If you call it, be sure to call it
/// prior to any invocation of crypt_key().
void enable_always_encrypt();


// Implementation

inline std::string crypt_key_2(bool always)
{
    if (const char* key = crypt_key(always)) // Throws
        return {key};
    return {};
}

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_CRYPT_KEY_HPP
