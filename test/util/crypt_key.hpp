/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_CRYPT_KEY_HPP
#define REALM_TEST_UTIL_CRYPT_KEY_HPP

namespace realm {
namespace test_util {

/// Returns a non-null encryption key if encryption is enabled at compile-time
/// (REALM_ENABLE_ENCRYPTION), and either \a always is true or global mode
/// "always encrypt" is enabled. Otherwise it returns null. The global mode
/// "always encrypt" can be enabled by calling always_encrypt(), but pay
/// attention to the rules governing its use.
///
/// This function is thread-safe as long as there are no concurrent invocations
/// of always_encrypt().
const char* crypt_key(bool always=false);

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

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_CRYPT_KEY_HPP
