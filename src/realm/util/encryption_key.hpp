#ifndef REALM_UTIL_ENCRYPTION_KEY_HPP
#define REALM_UTIL_ENCRYPTION_KEY_HPP

#include <realm/util/sensitive_buffer.hpp>

#include <array>

namespace realm::util {

typedef SensitiveBuffer<std::array<uint8_t, 64>> EncryptionKey;

typedef EncryptionKey::element_type EncryptionKeyStorageType;
static_assert(std::is_pod<EncryptionKeyStorageType>::value); // for the use with the reinterpret_cast
static_assert(std::is_pod<std::decay<decltype(*EncryptionKey().data())>::type>::value);

constexpr size_t EncryptionKeySize = std::tuple_size<decltype(EncryptionKeyStorageType())>::value;
static_assert(EncryptionKeySize == 64);

} // namespace realm::util

#endif // REALM_UTIL_ENCRYPTION_KEY_HPP
