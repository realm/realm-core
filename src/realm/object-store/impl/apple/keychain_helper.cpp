////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/impl/apple/keychain_helper.hpp>

#include <realm/util/cf_ptr.hpp>
#include <realm/util/optional.hpp>

#include <Security/Security.h>

#include <string>

using namespace realm;
using util::adoptCF;
using util::CFPtr;
using util::retainCF;

namespace {

std::runtime_error keychain_access_exception(int32_t error_code)
{
    return std::runtime_error(util::format("Keychain returned unexpected status code: %1", error_code));
}

constexpr size_t key_size = 64;
const CFStringRef s_account = CFSTR("metadata");
const CFStringRef s_legacy_service = CFSTR("io.realm.sync.keychain");

#if !TARGET_IPHONE_SIMULATOR
CFPtr<CFStringRef> convert_string(const std::string& string)
{
    auto result = adoptCF(CFStringCreateWithBytes(nullptr, reinterpret_cast<const UInt8*>(string.data()),
                                                  string.size(), kCFStringEncodingASCII, false));
    if (!result) {
        throw std::bad_alloc();
    }
    return result;
}
#endif

CFPtr<CFMutableDictionaryRef> build_search_dictionary(CFStringRef account, CFStringRef service,
                                                      __unused util::Optional<std::string> group)
{
    auto d = adoptCF(
        CFDictionaryCreateMutable(nullptr, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks));
    if (!d)
        throw std::bad_alloc();

    CFDictionaryAddValue(d.get(), kSecClass, kSecClassGenericPassword);
    CFDictionaryAddValue(d.get(), kSecReturnData, kCFBooleanTrue);
    CFDictionaryAddValue(d.get(), kSecAttrAccount, account);
    CFDictionaryAddValue(d.get(), kSecAttrService, service);
#if !TARGET_IPHONE_SIMULATOR
    if (group)
        CFDictionaryAddValue(d.get(), kSecAttrAccessGroup, convert_string(*group).get());
#endif
    return d;
}

/// Get the encryption key for a given service, returning true if it either exists or the keychain is not usable.
bool get_key(CFStringRef account, CFStringRef service, util::Optional<std::vector<char>>& result)
{
    auto search_dictionary = build_search_dictionary(account, service, none);
    CFDataRef retained_key_data;
    switch (OSStatus status = SecItemCopyMatching(search_dictionary.get(), (CFTypeRef*)&retained_key_data)) {
        case errSecSuccess: {
            // Key was previously stored. Extract it.
            CFPtr<CFDataRef> key_data = adoptCF(retained_key_data);
            if (key_size != CFDataGetLength(key_data.get()))
                return false;

            auto key_bytes = reinterpret_cast<const char*>(CFDataGetBytePtr(key_data.get()));
            result.emplace(key_bytes, key_bytes + key_size);
            return true;
        }
        case errSecItemNotFound:
            return false;
        case errSecUserCanceled:
            // Keychain is locked, and user did not enter the password to unlock it.
        case errSecInvalidKeychain:
            // The keychain is corrupted and cannot be used.
        case errSecInteractionNotAllowed:
            // We asked for it to not prompt the user and a prompt was needed
            return true;
        default:
            throw keychain_access_exception(status);
    }
}

void set_key(util::Optional<std::vector<char>>& key, CFStringRef account, CFStringRef service)
{
    if (!key)
        return;

    auto search_dictionary = build_search_dictionary(account, service, none);
    CFDictionaryAddValue(search_dictionary.get(), kSecAttrAccessible, kSecAttrAccessibleAfterFirstUnlock);
    auto key_data = adoptCF(CFDataCreate(nullptr, reinterpret_cast<const UInt8*>(key->data()), key_size));
    if (!key_data)
        throw std::bad_alloc();

    CFDictionaryAddValue(search_dictionary.get(), kSecValueData, key_data.get());
    switch (OSStatus status = SecItemAdd(search_dictionary.get(), nullptr)) {
        case errSecSuccess:
            return;
        case errSecDuplicateItem:
            // A keychain item already exists but we didn't fine it in get_key(),
            // meaning that we didn't have permission to access it.
        case errSecUserCanceled:
        case errSecInteractionNotAllowed:
        case errSecInvalidKeychain:
            // We were unable to save the key for "expected" reasons, so proceeed unencrypted
            key = none;
            return;
        default:
            // Unexpected keychain failure happened
            throw keychain_access_exception(status);
    }
}

void delete_key(CFStringRef account, CFStringRef service)
{
    auto search_dictionary = build_search_dictionary(account, service, none);
    auto status = SecItemDelete(search_dictionary.get());
    REALM_ASSERT(status == errSecSuccess || status == errSecItemNotFound);
}

CFPtr<CFStringRef> get_service_name(bool& have_bundle_id)
{
    CFPtr<CFStringRef> service;
    if (CFStringRef bundle_id = CFBundleGetIdentifier(CFBundleGetMainBundle())) {
        service = adoptCF(CFStringCreateWithFormat(NULL, NULL, CFSTR("%@ - Realm Sync Metadata Key"), bundle_id));
        have_bundle_id = true;
    }
    else {
        service = retainCF(s_legacy_service);
        have_bundle_id = false;
    }
    return service;
}

} // anonymous namespace

namespace realm::keychain {

util::Optional<std::vector<char>> get_existing_metadata_realm_key()
{
    bool have_bundle_id = false;
    CFPtr<CFStringRef> service = get_service_name(have_bundle_id);

    // Try retrieving the existing key.
    util::Optional<std::vector<char>> key;
    if (get_key(s_account, service.get(), key)) {
        return key;
    }

    if (have_bundle_id) {
        // See if there's a key stored using the legacy shared keychain item.
        if (get_key(s_account, s_legacy_service, key)) {
            // If so, copy it to the per-app keychain item before returning it.
            set_key(key, s_account, service.get());
            return key;
        }
    }
    return util::none;
}

util::Optional<std::vector<char>> create_new_metadata_realm_key()
{
    bool have_bundle_id = false;
    CFPtr<CFStringRef> service = get_service_name(have_bundle_id);

    util::Optional<std::vector<char>> key;
    key.emplace(key_size);
    arc4random_buf(key->data(), key_size);
    set_key(key, s_account, service.get());
    return key;
}

void delete_metadata_realm_encryption_key()
{
    delete_key(s_account, s_legacy_service);
    if (CFStringRef bundle_id = CFBundleGetIdentifier(CFBundleGetMainBundle())) {
        auto service =
            adoptCF(CFStringCreateWithFormat(NULL, NULL, CFSTR("%@ - Realm Sync Metadata Key"), bundle_id));
        delete_key(s_account, service.get());
    }
}

} // namespace realm::keychain
