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

#include <realm/exceptions.hpp>
#include <realm/util/cf_str.hpp>

#include <Security/Security.h>

#include <string>

using namespace realm;
using util::adoptCF;
using util::CFPtr;
using util::string_view_to_cfstring;

namespace {

REALM_NORETURN
REALM_COLD
void keychain_access_exception(int32_t error_code)
{
    if (auto message = adoptCF(SecCopyErrorMessageString(error_code, nullptr))) {
        if (auto msg = CFStringGetCStringPtr(message.get(), kCFStringEncodingUTF8)) {
            throw RuntimeError(ErrorCodes::RuntimeError,
                               util::format("Keychain returned unexpected status code: %1 (%2)", msg, error_code));
        }
        auto length = CFStringGetMaximumSizeForEncoding(CFStringGetLength(message.get()), kCFStringEncodingUTF8) + 1;
        auto buffer = std::make_unique<char[]>(length);
        if (CFStringGetCString(message.get(), buffer.get(), length, kCFStringEncodingUTF8)) {
            throw RuntimeError(
                ErrorCodes::RuntimeError,
                util::format("Keychain returned unexpected status code: %1 (%2)", buffer.get(), error_code));
        }
    }
    throw RuntimeError(ErrorCodes::RuntimeError,
                       util::format("Keychain returned unexpected status code: %1", error_code));
}

constexpr size_t key_size = 64;
const CFStringRef s_legacy_account = CFSTR("metadata");
const CFStringRef s_service = CFSTR("io.realm.sync.keychain");

CFPtr<CFMutableDictionaryRef> build_search_dictionary(CFStringRef account, CFStringRef service, CFStringRef group)
{
    auto d = adoptCF(
        CFDictionaryCreateMutable(nullptr, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks));
    if (!d)
        throw std::bad_alloc();

    CFDictionaryAddValue(d.get(), kSecClass, kSecClassGenericPassword);
    CFDictionaryAddValue(d.get(), kSecReturnData, kCFBooleanTrue);
    CFDictionaryAddValue(d.get(), kSecAttrAccount, account);
    CFDictionaryAddValue(d.get(), kSecAttrService, service);
    if (group) {
        CFDictionaryAddValue(d.get(), kSecAttrAccessGroup, group);
        if (__builtin_available(macOS 10.15, iOS 13.0, *)) {
            CFDictionaryAddValue(d.get(), kSecUseDataProtectionKeychain, kCFBooleanTrue);
        }
    }
    return d;
}

/// Get the encryption key for a given service, returning true if it either exists or the keychain is not usable.
bool get_key(CFStringRef account, CFStringRef service, std::string_view group,
             std::optional<std::vector<char>>& result, bool result_on_error = true)
{
    auto search_dictionary = build_search_dictionary(account, service, string_view_to_cfstring(group).get());
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
        case errSecNotAvailable:
            // There are no keychain files.
        case errSecInteractionNotAllowed:
            // We asked for it to not prompt the user and a prompt was needed
            return result_on_error;
        case errSecMissingEntitlement:
            throw InvalidArgument(util::format("Invalid access group '%1'. Make sure that you have added the access "
                                               "group to your app's Keychain Access Groups Entitlement.",
                                               group));
        default:
            keychain_access_exception(status);
    }
}

bool set_key(std::optional<std::vector<char>>& key, CFStringRef account, CFStringRef service, std::string_view group)
{
    // key may be nullopt here if the keychain was inaccessible
    if (!key)
        return false;

    auto search_dictionary = build_search_dictionary(account, service, string_view_to_cfstring(group).get());
    CFDictionaryAddValue(search_dictionary.get(), kSecAttrAccessible, kSecAttrAccessibleAfterFirstUnlock);
    auto key_data = adoptCF(CFDataCreateWithBytesNoCopy(nullptr, reinterpret_cast<const UInt8*>(key->data()),
                                                        key_size, kCFAllocatorNull));
    if (!key_data)
        throw std::bad_alloc();

    CFDictionaryAddValue(search_dictionary.get(), kSecValueData, key_data.get());
    switch (OSStatus status = SecItemAdd(search_dictionary.get(), nullptr)) {
        case errSecSuccess:
            return true;
        case errSecDuplicateItem:
            // A keychain item already exists but we didn't find it in get_key().
            // Either someone else created it between when we last checked and
            // now or we don't have permission to read it. Try to reread the key
            // and discard the one we just created in case it's the former
            if (get_key(account, service, group, key, false))
                return true;
        case errSecMissingEntitlement:
        case errSecUserCanceled:
        case errSecInteractionNotAllowed:
        case errSecInvalidKeychain:
        case errSecNotAvailable:
            // We were unable to save the key for "expected" reasons, so proceed unencrypted
            return false;
        default:
            // Unexpected keychain failure happened
            keychain_access_exception(status);
    }
}

void delete_key(CFStringRef account, CFStringRef service, CFStringRef group)
{
    auto search_dictionary = build_search_dictionary(account, service, group);
    auto status = SecItemDelete(search_dictionary.get());
    REALM_ASSERT(status == errSecSuccess || status == errSecItemNotFound);
}

CFPtr<CFStringRef> bundle_service()
{
    if (CFStringRef bundle_id = CFBundleGetIdentifier(CFBundleGetMainBundle())) {
        return adoptCF(CFStringCreateWithFormat(nullptr, nullptr, CFSTR("%@ - Realm Sync Metadata Key"), bundle_id));
    }
    return CFPtr<CFStringRef>{};
}

} // anonymous namespace

namespace realm::keychain {

std::optional<std::vector<char>> get_existing_metadata_realm_key(std::string_view app_id,
                                                                 std::string_view access_group)
{
    auto cf_app_id = string_view_to_cfstring(app_id);
    std::optional<std::vector<char>> key;

    // If we have a security access groups then keys are stored the same way
    // everywhere and we don't have any legacy storage methods to handle, so
    // we just either have a key or we don't.
    if (access_group.size()) {
        get_key(cf_app_id.get(), s_service, access_group, key);
        return key;
    }

    // When we don't have an access group we check a whole bunch of things because
    // there's been a variety of ways that we've stored metadata keys over the years.
    // If we find a key stored in a non-preferred way we copy it to the preferred
    // location before returning it.
    //
    // The original location was (account: "metadata", service: "io.realm.sync.keychain").
    // For processes with a bundle ID, we then switched to (account: "metadata",
    // service: "$bundleId - Realm Sync Metadata Key")
    // The current preferred location on non-macOS (account: appId, service: "io.realm.sync.keychain"),
    // and on macOS is (account: appId, service: "$bundleId - Realm Sync Metadata Key").
    //
    // On everything but macOS the keychain is scoped to the app, so there's no
    // need to include the bundle ID. On macOS it's user-wide, and we want each
    // application using Realm to have separate state. Using multiple server apps
    // in one client is unusual, but when it's done we want each metadata realm to
    // have a separate key.

#if TARGET_OS_MAC
    if (auto service = bundle_service()) {
        if (get_key(cf_app_id.get(), service.get(), {}, key))
            return key;
        if (get_key(s_legacy_account, service.get(), {}, key)) {
            set_key(key, cf_app_id.get(), service.get(), {});
            return key;
        }
        if (get_key(s_legacy_account, s_service, {}, key)) {
            set_key(key, cf_app_id.get(), service.get(), {});
            return key;
        }
    }
    else {
        if (get_key(cf_app_id.get(), s_service, {}, key))
            return key;
        if (get_key(s_legacy_account, s_service, {}, key)) {
            set_key(key, cf_app_id.get(), s_service, {});
            return key;
        }
    }
#else
    if (get_key(cf_app_id, s_service, {}, key))
        return key;
    if (auto service = bundle_service()) {
        if (get_key(cf_app_id, service, {}, key)) {
            set_key(key, cf_app_id, s_service, {});
            return key;
        }
    }
    if (get_key(s_legacy_account, s_service, {}, key)) {
        set_key(key, cf_app_id, s_service, {});
        return key;
    }
#endif

    return key;
}

std::optional<std::vector<char>> create_new_metadata_realm_key(std::string_view app_id, std::string_view access_group)
{
    auto cf_app_id = string_view_to_cfstring(app_id);
    std::optional<std::vector<char>> key;
    key.emplace(key_size);
    arc4random_buf(key->data(), key_size);

    // See above for why macOS is different
#if TARGET_OS_OSX
    if (!access_group.size()) {
        if (auto service = bundle_service()) {
            if (!set_key(key, cf_app_id.get(), service.get(), {}))
                key.reset();
            return key;
        }
    }
#endif

    // If we're unable to save the newly created key, clear it and proceed unencrypted
    if (!set_key(key, cf_app_id.get(), s_service, access_group))
        key.reset();
    return key;
}

void delete_metadata_realm_encryption_key(std::string_view app_id, std::string_view access_group)
{
    auto cf_app_id = string_view_to_cfstring(app_id);
    if (access_group.size()) {
        delete_key(cf_app_id.get(), s_service, string_view_to_cfstring(access_group).get());
        return;
    }

    delete_key(cf_app_id.get(), s_service, {});
    delete_key(s_legacy_account, s_service, {});
    if (auto service = bundle_service()) {
        delete_key(cf_app_id.get(), service.get(), {});
        delete_key(s_legacy_account, service.get(), {});
    }
}

} // namespace realm::keychain
