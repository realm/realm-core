////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#include <realm/list.hpp>
#include <realm/object-store/impl/apple/keychain_helper.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/sync/impl/app_metadata.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

#include <util/test_path.hpp>
#include <util/test_utils.hpp>

#include <iostream>

#if REALM_PLATFORM_APPLE
#include <realm/util/cf_str.hpp>
#include <Security/Security.h>
#endif

using namespace realm;
using namespace realm::app;
using realm::util::File;

namespace {
const std::string base_path = util::make_temp_dir() + "realm_objectstore_sync_metadata.test-dir";
const std::string metadata_path = base_path + "/mongodb-realm/app%20id/server-utility/metadata/sync_metadata.realm";
constexpr const char* user_id = "user_id";
constexpr const char* device_id = "device_id";
constexpr const char* app_id = "app id";
const auto access_token = encode_fake_jwt("access_token", 123, 456);
const auto refresh_token = encode_fake_jwt("refresh_token", 123, 456);

std::shared_ptr<Realm> get_metadata_realm()
{
    RealmConfig realm_config;
    realm_config.automatic_change_notifications = false;
    realm_config.path = metadata_path;
    return Realm::get_shared_realm(std::move(realm_config));
}

#if REALM_PLATFORM_APPLE
using realm::util::adoptCF;
using realm::util::CFPtr;

#if REALM_ENABLE_ENCRYPTION
constexpr const char* access_group = "";
bool can_access_keychain()
{
    static bool can_access_keychain = [] {
        bool can_access = keychain::create_new_metadata_realm_key(app_id, access_group) != none;
        if (can_access) {
            keychain::delete_metadata_realm_encryption_key(app_id, access_group);
        }
        else {
            std::cout << "Skipping keychain tests as the keychain is not accessible\n";
        }
        return can_access;
    }();
    return can_access_keychain;
}

CFPtr<CFMutableDictionaryRef> build_search_dictionary(CFStringRef account, CFStringRef service)
{
    auto d = adoptCF(
        CFDictionaryCreateMutable(nullptr, 0, &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks));
    CFDictionaryAddValue(d.get(), kSecClass, kSecClassGenericPassword);
    CFDictionaryAddValue(d.get(), kSecReturnData, kCFBooleanTrue);
    CFDictionaryAddValue(d.get(), kSecAttrAccount, account);
    CFDictionaryAddValue(d.get(), kSecAttrService, service);
    return d;
}

OSStatus get_key(CFStringRef account, CFStringRef service, std::vector<char>& result)
{
    auto search_dictionary = build_search_dictionary(account, service);
    CFDataRef retained_key_data;
    OSStatus status = SecItemCopyMatching(search_dictionary.get(), (CFTypeRef*)&retained_key_data);
    if (status == errSecSuccess) {
        CFPtr<CFDataRef> key_data = adoptCF(retained_key_data);
        auto key_bytes = reinterpret_cast<const char*>(CFDataGetBytePtr(key_data.get()));
        result.assign(key_bytes, key_bytes + CFDataGetLength(key_data.get()));
    }
    return status;
}

OSStatus set_key(const std::vector<char>& key, CFStringRef account, CFStringRef service)
{
    auto search_dictionary = build_search_dictionary(account, service);
    CFDictionaryAddValue(search_dictionary.get(), kSecAttrAccessible, kSecAttrAccessibleAfterFirstUnlock);
    auto key_data = adoptCF(CFDataCreateWithBytesNoCopy(nullptr, reinterpret_cast<const UInt8*>(key.data()),
                                                        key.size(), kCFAllocatorNull));
    CFDictionaryAddValue(search_dictionary.get(), kSecValueData, key_data.get());
    return SecItemAdd(search_dictionary.get(), nullptr);
}

std::vector<char> generate_key()
{
    std::vector<char> key(64);
    arc4random_buf(key.data(), key.size());
    return key;
}
#endif // REALM_ENABLE_ENCRYPTION
#endif // REALM_PLATFORM_APPLE
} // anonymous namespace

namespace realm::app {
static std::ostream& operator<<(std::ostream& os, AppConfig::MetadataMode mode)
{
    switch (mode) {
        case AppConfig::MetadataMode::InMemory:
            os << "InMemory";
            break;
        case AppConfig::MetadataMode::NoEncryption:
            os << "NoEncryption";
            break;
        case AppConfig::MetadataMode::Encryption:
            os << "Encryption";
            break;
        default:
            os << "unknown";
            break;
    }
    return os;
}
} // namespace realm::app

using Strings = std::vector<std::string>;

TEST_CASE("app metadata: common", "[sync][metadata]") {
    test_util::TestDirGuard test_dir(base_path);

    AppConfig config;
    config.app_id = app_id;
    config.metadata_mode = GENERATE(AppConfig::MetadataMode::InMemory, AppConfig::MetadataMode::NoEncryption);
    config.base_file_path = base_path;
    SyncFileManager file_manager(config);
    auto store = create_metadata_store(config, file_manager);

    INFO(config.metadata_mode);

    SECTION("create_user() creates new logged-in users") {
        REQUIRE_FALSE(store->has_logged_in_user(user_id));
        store->create_user(user_id, refresh_token, access_token, device_id);
        REQUIRE(store->has_logged_in_user(user_id));
        auto data = store->get_user(user_id);
        REQUIRE(data);
        REQUIRE(data->access_token.token == access_token);
        REQUIRE(data->refresh_token.token == refresh_token);
        REQUIRE(data->device_id == device_id);
    }

    SECTION("passing malformed tokens create_user() results in a logged out user") {
        store->create_user(user_id, refresh_token, "not a token", device_id);
        auto data = store->get_user(user_id);
        REQUIRE(data);
        REQUIRE(data->access_token.token == "");
        REQUIRE(data->refresh_token.token == "");
        REQUIRE(data->device_id == device_id);
    }

    SECTION("create_user() marks the new user as the current user if it was created") {
        CHECK(store->get_current_user() == "");
        store->create_user(user_id, refresh_token, access_token, device_id);
        CHECK(store->get_current_user() == user_id);
        store->create_user("user 2", refresh_token, access_token, device_id);
        CHECK(store->get_current_user() == "user 2");
        store->create_user(user_id, refresh_token, access_token, device_id);
        CHECK(store->get_current_user() == "user 2");
    }

    SECTION("create_user() only updates the given fields and leaves the rest unchanged") {
        store->create_user(user_id, refresh_token, access_token, device_id);
        auto data = store->get_user(user_id);
        REQUIRE(data);
        data->profile = bson::BsonDocument{{"name", "user's name"}, {"email", "user's email"}};
        data->identities = {{"identity", "provider"}};
        store->update_user(user_id, *data);

        const auto access_token_2 = encode_fake_jwt("access_token_2", 123, 456);
        const auto refresh_token_2 = encode_fake_jwt("refresh_token_2", 123, 456);
        store->create_user(user_id, refresh_token_2, access_token_2, "device id 2");

        auto data2 = store->get_user(user_id);
        REQUIRE(data2);
        CHECK(data2->access_token.token == access_token_2);
        CHECK(data2->refresh_token.token == refresh_token_2);
        CHECK(data2->legacy_identities.empty());
        CHECK(data2->device_id == "device id 2");
        CHECK(data2->identities == data->identities);
        CHECK(data2->profile.data() == data->profile.data());
    }

    SECTION("has_logged_in_user() is only true if user is present and valid") {
        CHECK_FALSE(store->has_logged_in_user(""));
        CHECK_FALSE(store->has_logged_in_user(user_id));

        store->create_user(user_id, refresh_token, "malformed token", device_id);
        CHECK_FALSE(store->has_logged_in_user(user_id));
        store->create_user(user_id, refresh_token, "", device_id);
        CHECK_FALSE(store->has_logged_in_user(user_id));
        store->create_user(user_id, "malformed token", access_token, device_id);
        CHECK_FALSE(store->has_logged_in_user(user_id));
        store->create_user(user_id, "", access_token, device_id);
        CHECK_FALSE(store->has_logged_in_user(user_id));

        store->create_user(user_id, refresh_token, access_token, device_id);
        store->log_out(user_id, SyncUser::State::LoggedOut);
        CHECK_FALSE(store->has_logged_in_user(user_id));

        store->create_user(user_id, refresh_token, access_token, device_id);
        store->log_out(user_id, SyncUser::State::Removed);
        CHECK_FALSE(store->has_logged_in_user(user_id));

        store->create_user(user_id, refresh_token, access_token, device_id);
        CHECK(store->has_logged_in_user(user_id));
        CHECK_FALSE(store->has_logged_in_user(""));
        CHECK_FALSE(store->has_logged_in_user("different user"));
    }

    SECTION("get_all_users() returns all non-removed users") {
        store->create_user("user 1", refresh_token, access_token, device_id);
        store->create_user("user 2", refresh_token, access_token, device_id);
        store->create_user("user 3", refresh_token, access_token, device_id);
        store->create_user("user 4", refresh_token, access_token, device_id);

        CHECK(store->get_all_users() == Strings{"user 1", "user 2", "user 3", "user 4"});

        store->log_out("user 2", SyncUser::State::LoggedOut);
        store->delete_user(file_manager, "user 4");

        CHECK(store->get_all_users() == Strings{"user 1", "user 2", "user 3"});
        CHECK(store->has_logged_in_user("user 1"));
        CHECK(!store->has_logged_in_user("user 2"));
        CHECK(store->has_logged_in_user("user 3"));
        CHECK(!store->has_logged_in_user("user 4"));

        store->create_user("user 1", "", access_token, device_id);
        CHECK(store->get_all_users() == Strings{"user 1", "user 2", "user 3"});
        CHECK(!store->has_logged_in_user("user 1"));
        CHECK(!store->has_logged_in_user("user 2"));
        CHECK(store->has_logged_in_user("user 3"));
        CHECK(!store->has_logged_in_user("user 4"));

        store->create_user("user 3", refresh_token, "", device_id);
        CHECK(store->get_all_users() == Strings{"user 1", "user 2", "user 3"});
        CHECK(!store->has_logged_in_user("user 1"));
        CHECK(!store->has_logged_in_user("user 2"));
        CHECK(!store->has_logged_in_user("user 3"));
        CHECK(!store->has_logged_in_user("user 4"));

        store->delete_user(file_manager, "user 1");
        store->delete_user(file_manager, "user 2");
        store->delete_user(file_manager, "user 3");
        store->delete_user(file_manager, "user 4");
        CHECK(store->get_all_users().empty());
        CHECK(!store->has_logged_in_user("user 1"));
        CHECK(!store->has_logged_in_user("user 2"));
        CHECK(!store->has_logged_in_user("user 3"));
        CHECK(!store->has_logged_in_user("user 4"));
    }

    SECTION("set_current_user() sets to the requested user") {
        CHECK(store->get_current_user() == "");
        store->create_user("user 1", refresh_token, access_token, device_id);
        CHECK(store->get_current_user() == "user 1");
        store->create_user("user 2", refresh_token, access_token, device_id);
        CHECK(store->get_current_user() == "user 2");

        store->set_current_user("");
        CHECK(store->get_current_user() == "user 1");
        store->set_current_user("user 2");
        CHECK(store->get_current_user() == "user 2");
        store->set_current_user("user 1");
        CHECK(store->get_current_user() == "user 1");
    }

    SECTION("current user falls back to the first valid one if current is invalid") {
        store->create_user("user 1", refresh_token, access_token, device_id);
        store->create_user("user 2", refresh_token, access_token, device_id);
        store->create_user("user 3", refresh_token, access_token, device_id);

        auto data = store->get_user("user 3");
        data->access_token.token.clear();
        data->refresh_token.token.clear();
        store->update_user("user 3", *data);
        CHECK(store->get_current_user() == "user 1");
        store->update_user("user 1", *data);
        CHECK(store->get_current_user() == "user 2");

        store->set_current_user("not a user");
        CHECK(store->get_current_user() == "user 2");
        store->set_current_user("");
        CHECK(store->get_current_user() == "user 2");
    }

    SECTION("log_out() updates the user state without deleting anything") {
        store->create_user(user_id, refresh_token, access_token, device_id);
        auto path = File::resolve("file 1", base_path);
        File(path, File::mode_Write);
        CHECK(File::exists(path));
        store->add_realm_path(user_id, path);
        store->add_realm_path(user_id, "invalid path");
        store->log_out(user_id, SyncUser::State::Removed);
        CHECK(File::exists(path));
    }

    SECTION("delete_user() deletes the files recorded with add_realm_file_path()") {
        store->create_user(user_id, refresh_token, access_token, device_id);
        auto path = File::resolve("file 1", base_path);
        File(path, File::mode_Write);
        CHECK(File::exists(path));
        store->add_realm_path(user_id, path);
        store->add_realm_path(user_id, "invalid path");
        store->delete_user(file_manager, user_id);
        CHECK_FALSE(File::exists(path));
    }

    SECTION("update_user() does not set legacy identities") {
        store->create_user(user_id, refresh_token, access_token, device_id);
        auto data = store->get_user(user_id);
        data->legacy_identities.push_back("legacy uuid");
        store->update_user(user_id, *data);
        data = store->get_user(user_id);
        REQUIRE(data->legacy_identities.empty());
    }

    SECTION("immediately run nonexistent action") {
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, "invalid"));
    }

    SECTION("immediately run DeleteRealm action") {
        auto path = util::make_temp_file("delete-realm-action");
        store->create_file_action(SyncFileAction::DeleteRealm, path, {});
        CHECK(File::exists(path));
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(File::exists(path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
    }

    SECTION("immediately run BackUpThenDeleteRealm action") {
        auto path = util::make_temp_file("delete-realm-action");
        auto backup_path = util::make_temp_file("backup-path");
        File::remove(backup_path);
        store->create_file_action(SyncFileAction::BackUpThenDeleteRealm, path, backup_path);
        CHECK(File::exists(path));
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(File::exists(path));
        CHECK(File::exists(backup_path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
    }

    SECTION("file actions replace existing ones for the same path") {
        auto path = util::make_temp_file("delete-realm-action");
        auto backup_path = util::make_temp_file("backup-path");
        store->create_file_action(SyncFileAction::BackUpThenDeleteRealm, path, backup_path);
        store->create_file_action(SyncFileAction::DeleteRealm, path, {});
        CHECK(File::exists(path));
        // Would return false if it tried to perform a backup
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(File::exists(path));
    }

    SECTION("failed backup action is preserved") {
        auto path = util::make_temp_file("delete-realm-action");
        auto backup_path = util::make_temp_file("backup-path");
        store->create_file_action(SyncFileAction::BackUpThenDeleteRealm, path, backup_path);
        CHECK(File::exists(path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
        File::remove(backup_path);
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(File::exists(path));
        CHECK(File::exists(backup_path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
    }

#if REALM_PLATFORM_APPLE
    SECTION("failed delete after backup succeeds turns into a delete action") {
        auto path = util::make_temp_file("delete-realm-action");
        auto backup_path = util::make_temp_file("backup-path");
        File::remove(backup_path);
        store->create_file_action(SyncFileAction::BackUpThenDeleteRealm, path, backup_path);
        CHECK(File::exists(path));

        REQUIRE(chflags(path.c_str(), UF_IMMUTABLE) == 0);
        // Returns false because it did something, but did not complete
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
        CHECK(File::exists(path));
        CHECK(File::exists(backup_path));

        // Should try again to remove the original file, but not perform another backup
        REQUIRE(chflags(path.c_str(), 0) == 0);
        REQUIRE(chflags(backup_path.c_str(), 0) == 0);
        File::remove(backup_path);
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(File::exists(path));
        CHECK_FALSE(File::exists(backup_path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
    }
#endif

    SECTION("file action on deleted file is considered successful") {
        auto path = util::make_temp_file("delete-realm-action");
        File::remove(path);

        store->create_file_action(SyncFileAction::BackUpThenDeleteRealm, path, path);
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));

        store->create_file_action(SyncFileAction::DeleteRealm, path, {});
        CHECK(store->immediately_run_file_actions(file_manager, path));
        CHECK_FALSE(store->immediately_run_file_actions(file_manager, path));
    }
}

TEST_CASE("app metadata: in memory", "[sync][metadata]") {
    test_util::TestDirGuard test_dir(base_path);
    AppConfig config;
    config.app_id = app_id;
    config.metadata_mode = AppConfig::MetadataMode::InMemory;
    config.base_file_path = base_path;
    SyncFileManager file_manager(config);

    SECTION("does not persist users between instances") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK_FALSE(store->has_logged_in_user(user_id));
        }
    }
}

TEST_CASE("app metadata: persisted", "[sync][metadata]") {
    test_util::TestDirGuard test_dir(base_path);

    AppConfig config;
    config.app_id = app_id;
    config.metadata_mode = AppConfig::MetadataMode::NoEncryption;
    config.base_file_path = base_path;
    SyncFileManager file_manager(config);

    SECTION("persists users between instances") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK(store->has_logged_in_user(user_id));
            store->log_out(user_id, SyncUser::State::LoggedOut);
        }
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK_FALSE(store->has_logged_in_user(user_id));
            CHECK(store->get_all_users() == Strings{user_id});
        }
    }

    SECTION("can read legacy identities if present") {
        auto store = create_metadata_store(config, file_manager);
        store->create_user(user_id, refresh_token, access_token, device_id);

        auto data = store->get_user(user_id);
        CHECK(data->legacy_identities.empty());

        {
            // Add some legacy uuids by modifying the underlying realm directly
            auto realm = get_metadata_realm();
            auto table = realm->read_group().get_table("class_UserMetadata");
            REQUIRE(table);
            REQUIRE(table->size() == 1);
            auto list = table->begin()->get_list<String>("legacy_uuids");
            realm->begin_transaction();
            list.add("uuid 1");
            list.add("uuid 2");
            realm->commit_transaction();
        }

        data = store->get_user(user_id);
        CHECK(data->legacy_identities == std::vector<std::string>{"uuid 1", "uuid 2"});
    }

    SECTION("runs file actions on creation") {
        auto path = util::make_temp_file("file_to_delete");
        auto nonexistent = util::make_temp_file("nonexistent");
        File::remove(nonexistent);

        {
            auto store = create_metadata_store(config, file_manager);
            store->create_file_action(SyncFileAction::DeleteRealm, path, "");
            store->create_file_action(SyncFileAction::DeleteRealm, nonexistent, "");
        }

        create_metadata_store(config, file_manager);
        REQUIRE_FALSE(File::exists(path));
        REQUIRE_FALSE(File::exists(nonexistent));

        // Check the underlying realm to verify both file actions are gone
        auto realm = get_metadata_realm();
        CHECK(realm->read_group().get_table("class_FileActionMetadata")->is_empty());
    }

    SECTION("deletes data for removed users on creation") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
            store->log_out(user_id, SyncUser::State::Removed);
        }
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK(store->get_all_users().empty());
        }
        // Check the underlying realm as removed users aren't exposed in the API
        auto realm = get_metadata_realm();
        CHECK(realm->read_group().get_table("class_UserMetadata")->is_empty());
    }

    SECTION("deletes realm files for removed users on creation") {
        auto path = util::make_temp_file("file_to_delete");
        auto nonexistent = util::make_temp_file("nonexistent");
        REQUIRE(File::exists(path));
        File::remove(nonexistent);

        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
            store->add_realm_path(user_id, nonexistent);
            store->add_realm_path(user_id, path);
            store->log_out(user_id, SyncUser::State::Removed);
        }

        create_metadata_store(config, file_manager);
        REQUIRE_FALSE(File::exists(path));
        REQUIRE_FALSE(File::exists(nonexistent));
    }

#if REALM_PLATFORM_APPLE
    SECTION("continues tracking files to delete if deletion fails") {
        auto path = util::make_temp_file("file_to_delete");
        REQUIRE(File::exists(path));

        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
            store->add_realm_path(user_id, path);
            store->log_out(user_id, SyncUser::State::Removed);
        }

        REQUIRE(chflags(path.c_str(), UF_IMMUTABLE) == 0);
        create_metadata_store(config, file_manager);
        REQUIRE(File::exists(path));
        REQUIRE(chflags(path.c_str(), 0) == 0);
        create_metadata_store(config, file_manager);
        REQUIRE_FALSE(File::exists(path));
    }
#endif

    SECTION("stops tracking files if it no longer exists") {
        auto path = util::make_temp_file("nonexistent");
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
            store->add_realm_path(user_id, path);
            store->log_out(user_id, SyncUser::State::Removed);
        }

        File::remove(path);
        create_metadata_store(config, file_manager);
        auto realm = get_metadata_realm();
        CHECK(realm->read_group().get_table("class_UserMetadata")->is_empty());
    }

    SECTION("deletes legacy untracked files") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
            store->log_out(user_id, SyncUser::State::Removed);
        }

        // Create some files in the user's directory without tracking them
        auto path_1 = file_manager.realm_file_path(user_id, {}, "file 1", "partition 1");
        auto path_2 = file_manager.realm_file_path(user_id, {}, "file 2", "partition 2");
        File{path_1, File::mode_Write};
        File{path_2, File::mode_Write};

        // Files should be deleted on next start since the user has been removed
        create_metadata_store(config, file_manager);
        CHECK_FALSE(File::exists(path_1));
        CHECK_FALSE(File::exists(path_2));
    }
}

#if REALM_ENABLE_ENCRYPTION
TEST_CASE("app metadata: encryption", "[sync][metadata]") {
    test_util::TestDirGuard test_dir(base_path);

    AppConfig config;
    config.app_id = app_id;
    config.metadata_mode = AppConfig::MetadataMode::Encryption;
    config.custom_encryption_key = make_test_encryption_key(10);
    config.base_file_path = base_path;
    SyncFileManager file_manager(config);

    // Verify that the Realm is actually encrypted with the expected key
    auto open_realm_with_key = [](auto& key) {
        RealmConfig realm_config;
        realm_config.automatic_change_notifications = false;
        realm_config.path = metadata_path;
        // sanity check that using the wrong key throws, as otherwise we'd pass
        // if we were checking the wrong path
        realm_config.encryption_key = make_test_encryption_key(0);
        CHECK_THROWS(Realm::get_shared_realm(realm_config));

        if (key) {
            realm_config.encryption_key = *key;
        }
        else {
            realm_config.encryption_key.clear();
        }
        CHECK_NOTHROW(Realm::get_shared_realm(realm_config));
    };

    SECTION("can open and reopen with an explicit key") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK(store->has_logged_in_user(user_id));
        }
        open_realm_with_key(config.custom_encryption_key);
    }

    SECTION("reopening with a different key deletes the existing data") {
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        open_realm_with_key(config.custom_encryption_key);

        // Change to new encryption key
        {
            config.custom_encryption_key = make_test_encryption_key(11);
            auto store = create_metadata_store(config, file_manager);
            CHECK_FALSE(store->has_logged_in_user(user_id));
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        open_realm_with_key(config.custom_encryption_key);

        // Change to unencrypted
        {
            config.metadata_mode = AppConfig::MetadataMode::NoEncryption;
            config.custom_encryption_key.reset();
            auto store = create_metadata_store(config, file_manager);
            CHECK_FALSE(store->has_logged_in_user(user_id));
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        open_realm_with_key(config.custom_encryption_key);

        // Change back to encrypted
        {
            config.metadata_mode = AppConfig::MetadataMode::Encryption;
            config.custom_encryption_key = make_test_encryption_key(12);
            auto store = create_metadata_store(config, file_manager);
            CHECK_FALSE(store->has_logged_in_user(user_id));
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        open_realm_with_key(config.custom_encryption_key);
    }

#if REALM_PLATFORM_APPLE
    if (!can_access_keychain()) {
        return;
    }
    auto delete_key = util::make_scope_exit([&]() noexcept {
        keychain::delete_metadata_realm_encryption_key(config.app_id, config.security_access_group);
    });

    SECTION("encryption key is automatically generated and stored for new files") {
        config.custom_encryption_key.reset();
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }
        auto key = keychain::get_existing_metadata_realm_key(config.app_id, config.security_access_group);
        REQUIRE(key);
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK(store->has_logged_in_user(user_id));
        }
        open_realm_with_key(key);
    }

    SECTION("existing unencrypted files are left unencrypted") {
        config.custom_encryption_key.reset();
        config.metadata_mode = AppConfig::MetadataMode::NoEncryption;
        {
            auto store = create_metadata_store(config, file_manager);
            store->create_user(user_id, refresh_token, access_token, device_id);
        }

        config.metadata_mode = AppConfig::MetadataMode::Encryption;
        {
            auto store = create_metadata_store(config, file_manager);
            CHECK(store->has_logged_in_user(user_id));
        }
        open_realm_with_key(config.custom_encryption_key);
    }
#else  // REALM_PLATFORM_APPLE
    SECTION("requires an explicit encryption key") {
        config.custom_encryption_key.reset();
        REQUIRE_EXCEPTION(create_metadata_store(config, file_manager), InvalidArgument,
                          "Metadata Realm encryption was specified, but no encryption key was provided.");
    }
#endif // REALM_PLATFORM_APPLE
}

#endif

#ifndef SWIFT_PACKAGE // The SPM build currently doesn't copy resource files
TEST_CASE("sync metadata: can open old metadata realms", "[sync][metadata]") {
    test_util::TestDirGuard test_dir(base_path);

    util::make_dir_recursive(File::parent_dir(metadata_path));

    const std::string provider_type = "https://realm.example.org";
    const auto identity = "metadata migration test";
    const std::string sample_token = encode_fake_jwt("metadata migration token", 456, 123);

    const auto access_token_1 = encode_fake_jwt("access token 1", 456, 123);
    const auto access_token_2 = encode_fake_jwt("access token 2", 456, 124);
    const auto refresh_token_1 = encode_fake_jwt("refresh token 1", 456, 123);
    const auto refresh_token_2 = encode_fake_jwt("refresh token 2", 456, 124);

    AppConfig config;
    config.app_id = app_id;
    config.base_file_path = base_path;
    config.metadata_mode = AppConfig::MetadataMode::NoEncryption;
    SyncFileManager file_manager(config);


    // change to true to create a test file for the current schema version
    // this will only work on unix-like systems
    if ((false)) {
#if false   // The code to generate the v4 and v5 Realms
        { // Create a metadata Realm with a test user
            SyncMetadataManager manager(metadata_path, false);
            auto user_metadata = manager.get_or_make_user_metadata(identity, provider_type);
            user_metadata->set_access_token(sample_token);
        }
#elif false // The code to generate the v6 Realm
        // Code to generate the v6 metadata Realm used to test the 6 -> 7 migration
        {
            using State = SyncUser::State;
            SyncMetadataManager manager(metadata_path, false);

            auto user = manager.get_or_make_user_metadata("removed user", "");
            user->set_state(State::Removed);

            auto make_user_pair = [&](const char* name, State state1, State state2, const std::string& token_1,
                                      const std::string& token_2) {
                auto user = manager.get_or_make_user_metadata(name, "a");
                user->set_state_and_tokens(state1, token_1, refresh_token_1);
                user->set_identities({{"identity 1", "a"}, {"shared identity", "shared"}});
                user->add_realm_file_path("file 1");
                user->add_realm_file_path("file 2");

                user = manager.get_or_make_user_metadata(name, "b");
                user->set_state_and_tokens(state2, token_2, refresh_token_2);
                user->set_identities({{"identity 2", "b"}, {"shared identity", "shared"}});
                user->add_realm_file_path("file 2");
                user->add_realm_file_path("file 3");
            };

            make_user_pair("first logged in, second logged out", State::LoggedIn, State::LoggedOut, access_token_1,
                           access_token_2);
            make_user_pair("first logged in, second removed", State::LoggedIn, State::Removed, access_token_1,
                           access_token_2);
            make_user_pair("second logged in, first logged out", State::LoggedOut, State::LoggedIn, access_token_1,
                           access_token_2);
            make_user_pair("second logged in, first removed", State::Removed, State::LoggedIn, access_token_1,
                           access_token_2);
            make_user_pair("both logged in, first newer", State::LoggedIn, State::LoggedIn, access_token_2,
                           access_token_1);
            make_user_pair("both logged in, second newer", State::LoggedIn, State::LoggedIn, access_token_1,
                           access_token_2);
        }

        // Replace the randomly generated UUIDs with deterministic values
        {
            Realm::Config config;
            config.path = metadata_path;
            auto realm = Realm::get_shared_realm(config);
            realm->begin_transaction();
            auto& group = realm->read_group();
            auto table = group.get_table("class_UserMetadata");
            auto col = table->get_column_key("local_uuid");
            size_t i = 0;
            for (auto& obj : *table) {
                obj.set(col, util::to_string(i++));
            }
            realm->commit_transaction();
        }
#else
        { // Create a metadata Realm with a test user
            auto store = create_metadata_store(config, file_manager);
            store->create_user(identity, sample_token, sample_token, "device id");
        }
#endif

        // Open the metadata Realm directly and grab the schema version from it
        auto realm = get_metadata_realm();
        realm->read_group();
        auto schema_version = realm->schema_version();

        // Take the path of this file, remove everything after the "test" directory,
        // then append the output filename
        std::string out_path = __FILE__;
        auto suffix = out_path.find("sync/metadata.cpp");
        REQUIRE(suffix != out_path.npos);
        out_path.resize(suffix);
        out_path.append(util::format("sync-metadata-v%1.realm", schema_version));

        // Write a compacted copy of the metadata realm to the test directory
        Realm::Config out_config;
        out_config.path = out_path;
        realm->convert(out_config);

        std::cout << "Wrote metadata realm to: " << out_path << "\n";
        return;
    }

    SECTION("open schema version 4") {
        File::copy(test_util::get_test_resource_path() + "sync-metadata-v4.realm", metadata_path);
        auto store = create_metadata_store(config, file_manager);
        auto user_metadata = store->get_user(identity);
        REQUIRE(user_metadata->access_token.token == sample_token);
    }

    SECTION("open schema version 5") {
        File::copy(test_util::get_test_resource_path() + "sync-metadata-v5.realm", metadata_path);
        auto store = create_metadata_store(config, file_manager);
        auto user_metadata = store->get_user(identity);
        REQUIRE(user_metadata->access_token.token == sample_token);
    }

    SECTION("open schema version 6") {
        File::copy(test_util::get_test_resource_path() + "sync-metadata-v6.realm", metadata_path);
        auto store = create_metadata_store(config, file_manager);

        UserIdentity id_1{"identity 1", "a"};
        UserIdentity id_2{"identity 2", "b"};
        UserIdentity id_shared{"shared identity", "shared"};
        const std::vector<UserIdentity> all_ids = {id_1, id_shared, id_2};
        const std::vector<std::string> realm_files = {"file 1", "file 2", "file 3"};

        auto check_user = [&](const char* user_id, const std::string& access_token, const std::string& refresh_token,
                              const std::vector<std::string>& uuids) {
            auto user = store->get_user(user_id);
            CAPTURE(user_id);
            CHECK(user->access_token.token == access_token);
            CHECK(user->refresh_token.token == refresh_token);
            CHECK(user->legacy_identities == uuids);
            CHECK(user->identities == all_ids);
        };

        REQUIRE_FALSE(store->has_logged_in_user("removed user"));
        check_user("first logged in, second logged out", access_token_1, refresh_token_1, {"1", "2"});
        check_user("first logged in, second removed", access_token_1, refresh_token_1, {"3", "4"});
        check_user("second logged in, first logged out", access_token_2, refresh_token_2, {"5", "6"});
        check_user("second logged in, first removed", access_token_2, refresh_token_2, {"7", "8"});
        check_user("both logged in, first newer", access_token_2, refresh_token_1, {"9", "10"});
        check_user("both logged in, second newer", access_token_2, refresh_token_2, {"11", "12"});
    }
}
#endif // SWIFT_PACKAGE

#if REALM_PLATFORM_APPLE && REALM_ENABLE_ENCRYPTION
TEST_CASE("keychain", "[sync][metadata]") {
    if (!can_access_keychain()) {
        return;
    }
    auto delete_key = util::make_scope_exit([=]() noexcept {
        keychain::delete_metadata_realm_encryption_key(app_id, access_group);
        keychain::delete_metadata_realm_encryption_key("app id 1", access_group);
        keychain::delete_metadata_realm_encryption_key("app id 2", access_group);
    });

    SECTION("create_new_metadata_realm_key() creates a new key if none exists") {
        auto key_1 = keychain::create_new_metadata_realm_key(app_id, access_group);
        REQUIRE(key_1);
        keychain::delete_metadata_realm_encryption_key(app_id, access_group);
        auto key_2 = keychain::create_new_metadata_realm_key(app_id, access_group);
        REQUIRE(key_2);
        REQUIRE(key_1 != key_2);
    }

    SECTION("create_new_metadata_realm_key() returns the existing one if inserting fails") {
        auto key_1 = keychain::create_new_metadata_realm_key(app_id, access_group);
        REQUIRE(key_1);
        auto key_2 = keychain::create_new_metadata_realm_key(app_id, access_group);
        REQUIRE(key_2);
        REQUIRE(key_1 == key_2);
    }

    SECTION("get_existing_metadata_realm_key() returns the key from create_new_metadata_realm_key()") {
        auto key_1 = keychain::get_existing_metadata_realm_key(app_id, access_group);
        REQUIRE_FALSE(key_1);
        auto key_2 = keychain::create_new_metadata_realm_key(app_id, access_group);
        REQUIRE(key_2);
        auto key_3 = keychain::get_existing_metadata_realm_key(app_id, access_group);
        REQUIRE(key_3);
        REQUIRE(key_2 == key_3);
    }

    SECTION("keys are scoped to app ids") {
        auto key_1 = keychain::create_new_metadata_realm_key("app id 1", access_group);
        REQUIRE(key_1);
        auto key_2 = keychain::create_new_metadata_realm_key("app id 2", access_group);
        REQUIRE(key_2);
        REQUIRE(key_1 != key_2);
    }

    SECTION("legacy key migration") {
        auto key = generate_key();
        const auto legacy_account = CFSTR("metadata");
        const auto service_name = CFSTR("io.realm.sync.keychain");
        const auto bundle_id = CFBundleGetIdentifier(CFBundleGetMainBundle());
        // Could be either ObjectStoreTests or CombinedTests but must be set
        REQUIRE(bundle_id);
        const auto bundle_service =
            adoptCF(CFStringCreateWithFormat(nullptr, nullptr, CFSTR("%@ - Realm Sync Metadata Key"), bundle_id));

        enum class Location { Original, Bundle, BundleAndAppId };
        auto location = GENERATE(Location::Original, Location::Bundle, Location::BundleAndAppId);
        CAPTURE(location);
        CFStringRef account, service;
        switch (location) {
            case Location::Original:
                account = legacy_account;
                service = service_name;
                break;
            case Location::Bundle:
                account = legacy_account;
                service = bundle_service.get();
                break;
            case Location::BundleAndAppId:
                account = CFSTR("app id");
                service = bundle_service.get();
                break;
        }

        set_key(key, account, service);
        auto key_2 = keychain::get_existing_metadata_realm_key(app_id, {});
        REQUIRE(key_2 == key);

        // Key should have been copied to the preferred location
        REQUIRE(get_key(CFSTR("app id"), bundle_service.get(), key) == errSecSuccess);
        REQUIRE(key_2 == key);

        // Key should not have been deleted from the original location
        REQUIRE(get_key(account, service, key) == errSecSuccess);
        REQUIRE(key_2 == key);
    }
}
#endif
