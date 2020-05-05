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

#include "sync_test_utils.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include "sync/session/session_util.hpp"
#include "util/event_loop.hpp"
#include "util/test_utils.hpp"
#include <realm/util/logger.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

static const std::string base_path = tmp_dir() + "realm_objectstore_sync_manager/";

namespace {

bool validate_user_in_vector(std::vector<std::shared_ptr<SyncUser>> vector,
                             const std::string& identity,
                             const std::string& provider_type,
                             const std::string& refresh_token,
                             const std::string& access_token) {
    for (auto& user : vector) {
        if (user->identity() == identity
            && user->refresh_token() == refresh_token
            && provider_type == user->provider_type()
            && user->access_token() == access_token) {
           return true;
        }
    }
    return false;
}

}

TEST_CASE("sync_manager: basic properties and APIs", "[sync]") {
    TestSyncManager init_sync_manager("");

    SECTION("should work for log level") {
        SyncManager::shared().set_log_level(util::Logger::Level::info);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::info);
        SyncManager::shared().set_log_level(util::Logger::Level::error);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::error);
    }

    SECTION("should not crash on 'reconnect()'") {
        SyncManager::shared().reconnect();
    }
}

TEST_CASE("sync_manager: `path_for_realm` API", "[sync]") {
    reset_test_directory(base_path);
    const std::string auth_server_url = "https://realm.example.org";
    const std::string raw_url = "realms://realm.example.org/a/b/~/123456/xyz";

    SECTION("should work properly without metadata") {
        TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoMetadata);
        // Get a sync user
        const std::string identity = "foobarbaz";
        auto user = SyncManager::shared().get_user(identity, ENCODE_FAKE_JWT("dummy_token"), ENCODE_FAKE_JWT("not_a_real_token"), auth_server_url);
        const auto expected = base_path + "realm-object-server/foobarbaz/realms%3A%2F%2Frealm.example.org%2Fa%2Fb%2F%7E%2F123456%2Fxyz";
        REQUIRE(SyncManager::shared().path_for_realm(*user, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(base_path + "realm-object-server/foobarbaz/");
    }

    SECTION("should work properly with metadata") {
        TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoEncryption);
        const std::string identity = "foobarbaz";
        auto user = SyncManager::shared().get_user(identity, ENCODE_FAKE_JWT("dummy_token"), ENCODE_FAKE_JWT("not_a_real_token"), auth_server_url);
        auto local_identity = user->local_identity();
        const auto expected = base_path + "realm-object-server/" + local_identity + "/realms%3A%2F%2Frealm.example.org%2Fa%2Fb%2F%7E%2F123456%2Fxyz";
        REQUIRE(SyncManager::shared().path_for_realm(*user, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(base_path + "realm-object-server/" + local_identity + "/");
    }
}

TEST_CASE("sync_manager: user state management", "[sync]") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoEncryption);

    const std::string url_1 = "https://realm.example.org/1/";
    const std::string url_2 = "https://realm.example.org/2/";
    const std::string url_3 = "https://realm.example.org/3/";

    const std::string r_token_1 = ENCODE_FAKE_JWT("foo_token");
    const std::string r_token_2 = ENCODE_FAKE_JWT("bar_token");
    const std::string r_token_3 = ENCODE_FAKE_JWT("baz_token");

    const std::string a_token_1 = ENCODE_FAKE_JWT("wibble");
    const std::string a_token_2 = ENCODE_FAKE_JWT("wobble");
    const std::string a_token_3 = ENCODE_FAKE_JWT("wubble");

    const std::string identity_1 = "user-foo";
    const std::string identity_2 = "user-bar";
    const std::string identity_3 = "user-baz";

    SECTION("should get all users that are created during run time") {
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1);
        SyncManager::shared().get_user(identity_2, r_token_2, a_token_2, url_2);
        auto users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2));
    }

    SECTION("should be able to distinguish users based solely on URL") {
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1);
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_2);
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_3);
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1); // existing
        auto users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_1, url_2, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_1, url_2, r_token_1, a_token_1));
    }

    SECTION("should be able to distinguish users based solely on user ID") {
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1);
        SyncManager::shared().get_user(identity_2, r_token_1, a_token_1, url_1);
        SyncManager::shared().get_user(identity_3, r_token_1, a_token_1, url_1);
        SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1); // existing
        auto users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_1, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_3, url_1, r_token_1, a_token_1));
    }

    SECTION("should properly update state in response to users logging in and out") {
        auto r_token_3a = ENCODE_FAKE_JWT("qwerty");
        auto a_token_3a = ENCODE_FAKE_JWT("ytrewq");

        auto u1 = SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1);
        auto u2 = SyncManager::shared().get_user(identity_2, r_token_2, a_token_2, url_2);
        auto u3 = SyncManager::shared().get_user(identity_3, r_token_3, a_token_3, url_3);
        auto users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3, a_token_3));
        // Log out users 1 and 3
        u1->log_out();
        u3->log_out();
        users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2));
        // Log user 3 back in
        u3 = SyncManager::shared().get_user(identity_3, r_token_3a, a_token_3a, url_3);
        users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3a, a_token_3a));
        // Log user 2 out
        u2->log_out();
        users = SyncManager::shared().all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3a, a_token_3a));
    }

    SECTION("should return current user that was created during run time") {
        auto u_null = SyncManager::shared().get_current_user();
        REQUIRE(u_null == nullptr);

        auto u1 = SyncManager::shared().get_user(identity_1, r_token_1, a_token_1, url_1);
        auto u_current = SyncManager::shared().get_current_user();
        REQUIRE(u_current == u1);

        auto u2 = SyncManager::shared().get_user(identity_2, r_token_2, a_token_2, url_2);
        // The current user has switched to return the most recently used: "u2"
        u_current = SyncManager::shared().get_current_user();
        REQUIRE(u_current == u2);
    }
}

TEST_CASE("sync_manager: persistent user state management", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string url_1 = "https://realm.example.org/1/";
    const std::string url_2 = "https://realm.example.org/2/";
    const std::string url_3 = "https://realm.example.org/3/";
    const std::string r_token_1 = ENCODE_FAKE_JWT("foo_token");
    const std::string r_token_2 = ENCODE_FAKE_JWT("bar_token");
    const std::string r_token_3 = ENCODE_FAKE_JWT("baz_token");
    const std::string a_token_1 = ENCODE_FAKE_JWT("wibble");
    const std::string a_token_2 = ENCODE_FAKE_JWT("wobble");
    const std::string a_token_3 = ENCODE_FAKE_JWT("wubble");

    SECTION("when users are persisted") {
        const std::string identity_1 = "foo-1";
        const std::string identity_2 = "bar-1";
        const std::string identity_3 = "baz-1";
        // First, create a few users and add them to the metadata.
        auto u1 = manager.get_or_make_user_metadata(identity_1, url_1);
        u1->set_access_token(a_token_1);
        u1->set_refresh_token(r_token_1);
        auto u2 = manager.get_or_make_user_metadata(identity_2, url_2);
        u2->set_access_token(a_token_2);
        u2->set_refresh_token(r_token_2);
        auto u3 = manager.get_or_make_user_metadata(identity_3, url_3);
        u3->set_access_token(a_token_3);
        u3->set_refresh_token(r_token_3);
        // The fourth user is an "invalid" user: no token, so shouldn't show up.
        auto u_invalid = manager.get_or_make_user_metadata("invalid_user", url_1);
        REQUIRE(manager.all_unmarked_users().size() == 4);

        SECTION("they should be added to the active users list when metadata is enabled") {
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_users();
            REQUIRE(users.size() == 3);
            REQUIRE(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1));
            REQUIRE(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2));
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, r_token_3, a_token_3));
        }
        SECTION("they should not be added to the active users list when metadata is disabled") {
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_users();
            REQUIRE(users.size() == 0);
        }
    }

    SECTION("when users are marked") {
        const std::string auth_url = "https://example.realm.org";
        const std::string identity_1 = "foo-2";
        const std::string identity_2 = "bar-2";
        const std::string identity_3 = "baz-2";

        // Create the user metadata.
        auto u1 = manager.get_or_make_user_metadata(identity_1, auth_url);
        u1->mark_for_removal();
        auto u2 = manager.get_or_make_user_metadata(identity_2, auth_url);
        u2->mark_for_removal();
        // Don't mark this user for deletion.
        auto u3 = manager.get_or_make_user_metadata(identity_3, auth_url);
        u3->set_access_token(a_token_3);
        u3->set_refresh_token(r_token_3);
        // Pre-populate the user directories.
        const auto user_dir_1 = file_manager.user_directory(u1->local_uuid());
        const auto user_dir_2 = file_manager.user_directory(u2->local_uuid());
        const auto user_dir_3 = file_manager.user_directory(u3->local_uuid());
        create_dummy_realm(user_dir_1 + "123456789");
        create_dummy_realm(user_dir_1 + "foo");
        create_dummy_realm(user_dir_2 + "123456789");
        create_dummy_realm(user_dir_3 + "foo");
        create_dummy_realm(user_dir_3 + "bar");
        create_dummy_realm(user_dir_3 + "baz");

        SECTION("they should be cleaned up if metadata is enabled") {
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_users();
            REQUIRE(users.size() == 1);
            REQUIRE(validate_user_in_vector(users, identity_3, auth_url, r_token_3, a_token_3));
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_1);
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
        SECTION("they should be left alone if metadata is disabled") {
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_users();
            REQUIRE_DIR_EXISTS(user_dir_1);
            REQUIRE_DIR_EXISTS(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
    }
}

TEST_CASE("sync_manager: file actions", "[sync]") {
    using Action = SyncFileActionMetadata::Action;
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string realm_url = "https://example.realm.com/~/1";
    const std::string local_uuid_1 = "foo-1";
    const std::string local_uuid_2 = "bar-1";
    const std::string local_uuid_3 = "baz-1";
    const std::string local_uuid_4 = "baz-2";

    // Realm paths
    const std::string realm_path_1 = file_manager.path(local_uuid_1, realm_url);
    const std::string realm_path_2 = file_manager.path(local_uuid_2, realm_url);
    const std::string realm_path_3 = file_manager.path(local_uuid_3, realm_url);
    const std::string realm_path_4 = file_manager.path(local_uuid_4, realm_url);

    SECTION("Action::DeleteRealm") {
        // Create some file actions
        manager.make_file_action_metadata(realm_path_1, realm_url, "user1", Action::DeleteRealm);
        manager.make_file_action_metadata(realm_path_2, realm_url, "user2", Action::DeleteRealm);
        manager.make_file_action_metadata(realm_path_3, realm_url, "user3", Action::DeleteRealm);

        SECTION("should properly delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // All Realms should be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
        }

        SECTION("should fail gracefully if the Realm is missing") {
            // Don't actually create the Realm files
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoMetadata);
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
        }
    }

    SECTION("Action::BackUpThenDeleteRealm") {
        const auto recovery_dir = file_manager.recovery_directory_path();
        // Create some file actions
        const std::string recovery_1 = util::file_path_by_appending_component(recovery_dir, "recovery-1");
        const std::string recovery_2 = util::file_path_by_appending_component(recovery_dir, "recovery-2");
        const std::string recovery_3 = util::file_path_by_appending_component(recovery_dir, "recovery-3");
        manager.make_file_action_metadata(realm_path_1, realm_url, "user1", Action::BackUpThenDeleteRealm, recovery_1);
        manager.make_file_action_metadata(realm_path_2, realm_url, "user2", Action::BackUpThenDeleteRealm, recovery_2);
        manager.make_file_action_metadata(realm_path_3, realm_url, "user3", Action::BackUpThenDeleteRealm, recovery_3);

        SECTION("should properly copy the Realm file and delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // All Realms should be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(recovery_1));
            CHECK(File::exists(recovery_2));
            CHECK(File::exists(recovery_3));
        }

        SECTION("should copy the Realm to the recovery_directory_path") {
            const std::string identity = "b241922032489d4836ecd0c82d0445f0";
            const auto realm_base_path = file_manager.user_directory(identity) + "realmtasks";
            std::string recovery_path = util::reserve_unique_file_name(file_manager.recovery_directory_path(),
                                                                       util::create_timestamped_template("recovered_realm"));
            create_dummy_realm(realm_base_path);
            REQUIRE_REALM_EXISTS(realm_base_path);
            REQUIRE(!File::exists(recovery_path));
            // Manually create a file action metadata entry to simulate a client reset.
            manager.make_file_action_metadata(realm_base_path, realm_url, identity, Action::BackUpThenDeleteRealm, recovery_path);
            auto pending_actions = manager.all_pending_actions();
            REQUIRE(pending_actions.size() == 4);

            // Simulate client launch.
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);

            CHECK(pending_actions.size() == 0);
            CHECK(File::exists(recovery_path));
            REQUIRE_REALM_DOES_NOT_EXIST(realm_base_path);
        }

        SECTION("should fail gracefully if the Realm is missing") {
            // Don't actually create the Realm files
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // There should not be recovery files.
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_1)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_2)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_3)));
        }

        SECTION("should work properly when manually driven") {
            REQUIRE(!File::exists(recovery_1));
            // Create a Realm file
            create_dummy_realm(realm_path_4);
            // Configure the system
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            REQUIRE(manager.all_pending_actions().size() == 0);
            // Add a file action after the system is configured.
            REQUIRE_REALM_EXISTS(realm_path_4);
            REQUIRE(File::exists(file_manager.recovery_directory_path()));
            manager.make_file_action_metadata(realm_path_4, realm_url, "user4", Action::BackUpThenDeleteRealm, recovery_1);
            REQUIRE(manager.all_pending_actions().size() == 1);
            // Force the recovery. (In a real application, the user would have closed the files by now.)
            REQUIRE(SyncManager::shared().immediately_run_file_actions(realm_path_4));
            // There should be recovery files.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_4);
            CHECK(File::exists(recovery_1));
            REQUIRE(manager.all_pending_actions().size() == 0);
        }

        SECTION("should fail gracefully if there is already a file at the destination") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            create_dummy_realm(recovery_1);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoEncryption);
            // Most file actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 1);
            // Realms should be deleted.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(recovery_2));
            CHECK(File::exists(recovery_3));
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager::configure("", base_path, SyncManager::MetadataMode::NoMetadata);
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
            // There should not be recovery files.
            CHECK(!File::exists(recovery_1));
            CHECK(!File::exists(recovery_2));
            CHECK(!File::exists(recovery_3));
        }
    }
}

TEST_CASE("sync_manager: metadata") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);

    SECTION("should be reset in case of decryption error") {
        SyncClientConfig config;
        config.base_file_path = base_path;
        config.metadata_mode = SyncManager::MetadataMode::Encryption;
        config.custom_encryption_key = make_test_encryption_key();
        SyncManager::shared().configure(config);

        SyncManager::shared().reset_for_testing();

        config.custom_encryption_key = make_test_encryption_key(1);
        config.reset_metadata_on_error = true;
        SyncManager::shared().configure(config);
    }
}

TEST_CASE("sync_manager: has_active_sessions", "[active_sessions]") {
    reset_test_directory(base_path);
    SyncServer server(false);
    TestSyncManager init_sync_manager(server, base_path, SyncManager::MetadataMode::NoMetadata);

    SECTION("no active sessions") {
        REQUIRE(!SyncManager::shared().has_existing_sessions());
    }

    auto schema = Schema{
        {"object", {
            {"value", PropertyType::Int},
        }},
    };

    std::atomic<bool> error_handler_invoked(false);
    Realm::Config config;
    auto user = SyncManager::shared().get_user("user-name", ENCODE_FAKE_JWT("not_a_real_token"), ENCODE_FAKE_JWT("samesies"), "https://realm.example.org");
    auto create_session = [&](SyncSessionStopPolicy stop_policy) {
        std::shared_ptr<SyncSession> session = sync_session(user, "/test-dying-state",
                                    [&](auto, auto) { error_handler_invoked = true; },
                                    stop_policy, nullptr, schema, &config);
        EventLoop::main().run_until([&] { return sessions_are_active(*session); });
        return session;
    };

    SECTION("active sessions") {
        {
            auto session = create_session(SyncSessionStopPolicy::Immediately);
            REQUIRE(SyncManager::shared().has_existing_sessions());
            session->close();
        }
        EventLoop::main().run_until([&] { return !SyncManager::shared().has_existing_sessions(); });
        REQUIRE(!SyncManager::shared().has_existing_sessions());
    }
}
