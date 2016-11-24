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

#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"
#include <realm/util/logger.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

static const std::string base_path = tmp_dir() + "/realm_objectstore_sync_manager/";

namespace {

bool validate_user_in_vector(std::vector<std::shared_ptr<SyncUser>> vector,
                             const std::string& identity,
                             util::Optional<std::string> url,
                             const std::string& token) {
    for (auto& user : vector) {
        if (user->identity() == identity && user->refresh_token() == token && url.value_or("") == user->server_url()) {
           return true;
        }
    }
    return false;
}

}

TEST_CASE("sync_manager: basic property APIs", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    SECTION("should work for log level") {
        SyncManager::shared().set_log_level(util::Logger::Level::info);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::info);
        SyncManager::shared().set_log_level(util::Logger::Level::error);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::error);
    }

    SECTION("should work for 'should reconnect immediately'") {
        SyncManager::shared().set_client_should_reconnect_immediately(true);
        REQUIRE(SyncManager::shared().client_should_reconnect_immediately());
        SyncManager::shared().set_client_should_reconnect_immediately(false);
        REQUIRE(!SyncManager::shared().client_should_reconnect_immediately());
    }

    SECTION("should work for 'should validate SSL'") {
        SyncManager::shared().set_client_should_validate_ssl(true);
        REQUIRE(SyncManager::shared().client_should_validate_ssl());
        SyncManager::shared().set_client_should_validate_ssl(false);
        REQUIRE(!SyncManager::shared().client_should_validate_ssl());
    }
}

TEST_CASE("sync_manager: `path_for_realm` API", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    SECTION("should work properly") {
        const std::string identity = "foobarbaz";
        const std::string raw_url = "realms://foo.bar.example.com/realm/something/~/123456/xyz";
        const auto expected = base_path + "realm-object-server/foobarbaz/realms%3A%2F%2Ffoo.bar.example.com%2Frealm%2Fsomething%2F%7E%2F123456%2Fxyz";
        REQUIRE(SyncManager::shared().path_for_realm(identity, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(base_path + "realm-object-server/foobarbaz/");
    }
}

TEST_CASE("sync_manager: user state management", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    const std::string url_1 = "https://example.realm.com/1/";
    const std::string url_2 = "https://example.realm.com/2/";
    const std::string url_3 = "https://example.realm.com/3/";
    const std::string token_1 = "foo_token";
    const std::string token_2 = "bar_token";
    const std::string token_3 = "baz_token";
    const std::string identity_1 = "user-foo";
    const std::string identity_2 = "user-bar";
    const std::string identity_3 = "user-baz";

    SECTION("should get all users that are created during run time") {
        SyncManager::shared().get_user(identity_1, token_1, url_1);
        SyncManager::shared().get_user(identity_2, token_2, url_2);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_1, url_1, token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
    }

    SECTION("should properly update state in response to users logging in and out") {
        auto token_3a = "qwerty";
        auto u1 = SyncManager::shared().get_user(identity_1, token_1, url_1);
        auto u2 = SyncManager::shared().get_user(identity_2, token_2, url_2);
        auto u3 = SyncManager::shared().get_user(identity_3, token_3, url_3);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3));
        // Log out users 1 and 3
        u1->log_out();
        u3->log_out();
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 1);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        // Log user 3 back in
        u3 = SyncManager::shared().get_user(identity_3, token_3a, url_3);
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3a));
        // Log user 2 out
        u2->log_out();
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 1);
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3a));
    }

    SECTION("should contain admin-token users if such users are created.") {
        SyncManager::shared().get_user(identity_2, token_2, url_2);
        SyncManager::shared().get_user(identity_3, token_3, none, true);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, none, token_3));
    }

    SECTION("should return current user that was created during run time") {
        auto u_null = SyncManager::shared().get_current_user();
        REQUIRE(u_null == nullptr);

        auto u1 = SyncManager::shared().get_user(identity_1, token_1, url_1);
        auto u_current = SyncManager::shared().get_current_user();
        REQUIRE(u_current == u1);

        auto u2 = SyncManager::shared().get_user(identity_2, token_2, url_2);
        REQUIRE_THROWS_AS(SyncManager::shared().get_current_user(), std::logic_error);
    }
}

TEST_CASE("sync_manager: persistent user state management", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string url_1 = "https://example.realm.com/1/";
    const std::string url_2 = "https://example.realm.com/2/";
    const std::string url_3 = "https://example.realm.com/3/";
    const std::string token_1 = "foo_token";
    const std::string token_2 = "bar_token";
    const std::string token_3 = "baz_token";

    SECTION("when users are persisted") {
        const std::string identity_1 = "foo-1";
        const std::string identity_2 = "bar-1";
        const std::string identity_3 = "baz-1";
        // First, create a few users and add them to the metadata.
        auto u1 = SyncUserMetadata(manager, identity_1);
        u1.set_state(url_1, token_1);
        auto u2 = SyncUserMetadata(manager, identity_2);
        u2.set_state(url_2, token_2);
        auto u3 = SyncUserMetadata(manager, identity_3);
        u3.set_state(url_3, token_3);
        // The fourth user is an "invalid" user: no token, so shouldn't show up.
        auto u_invalid = SyncUserMetadata(manager, "invalid_user");
        REQUIRE(manager.all_unmarked_users().size() == 4);

        SECTION("they should be added to the active users list when metadata is enabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 3);
            REQUIRE(validate_user_in_vector(users, identity_1, url_1, token_1));
            REQUIRE(validate_user_in_vector(users, identity_2, url_2, token_2));
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
        }
        SECTION("they should not be added to the active users list when metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 0);
        }
    }

    SECTION("when users are marked") {
        const std::string identity_1 = "foo-2";
        const std::string identity_2 = "bar-2";
        const std::string identity_3 = "baz-2";
        // Pre-populate the user directories.
        const auto user_dir_1 = file_manager.user_directory(identity_1);
        const auto user_dir_2 = file_manager.user_directory(identity_2);
        const auto user_dir_3 = file_manager.user_directory(identity_3);
        create_dummy_realm(user_dir_1 + "123456789");
        create_dummy_realm(user_dir_1 + "foo");
        create_dummy_realm(user_dir_2 + "123456789");
        create_dummy_realm(user_dir_3 + "foo");
        create_dummy_realm(user_dir_3 + "bar");
        create_dummy_realm(user_dir_3 + "baz");
        // Create the user metadata.
        auto u1 = SyncUserMetadata(manager, identity_1);
        u1.mark_for_removal();
        auto u2 = SyncUserMetadata(manager, identity_2);
        u2.mark_for_removal();
        // Don't mark this user for deletion.
        auto u3 = SyncUserMetadata(manager, identity_3);
        u3.set_state(url_3, token_3);

        SECTION("they should be cleaned up if metadata is enabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 1);
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_1);
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
        SECTION("they should be left alone if metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE_DIR_EXISTS(user_dir_1);
            REQUIRE_DIR_EXISTS(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
    }
}

TEST_CASE("sync_manager: file actions", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string realm_url = "https://example.realm.com/~/1";
    const std::string identity_1 = "foo-1";
    const std::string identity_2 = "bar-1";
    const std::string identity_3 = "baz-1";
    const std::string identity_4 = "baz-2";

    // Realm paths
    const std::string realm_path_1 = file_manager.path(identity_1, realm_url);
    const std::string realm_path_2 = file_manager.path(identity_2, realm_url);
    const std::string realm_path_3 = file_manager.path(identity_3, realm_url);
    const std::string realm_path_4 = file_manager.path(identity_4, realm_url);

    SECTION("Action::DeleteRealm") {
        // Create some file actions
        auto a1 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::DeleteRealm, realm_path_1, "user1", realm_url);
        auto a2 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::DeleteRealm, realm_path_2, "user2", realm_url);
        auto a3 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::DeleteRealm, realm_path_3, "user3", realm_url);

        SECTION("should properly delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
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
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
        }
    }

    SECTION("Action::HandleRealmForClientReset") {
        // Create some file actions
        const std::string recovery_1 = "recovery-1";
        const std::string recovery_2 = "recovery-2";
        const std::string recovery_3 = "recovery-3";
        auto a1 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::HandleRealmForClientReset, realm_path_1, "user1", realm_url, recovery_1);
        auto a2 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::HandleRealmForClientReset, realm_path_2, "user2", realm_url, recovery_2);
        auto a3 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::HandleRealmForClientReset, realm_path_3, "user3", realm_url, recovery_3);
        const std::string recovery_dir = file_manager.recovery_directory_path();

        SECTION("should properly copy the Realm file and delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // All Realms should be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_1)));
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_2)));
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_3)));
        }

        SECTION("should fail gracefully if the Realm is missing") {
            // Don't actually create the Realm files
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // There should not be recovery files.
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_1)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_2)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_3)));
        }

        SECTION("should work properly when manually driven") {
            // Create a Realm file
            create_dummy_realm(realm_path_4);
            // Configure the system
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            // Add a file action after the system is configured.
            REQUIRE_REALM_EXISTS(realm_path_4);
            auto a4 = SyncFileActionMetadata(manager, SyncFileActionMetadata::Action::HandleRealmForClientReset, realm_path_4, "user4", realm_url, recovery_1);
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 1);
            // Force the recovery. (In a real application, the user would have closed the files by now.)
            REQUIRE(SyncManager::shared().immediately_run_file_actions(realm_path_4));
            // There should be recovery files.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_4);
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_1)));
            pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
        }

        SECTION("should fail gracefully if there is already a file at the destination") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            create_dummy_realm(util::file_path_by_appending_component(recovery_dir, recovery_1));
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            // Most file actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 1);
            // Realms should be deleted.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_2)));
            CHECK(File::exists(util::file_path_by_appending_component(recovery_dir, recovery_3)));
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
            // There should not be recovery files.
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_1)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_2)));
            CHECK(!File::exists(util::file_path_by_appending_component(recovery_dir, recovery_3)));
        }
    }
}

TEST_CASE("sync_manager: metadata") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);

    SECTION("should be reset in case of decryption error") {
        SyncManager::shared().configure_file_system(base_path,
                                                    SyncManager::MetadataMode::Encryption,
                                                    make_test_encryption_key());

        SyncManager::shared().reset_for_testing();

        SyncManager::shared().configure_file_system(base_path,
                                                    SyncManager::MetadataMode::Encryption,
                                                    make_test_encryption_key(1),
                                                    true);
    }
}
