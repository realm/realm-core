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
                             const std::string& url,
                             const std::string& token) {
    for (auto& user : vector) {
        if (user->identity() == identity && user->server_url() == url && user->refresh_token() == token) {
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
            auto users = SyncManager::shared().all_users();
            REQUIRE(users.size() == 3);
            REQUIRE(validate_user_in_vector(users, identity_1, url_1, token_1));
            REQUIRE(validate_user_in_vector(users, identity_2, url_2, token_2));
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
        }
        SECTION("they should not be added to the active users list when metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_users();
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
            auto users = SyncManager::shared().all_users();
            REQUIRE(users.size() == 1);
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_1);
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
        SECTION("they should be left alone if metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_users();
            REQUIRE_DIR_EXISTS(user_dir_1);
            REQUIRE_DIR_EXISTS(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
    }
}
