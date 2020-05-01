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

#include "sync/app_credentials.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_user.hpp"

#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

static const std::string base_path = tmp_dir() + "/realm_objectstore_sync_user/";

TEST_CASE("sync_user: SyncManager `get_user()` API", "[sync]") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);
    const std::string identity = "sync_test_identity";
    const std::string refresh_token = ENCODE_FAKE_JWT("1234567890-fake-refresh-token");
    const std::string access_token = ENCODE_FAKE_JWT("1234567890-fake-access-token");
    const std::string server_url = "https://realm.example.org";

    SECTION("properly creates a new normal user") {
        auto user = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        REQUIRE(user);
        // The expected state for a newly created user:
        REQUIRE(user->identity() == identity);
        REQUIRE(user->provider_type() == server_url);
        REQUIRE(user->refresh_token() == refresh_token);
        REQUIRE(user->access_token() == access_token);
        REQUIRE(user->state() == SyncUser::State::LoggedIn);
    }

    SECTION("properly retrieves a previously created user, updating fields as necessary") {
        const std::string second_refresh_token = ENCODE_FAKE_JWT("0987654321-fake-refresh-token");
        const std::string second_access_token = ENCODE_FAKE_JWT("0987654321-fake-access-token");

        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        REQUIRE(first);
        REQUIRE(first->identity() == identity);
        REQUIRE(first->refresh_token() == refresh_token);
        // Get the user again, but with a different token.
        auto second = SyncManager::shared().get_user(identity, second_refresh_token, second_access_token, server_url);
        REQUIRE(second == first);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->access_token() == second_access_token);
        REQUIRE(second->refresh_token() == second_refresh_token);
    }

    SECTION("properly resurrects a logged-out user") {
        const std::string second_refresh_token = ENCODE_FAKE_JWT("0987654321-fake-refresh-token");
        const std::string second_access_token = ENCODE_FAKE_JWT("0987654321-fake-access-token");

        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        REQUIRE(first->identity() == identity);
        first->log_out();
        REQUIRE(first->state() == SyncUser::State::LoggedOut);
        // Get the user again, with a new token.
        auto second = SyncManager::shared().get_user(identity, second_refresh_token, second_access_token, server_url);
        REQUIRE(second == first);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->refresh_token() == second_refresh_token);
        REQUIRE(second->state() == SyncUser::State::LoggedIn);
    }
}

TEST_CASE("sync_user: SyncManager `get_existing_logged_in_user()` API", "[sync]") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoMetadata);
    const std::string identity = "sync_test_identity";
    const std::string refresh_token = ENCODE_FAKE_JWT("1234567890-fake-refresh-token");
    const std::string access_token = ENCODE_FAKE_JWT("1234567890-fake-access-token");
    const std::string server_url = "https://realm.example.org";

    SECTION("properly returns a null pointer when called for a non-existent user") {
        std::shared_ptr<SyncUser> user = SyncManager::shared().get_existing_logged_in_user(identity);
        REQUIRE(!user);
    }

    SECTION("properly returns an existing logged-in user") {
        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        REQUIRE(first->identity() == identity);
        REQUIRE(first->state() == SyncUser::State::LoggedIn);
        // Get that user using the 'existing user' API.
        auto second = SyncManager::shared().get_existing_logged_in_user(identity);
        REQUIRE(second == first);
        REQUIRE(second->refresh_token() == refresh_token);
    }

    SECTION("properly returns a null pointer for a logged-out user") {
        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        first->log_out();
        REQUIRE(first->identity() == identity);
        REQUIRE(first->state() == SyncUser::State::LoggedOut);
        // Get that user using the 'existing user' API.
        auto second = SyncManager::shared().get_existing_logged_in_user(identity);
        REQUIRE(!second);
    }
}

TEST_CASE("sync_user: logout", "[sync]") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoMetadata);
    const std::string identity = "sync_test_identity";
    const std::string refresh_token = ENCODE_FAKE_JWT("1234567890-fake-refresh-token");
    const std::string access_token = ENCODE_FAKE_JWT("1234567890-fake-access-token");
    const std::string server_url = "https://realm.example.org";

    SECTION("properly changes the state of the user object") {
        auto user = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        REQUIRE(user->state() == SyncUser::State::LoggedIn);
        user->log_out();
        REQUIRE(user->state() == SyncUser::State::LoggedOut);
    }
}

TEST_CASE("sync_user: user persistence", "[sync]") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager("", base_path, SyncManager::MetadataMode::NoEncryption);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    SECTION("properly persists a user's information upon creation") {
        const std::string identity = "test_identity_1";
        const std::string refresh_token = ENCODE_FAKE_JWT("r-token-1");
        const std::string access_token = ENCODE_FAKE_JWT("a-token-1");
        const std::string server_url = "https://realm.example.org/1/";
        auto user = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        // Now try to pull the user out of the shadow manager directly.
        auto metadata = manager.get_or_make_user_metadata(identity, server_url, false);
        REQUIRE(metadata->is_valid());
        REQUIRE(metadata->provider_type() == server_url);
        REQUIRE(metadata->access_token() == access_token);
    }

    SECTION("properly persists a user's information when the user is updated") {
        const std::string identity = "test_identity_2";
        const std::string refresh_token = ENCODE_FAKE_JWT("r_token-2a");
        const std::string access_token = ENCODE_FAKE_JWT("a_token-1a");
        const std::string server_url = "https://realm.example.org/2/";
        // Create the user and validate it.
        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, server_url);
        auto first_metadata = manager.get_or_make_user_metadata(identity, server_url, false);
        REQUIRE(first_metadata->is_valid());
        REQUIRE(first_metadata->access_token() == access_token);
        const std::string token_2 = ENCODE_FAKE_JWT("token-2b");
        // Update the user.
        auto second = SyncManager::shared().get_user(identity, refresh_token, token_2, server_url);
        auto second_metadata = manager.get_or_make_user_metadata(identity, server_url, false);
        REQUIRE(second_metadata->is_valid());
        REQUIRE(second_metadata->access_token() == token_2);
    }

    SECTION("properly does not mark a user when the user is logged out and not anon") {
        const std::string identity = "test_identity_3";
        const std::string refresh_token = ENCODE_FAKE_JWT("r-token-3");
        const std::string access_token = ENCODE_FAKE_JWT("a-token-3");
        const std::string provider_type = app::IdentityProviderGoogle;
        // Create the user and validate it.
        auto user = SyncManager::shared().get_user(identity, refresh_token, access_token, provider_type);
        auto marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 0);
        // Log out the user.
        user->log_out();
        marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 0);
    }

    SECTION("properly removes a user when the user is logged out and is anon") {
        const std::string identity = "test_identity_3";
        const std::string refresh_token = ENCODE_FAKE_JWT("r-token-3");
        const std::string access_token = ENCODE_FAKE_JWT("a-token-3");
        const std::string provider_type = app::IdentityProviderAnonymous;
        // Create the user and validate it.
        auto user = SyncManager::shared().get_user(identity, refresh_token, access_token, provider_type);
        auto marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 0);
        // Log out the user.
        user->log_out();
        REQUIRE(SyncManager::shared().all_users().size() == 0);
    }

    SECTION("properly revives a logged-out user when it's requested again") {
        const std::string identity = "test_identity_3";
        const std::string refresh_token = ENCODE_FAKE_JWT("r-token-4a");
        const std::string access_token = ENCODE_FAKE_JWT("a-token-4a");
        const std::string provider_type = app::IdentityProviderApple;
        // Create the user and log it out.
        auto first = SyncManager::shared().get_user(identity, refresh_token, access_token, provider_type);
        first->log_out();
        REQUIRE(SyncManager::shared().all_users().size() == 1);
        REQUIRE(SyncManager::shared().all_users()[0]->state() == SyncUser::State::LoggedOut);
        // Log the user back in.
        const std::string r_token_2 = ENCODE_FAKE_JWT("r-token-4b");
        const std::string a_token_2 = ENCODE_FAKE_JWT("atoken-4b");
        auto second = SyncManager::shared().get_user(identity, r_token_2, a_token_2, provider_type);
        REQUIRE(SyncManager::shared().all_users().size() == 1);
        REQUIRE(SyncManager::shared().all_users()[0]->state() == SyncUser::State::LoggedIn);
    }
}
