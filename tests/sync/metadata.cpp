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

#include "object_schema.hpp"
#include "property.hpp"
#include "schema.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;
using SyncAction = SyncFileActionMetadata::Action;

static const std::string base_path = tmp_dir() + "/realm_objectstore_sync_metadata/";
static const std::string metadata_path = base_path + "/metadata.realm";

namespace {

Property make_nullable_string_property(const char* name)
{
    Property p = {name, PropertyType::String};
    p.is_nullable = true;
    return p;
}

Property make_primary_key_property(const char* name)
{
    Property p = {name, PropertyType::String};
    p.is_indexed = true;
    p.is_primary = true;
    return p;
}

}

TEST_CASE("sync_metadata: migration", "[sync") {
    reset_test_directory(base_path);
    const auto identity = "migrationtestuser";

    const Schema v0_schema{
        {"UserMetadata", {
            make_primary_key_property("identity"),
            {"marked_for_removal", PropertyType::Bool},
            make_nullable_string_property("auth_server_url"),
            make_nullable_string_property("user_token"),
        }},
        {"FileActionMetadata", {
            make_primary_key_property("original_name"),
            {"action", PropertyType::Int},
            make_nullable_string_property("new_name"),
            {"url", PropertyType::String},
            {"identity", PropertyType::String},
        }},
    };

    SECTION("properly upgrades from v0 to v1") {
        // Open v0 metadata (create a Realm directly)
        {
            Realm::Config config;
            config.path = metadata_path;
            config.schema = v0_schema;
            config.schema_version = 0;
            config.schema_mode = SchemaMode::Additive;
            auto realm = Realm::get_shared_realm(std::move(config));
            REQUIRE(realm);
        }
        // Open v1 metadata
        {
            SyncMetadataManager manager(metadata_path, false, none);
            auto user_metadata = SyncUserMetadata(manager, identity);
            REQUIRE(user_metadata.is_valid());
            CHECK(user_metadata.identity() == identity);
            CHECK(!user_metadata.is_admin());
            user_metadata.set_is_admin(true);
            CHECK(user_metadata.is_admin());
        }
    }
}

TEST_CASE("sync_metadata: user metadata", "[sync]") {
    reset_test_directory(base_path);
    SyncMetadataManager manager(metadata_path, false);

    SECTION("can be properly constructed") {
        const auto identity = "testcase1a";
        auto user_metadata = SyncUserMetadata(manager, identity);
        REQUIRE(user_metadata.identity() == identity);
        REQUIRE(user_metadata.server_url() == none);
        REQUIRE(user_metadata.user_token() == none);
        REQUIRE(!user_metadata.is_admin());
    }

    SECTION("properly reflects setting state") {
        const auto identity = "testcase1b";
        const std::string sample_url = "https://realm.example.org";
        const std::string sample_token = "this_is_a_user_token";
        auto user_metadata = SyncUserMetadata(manager, identity);
        user_metadata.set_state(sample_url, sample_token);
        REQUIRE(user_metadata.identity() == identity);
        REQUIRE(user_metadata.server_url() == sample_url);
        REQUIRE(user_metadata.user_token() == sample_token);
        user_metadata.set_is_admin(true);
        REQUIRE(user_metadata.is_admin());
    }

    SECTION("can be properly re-retrieved from the same manager") {
        const auto identity = "testcase1c";
        const std::string sample_url = "https://realm.example.org";
        const std::string sample_token = "this_is_a_user_token";
        auto first = SyncUserMetadata(manager, identity);
        first.set_state(sample_url, sample_token);
        // Get a second instance of the user metadata for the same identity.
        auto second = SyncUserMetadata(manager, identity, false);
        REQUIRE(second.identity() == identity);
        REQUIRE(second.server_url() == sample_url);
        REQUIRE(second.user_token() == sample_token);
    }

    SECTION("properly reflects changes across different instances") {
        const auto identity = "testcase1d";
        const std::string sample_url_1 = "https://realm.example.org";
        const std::string sample_token_1 = "this_is_a_user_token";
        auto first = SyncUserMetadata(manager, identity);
        auto second = SyncUserMetadata(manager, identity);
        CHECK(!first.is_admin());
        first.set_state(sample_url_1, sample_token_1);
        REQUIRE(first.identity() == identity);
        REQUIRE(first.server_url() == sample_url_1);
        REQUIRE(first.user_token() == sample_token_1);
        CHECK(!first.is_admin());
        REQUIRE(second.identity() == identity);
        REQUIRE(second.server_url() == sample_url_1);
        REQUIRE(second.user_token() == sample_token_1);
        CHECK(!second.is_admin());
        // Set the state again.
        const std::string sample_url_2 = "https://foobar.example.org";
        const std::string sample_token_2 = "this_is_another_user_token";
        second.set_state(sample_url_2, sample_token_2);
        REQUIRE(first.identity() == identity);
        REQUIRE(first.server_url() == sample_url_2);
        REQUIRE(first.user_token() == sample_token_2);
        REQUIRE(second.identity() == identity);
        REQUIRE(second.server_url() == sample_url_2);
        REQUIRE(second.user_token() == sample_token_2);
    }

    SECTION("can be removed") {
        const auto identity = "testcase1e";
        auto user_metadata = SyncUserMetadata(manager, identity);
        REQUIRE(user_metadata.is_valid());
        user_metadata.remove();
        REQUIRE(!user_metadata.is_valid());
    }

    SECTION("respects make_if_absent flag set to false in constructor") {
        const std::string sample_url = "https://realm.example.org";
        const std::string sample_token = "this_is_a_user_token";

        SECTION("with no prior metadata for the identifier") {
            const auto identity = "testcase1g1";
            auto user_metadata = SyncUserMetadata(manager, identity, false);
            REQUIRE(!user_metadata.is_valid());
        }
        SECTION("with valid prior metadata for the identifier") {
            const auto identity = "testcase1g2";
            auto first = SyncUserMetadata(manager, identity, true);
            first.set_state(sample_url, sample_token);
            auto second = SyncUserMetadata(manager, identity, false);
            REQUIRE(second.is_valid());
            REQUIRE(second.identity() == identity);
            REQUIRE(second.server_url() == sample_url);
            REQUIRE(second.user_token() == sample_token);
            REQUIRE(!second.is_admin());
        }
        SECTION("with invalid prior metadata for the identifier") {
            const auto identity = "testcase1g3";
            auto first = SyncUserMetadata(manager, identity);
            first.set_state(sample_url, sample_token);
            first.mark_for_removal();
            auto second = SyncUserMetadata(manager, identity, false);
            REQUIRE(!second.is_valid());
        }
    }
}

TEST_CASE("sync_metadata: user metadata APIs", "[sync]") {
    reset_test_directory(base_path);
    SyncMetadataManager manager(metadata_path, false);

    SECTION("properly list all marked and unmarked users") {
        const auto identity1 = "testcase2a1";
        const auto identity2 = "testcase2a2";
        const auto identity3 = "testcase2a3";
        auto first = SyncUserMetadata(manager, identity1);
        auto second = SyncUserMetadata(manager, identity2);
        auto third = SyncUserMetadata(manager, identity3);
        auto unmarked_users = manager.all_unmarked_users();
        REQUIRE(unmarked_users.size() == 3);
        REQUIRE(results_contains_user(unmarked_users, identity1));
        REQUIRE(results_contains_user(unmarked_users, identity2));
        REQUIRE(results_contains_user(unmarked_users, identity3));
        auto marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 0);
        // Now, mark a few users for removal.
        first.mark_for_removal();
        third.mark_for_removal();
        unmarked_users = manager.all_unmarked_users();
        REQUIRE(unmarked_users.size() == 1);
        REQUIRE(results_contains_user(unmarked_users, identity2));
        marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 2);
        REQUIRE(results_contains_user(marked_users, identity1));
        REQUIRE(results_contains_user(marked_users, identity3));
    }
}

TEST_CASE("sync_metadata: file action metadata", "[sync]") {
    reset_test_directory(base_path);
    SyncMetadataManager manager(metadata_path, false);

    const std::string identity_1 = "asdfg";
    const std::string identity_2 = "qwerty";
    const std::string url_1 = "realm://realm.example.com/1";
    const std::string url_2 = "realm://realm.example.com/2";

    SECTION("can be properly constructed") {
        const auto original_name = tmp_dir() + "foobar/test1";
        auto user_metadata = SyncFileActionMetadata(manager, SyncAction::HandleRealmForClientReset, original_name, url_1, identity_1);
        REQUIRE(user_metadata.original_name() == original_name);
        REQUIRE(user_metadata.new_name() == none);
        REQUIRE(user_metadata.action() == SyncAction::HandleRealmForClientReset);
        REQUIRE(user_metadata.url() == url_1);
        REQUIRE(user_metadata.user_identity() == identity_1);
    }

    SECTION("properly reflects updating state, across multiple instances") {
        const auto original_name = tmp_dir() + "foobar/test2a";
        const std::string new_name_1 = tmp_dir() + "foobar/test2b";
        const std::string new_name_2 = tmp_dir() + "foobar/test2c";
        auto user_metadata_1 = SyncFileActionMetadata(manager, SyncAction::HandleRealmForClientReset, original_name, url_1, identity_1, new_name_1);
        REQUIRE(user_metadata_1.original_name() == original_name);
        REQUIRE(user_metadata_1.new_name() == new_name_1);
        REQUIRE(user_metadata_1.action() == SyncAction::HandleRealmForClientReset);
        REQUIRE(user_metadata_1.url() == url_1);
        REQUIRE(user_metadata_1.user_identity() == identity_1);

        auto user_metadata_2 = SyncFileActionMetadata(manager, SyncAction::DeleteRealm, original_name, url_2, identity_2, new_name_2);
        REQUIRE(user_metadata_1.original_name() == original_name);
        REQUIRE(user_metadata_1.new_name() == new_name_2);
        REQUIRE(user_metadata_1.action() == SyncAction::DeleteRealm);
        REQUIRE(user_metadata_2.original_name() == original_name);
        REQUIRE(user_metadata_2.new_name() == new_name_2);
        REQUIRE(user_metadata_2.action() == SyncAction::DeleteRealm);
        REQUIRE(user_metadata_1.url() == url_2);
        REQUIRE(user_metadata_1.user_identity() == identity_2);
    }
}

TEST_CASE("sync_metadata: file action metadata APIs", "[sync]") {
    reset_test_directory(base_path);
    SyncMetadataManager manager(metadata_path, false);
    SECTION("properly list all pending actions, reflecting their deletion") {
        const auto filename1 = tmp_dir() + "foobar/file1";
        const auto filename2 = tmp_dir() + "foobar/file2";
        const auto filename3 = tmp_dir() + "foobar/file3";
        auto first = SyncFileActionMetadata(manager, SyncAction::HandleRealmForClientReset, filename1, "asdf", "realm://realm.example.com/1");
        auto second = SyncFileActionMetadata(manager, SyncAction::HandleRealmForClientReset, filename2, "asdf", "realm://realm.example.com/2");
        auto third = SyncFileActionMetadata(manager, SyncAction::HandleRealmForClientReset, filename3, "asdf", "realm://realm.example.com/3");
        auto actions = manager.all_pending_actions();
        REQUIRE(actions.size() == 3);
        REQUIRE(results_contains_original_name(actions, filename1));
        REQUIRE(results_contains_original_name(actions, filename2));
        REQUIRE(results_contains_original_name(actions, filename3));
        first.remove();
        second.remove();
        third.remove();
        REQUIRE(actions.size() == 0);
    }
}

TEST_CASE("sync_metadata: results", "[sync]") {
    reset_test_directory(base_path);
    SyncMetadataManager manager(metadata_path, false);

    SECTION("properly update as underlying items are added") {
        const auto identity1 = "testcase3a1";
        const auto identity2 = "testcase3a2";
        const auto identity3 = "testcase3a3";
        auto results = manager.all_unmarked_users();
        REQUIRE(results.size() == 0);
        // Add users, one at a time.
        auto first = SyncUserMetadata(manager, identity1);
        REQUIRE(results.size() == 1);
        REQUIRE(results_contains_user(results, identity1));
        auto second = SyncUserMetadata(manager, identity2);
        REQUIRE(results.size() == 2);
        REQUIRE(results_contains_user(results, identity2));
        auto third = SyncUserMetadata(manager, identity3);
        REQUIRE(results.size() == 3);
        REQUIRE(results_contains_user(results, identity3));
    }

    SECTION("properly update as underlying items are removed") {
        const auto identity1 = "testcase3b1";
        const auto identity2 = "testcase3b2";
        const auto identity3 = "testcase3b3";
        auto results = manager.all_unmarked_users();
        auto first = SyncUserMetadata(manager, identity1);
        auto second = SyncUserMetadata(manager, identity2);
        auto third = SyncUserMetadata(manager, identity3);
        REQUIRE(results.size() == 3);
        REQUIRE(results_contains_user(results, identity1));
        REQUIRE(results_contains_user(results, identity2));
        REQUIRE(results_contains_user(results, identity3));
        // Remove users, one at a time.
        third.remove();
        REQUIRE(results.size() == 2);
        REQUIRE(!results_contains_user(results, identity3));
        first.remove();
        REQUIRE(results.size() == 1);
        REQUIRE(!results_contains_user(results, identity1));
        second.remove();
        REQUIRE(results.size() == 0);
    }
}

TEST_CASE("sync_metadata: persistence across metadata manager instances", "[sync]") {
    reset_test_directory(base_path);

    SECTION("works for the basic case") {
        const auto identity = "testcase4a";
        const std::string sample_url = "https://realm.example.org";
        const std::string sample_token = "this_is_a_user_token";
        SyncMetadataManager first_manager(metadata_path, false);
        auto first = SyncUserMetadata(first_manager, identity);
        first.set_state(sample_url, sample_token);
        first.set_is_admin(true);
        REQUIRE(first.identity() == identity);
        REQUIRE(first.server_url() == sample_url);
        REQUIRE(first.user_token() == sample_token);
        REQUIRE(first.is_admin());
        SyncMetadataManager second_manager(metadata_path, false);
        auto second = SyncUserMetadata(second_manager, identity, false);
        REQUIRE(second.identity() == identity);
        REQUIRE(second.server_url() == sample_url);
        REQUIRE(second.user_token() == sample_token);
        REQUIRE(second.is_admin());
    }
}

TEST_CASE("sync_metadata: encryption", "[sync]") {
    reset_test_directory(base_path);

    SECTION("prohibits opening the metadata Realm with different keys") {
        SECTION("different keys") {
            SyncMetadataManager first_manager(metadata_path, true, make_test_encryption_key(10));
            REQUIRE_THROWS(SyncMetadataManager(metadata_path, true, make_test_encryption_key(11)));
        }
        SECTION("different encryption settings") {
            SyncMetadataManager first_manager(metadata_path, true, make_test_encryption_key(10));
            REQUIRE_THROWS(SyncMetadataManager(metadata_path, false));
        }
    }

    SECTION("works when enabled") {
        std::vector<char> key = make_test_encryption_key(10);
        const auto identity = "testcase5a";
        SyncMetadataManager manager(metadata_path, true, key);
        auto user_metadata = SyncUserMetadata(manager, identity);
        REQUIRE(user_metadata.identity() == identity);
        REQUIRE(user_metadata.server_url() == none);
        REQUIRE(user_metadata.user_token() == none);
        // Reopen the metadata file with the same key.
        SyncMetadataManager manager_2(metadata_path, true, key);
        auto user_metadata_2 = SyncUserMetadata(manager_2, identity, false);
        REQUIRE(user_metadata_2.identity() == identity);
        REQUIRE(user_metadata_2.is_valid());
    }
}
