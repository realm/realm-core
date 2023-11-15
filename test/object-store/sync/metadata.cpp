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

#include <util/test_path.hpp>
#include <util/test_utils.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/impl/apple/keychain_helper.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

#include <iostream>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;
using SyncAction = SyncFileActionMetadata::Action;

static const std::string base_path = util::make_temp_dir() + "realm_objectstore_sync_metadata";
static const std::string metadata_path = base_path + "/metadata.realm";

TEST_CASE("sync_metadata: user metadata", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SyncMetadataManager manager(metadata_path, false);

    SECTION("can be properly constructed") {
        const auto identity = "testcase1a";
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        REQUIRE(user_metadata->identity() == identity);
        REQUIRE(user_metadata->access_token().empty());
    }

    SECTION("properly reflects updating state") {
        const auto identity = "testcase1b";
        const std::string sample_token = "this_is_a_user_token";
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        user_metadata->set_access_token(sample_token);
        REQUIRE(user_metadata->identity() == identity);
        REQUIRE(user_metadata->access_token() == sample_token);
    }

    SECTION("can be properly re-retrieved from the same manager") {
        const auto identity = "testcase1c";
        const std::string sample_token = "this_is_a_user_token";
        auto first = manager.get_or_make_user_metadata(identity);
        first->set_access_token(sample_token);
        // Get a second instance of the user metadata for the same identity.
        auto second = manager.get_or_make_user_metadata(identity, false);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->access_token() == sample_token);
    }

    SECTION("properly reflects changes across different instances") {
        const auto identity = "testcase1d";
        const std::string sample_token_1 = "this_is_a_user_token";
        auto first = manager.get_or_make_user_metadata(identity);
        auto second = manager.get_or_make_user_metadata(identity);
        first->set_access_token(sample_token_1);
        REQUIRE(first->identity() == identity);
        REQUIRE(first->access_token() == sample_token_1);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->access_token() == sample_token_1);
        // Set the state again.
        const std::string sample_token_2 = "this_is_another_user_token";
        second->set_access_token(sample_token_2);
        REQUIRE(first->identity() == identity);
        REQUIRE(first->access_token() == sample_token_2);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->access_token() == sample_token_2);
    }

    SECTION("can be removed") {
        const auto identity = "testcase1e";
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        REQUIRE(user_metadata->is_valid());
        user_metadata->remove();
        REQUIRE(!user_metadata->is_valid());
    }

    SECTION("respects make_if_absent flag set to false in constructor") {
        const std::string sample_token = "this_is_a_user_token";

        SECTION("with no prior metadata for the identifier") {
            const auto identity = "testcase1g1";
            auto user_metadata = manager.get_or_make_user_metadata(identity, false);
            REQUIRE(!user_metadata);
        }
        SECTION("with valid prior metadata for the identifier") {
            const auto identity = "testcase1g2";
            auto first = manager.get_or_make_user_metadata(identity);
            first->set_access_token(sample_token);
            auto second = manager.get_or_make_user_metadata(identity, false);
            REQUIRE(second->is_valid());
            REQUIRE(second->identity() == identity);
            REQUIRE(second->access_token() == sample_token);
        }
        SECTION("with invalid prior metadata for the identifier") {
            const auto identity = "testcase1g3";
            auto first = manager.get_or_make_user_metadata(identity);
            first->set_access_token(sample_token);
            first->set_state(SyncUser::State::Removed);
            auto second = manager.get_or_make_user_metadata(identity, false);
            REQUIRE(!second);
        }
    }
}

TEST_CASE("sync_metadata: user metadata APIs", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SyncMetadataManager manager(metadata_path, false);
    const std::string provider_type = "https://realm.example.org";

    SECTION("properly list all marked and unmarked users") {
        const auto identity1 = "testcase2a1";
        const auto identity2 = "testcase2a3";
        auto first = manager.get_or_make_user_metadata(identity1);
        auto second = manager.get_or_make_user_metadata(identity1);
        auto third = manager.get_or_make_user_metadata(identity2);
        auto unmarked_users = manager.all_unmarked_users();
        REQUIRE(unmarked_users.size() == 2);
        REQUIRE(results_contains_user(unmarked_users, identity1));
        REQUIRE(results_contains_user(unmarked_users, identity2));
        auto marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 0);
        // Now, mark a few users for removal.
        first->set_state(SyncUser::State::Removed);
        unmarked_users = manager.all_unmarked_users();
        REQUIRE(unmarked_users.size() == 1);
        REQUIRE(results_contains_user(unmarked_users, identity2));
        marked_users = manager.all_users_marked_for_removal();
        REQUIRE(marked_users.size() == 1);
        REQUIRE(results_contains_user(marked_users, identity1));
    }
}

TEST_CASE("sync_metadata: file action metadata", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SyncMetadataManager manager(metadata_path, false);

    const std::string local_uuid_1 = "asdfg";
    const std::string local_uuid_2 = "qwerty";
    const std::string url_1 = "realm://realm.example.com/1";
    const std::string url_2 = "realm://realm.example.com/2";

    SECTION("can be properly constructed") {
        const auto original_name = util::make_temp_dir() + "foobar/test1";
        manager.make_file_action_metadata(original_name, url_1, local_uuid_1, SyncAction::BackUpThenDeleteRealm);
        auto metadata = *manager.get_file_action_metadata(original_name);
        REQUIRE(metadata.original_name() == original_name);
        REQUIRE(metadata.new_name() == none);
        REQUIRE(metadata.action() == SyncAction::BackUpThenDeleteRealm);
        REQUIRE(metadata.partition() == url_1);
        REQUIRE(metadata.user_local_uuid() == local_uuid_1);
    }

    SECTION("properly reflects updating state, across multiple instances") {
        const auto original_name = util::make_temp_dir() + "foobar/test2a";
        const std::string new_name_1 = util::make_temp_dir() + "foobar/test2b";
        const std::string new_name_2 = util::make_temp_dir() + "foobar/test2c";

        manager.make_file_action_metadata(original_name, url_1, local_uuid_1, SyncAction::BackUpThenDeleteRealm,
                                          new_name_1);
        auto metadata_1 = *manager.get_file_action_metadata(original_name);
        REQUIRE(metadata_1.original_name() == original_name);
        REQUIRE(metadata_1.new_name() == new_name_1);
        REQUIRE(metadata_1.action() == SyncAction::BackUpThenDeleteRealm);
        REQUIRE(metadata_1.partition() == url_1);
        REQUIRE(metadata_1.user_local_uuid() == local_uuid_1);

        manager.make_file_action_metadata(original_name, url_2, local_uuid_2, SyncAction::DeleteRealm, new_name_2);
        auto metadata_2 = *manager.get_file_action_metadata(original_name);
        REQUIRE(metadata_1.original_name() == original_name);
        REQUIRE(metadata_1.new_name() == new_name_2);
        REQUIRE(metadata_1.action() == SyncAction::DeleteRealm);
        REQUIRE(metadata_2.original_name() == original_name);
        REQUIRE(metadata_2.new_name() == new_name_2);
        REQUIRE(metadata_2.action() == SyncAction::DeleteRealm);
        REQUIRE(metadata_1.partition() == url_2);
        REQUIRE(metadata_1.user_local_uuid() == local_uuid_2);
    }
}

TEST_CASE("sync_metadata: file action metadata APIs", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SyncMetadataManager manager(metadata_path, false);
    SECTION("properly list all pending actions, reflecting their deletion") {
        const auto filename1 = util::make_temp_dir() + "foobar/file1";
        const auto filename2 = util::make_temp_dir() + "foobar/file2";
        const auto filename3 = util::make_temp_dir() + "foobar/file3";
        manager.make_file_action_metadata(filename1, "asdf", "realm://realm.example.com/1",
                                          SyncAction::BackUpThenDeleteRealm);
        manager.make_file_action_metadata(filename2, "asdf", "realm://realm.example.com/2",
                                          SyncAction::BackUpThenDeleteRealm);
        manager.make_file_action_metadata(filename3, "asdf", "realm://realm.example.com/3",
                                          SyncAction::BackUpThenDeleteRealm);
        auto actions = manager.all_pending_actions();
        REQUIRE(actions.size() == 3);
        REQUIRE(results_contains_original_name(actions, filename1));
        REQUIRE(results_contains_original_name(actions, filename2));
        REQUIRE(results_contains_original_name(actions, filename3));
        manager.get_file_action_metadata(filename1)->remove();
        manager.get_file_action_metadata(filename2)->remove();
        manager.get_file_action_metadata(filename3)->remove();
        REQUIRE(actions.size() == 0);
    }
}

TEST_CASE("sync_metadata: results", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SyncMetadataManager manager(metadata_path, false);
    const auto identity1 = "testcase3a1";
    const auto identity2 = "testcase3a3";

    SECTION("properly update as underlying items are added") {
        auto results = manager.all_unmarked_users();
        REQUIRE(results.size() == 0);
        // Add users, one at a time.
        auto first = manager.get_or_make_user_metadata(identity1);
        REQUIRE(results.size() == 1);
        REQUIRE(results_contains_user(results, identity1));
        auto second = manager.get_or_make_user_metadata(identity2);
        REQUIRE(results.size() == 2);
        REQUIRE(results_contains_user(results, identity2));
    }

    SECTION("properly update as underlying items are removed") {
        auto results = manager.all_unmarked_users();
        auto first = manager.get_or_make_user_metadata(identity1);
        auto second = manager.get_or_make_user_metadata(identity2);
        REQUIRE(results.size() == 2);
        REQUIRE(results_contains_user(results, identity1));
        REQUIRE(results_contains_user(results, identity2));
        // Remove users, one at a time.
        first->remove();
        REQUIRE(results.size() == 1);
        REQUIRE(!results_contains_user(results, identity1));
        second->remove();
        REQUIRE(results.size() == 0);
    }
}

TEST_CASE("sync_metadata: persistence across metadata manager instances", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    SECTION("works for the basic case") {
        const auto identity = "testcase4a";
        const std::string provider_type = "any-type";
        const std::string sample_token = "this_is_a_user_token";
        SyncMetadataManager first_manager(metadata_path, false);
        auto first = first_manager.get_or_make_user_metadata(identity);
        first->set_access_token(sample_token);
        REQUIRE(first->identity() == identity);
        REQUIRE(first->access_token() == sample_token);
        REQUIRE(first->state() == SyncUser::State::LoggedIn);
        first->set_state(SyncUser::State::LoggedOut);

        SyncMetadataManager second_manager(metadata_path, false);
        auto second = second_manager.get_or_make_user_metadata(identity, false);
        REQUIRE(second->identity() == identity);
        REQUIRE(second->access_token() == sample_token);
        REQUIRE(second->state() == SyncUser::State::LoggedOut);
    }
}

TEST_CASE("sync_metadata: encryption", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    const auto identity0 = "identity0";
    SECTION("prohibits opening the metadata Realm with different keys") {
        SECTION("different keys") {
            {
                // Open metadata realm, make metadata
                std::vector<char> key0 = make_test_encryption_key(10);
                SyncMetadataManager manager0(metadata_path, true, key0);

                auto user_metadata0 = manager0.get_or_make_user_metadata(identity0);
                REQUIRE(bool(user_metadata0));
                CHECK(user_metadata0->identity() == identity0);
                CHECK(user_metadata0->access_token().empty());
                CHECK(user_metadata0->is_valid());
            }
            // Metadata realm is closed because only reference to the realm (user_metadata) is now out of scope
            // Open new metadata realm at path with different key
            std::vector<char> key1 = make_test_encryption_key(11);
            SyncMetadataManager manager1(metadata_path, true, key1);

            auto user_metadata1 = manager1.get_or_make_user_metadata(identity0, false);
            // Expect previous metadata to have been deleted
            CHECK_FALSE(bool(user_metadata1));

            // But new metadata can still be created
            const auto identity1 = "identity1";
            auto user_metadata2 = manager1.get_or_make_user_metadata(identity1);
            CHECK(user_metadata2->identity() == identity1);
            CHECK(user_metadata2->access_token().empty());
            CHECK(user_metadata2->is_valid());
        }
        SECTION("different encryption settings") {
            {
                // Encrypt metadata realm at path, make metadata
                SyncMetadataManager manager0(metadata_path, true, make_test_encryption_key(10));

                auto user_metadata0 = manager0.get_or_make_user_metadata(identity0);
                REQUIRE(bool(user_metadata0));
                CHECK(user_metadata0->identity() == identity0);
                CHECK(user_metadata0->access_token().empty());
                CHECK(user_metadata0->is_valid());
            }
            // Metadata realm is closed because only reference to the realm (user_metadata) is now out of scope
            // Open new metadata realm at path with different encryption configuration
            SyncMetadataManager manager1(metadata_path, false);
            auto user_metadata1 = manager1.get_or_make_user_metadata(identity0, false);
            // Expect previous metadata to have been deleted
            CHECK_FALSE(bool(user_metadata1));

            // But new metadata can still be created
            const auto identity1 = "identity1";
            auto user_metadata2 = manager1.get_or_make_user_metadata(identity1);
            CHECK(user_metadata2->identity() == identity1);
            CHECK(user_metadata2->access_token().empty());
            CHECK(user_metadata2->is_valid());
        }
    }

    SECTION("works when enabled") {
        std::vector<char> key = make_test_encryption_key(10);
        const auto identity = "testcase5a";
        SyncMetadataManager manager(metadata_path, true, key);
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        REQUIRE(bool(user_metadata));
        CHECK(user_metadata->identity() == identity);
        CHECK(user_metadata->access_token().empty());
        CHECK(user_metadata->is_valid());
        // Reopen the metadata file with the same key.
        SyncMetadataManager manager_2(metadata_path, true, key);
        auto user_metadata_2 = manager_2.get_or_make_user_metadata(identity, false);
        REQUIRE(bool(user_metadata_2));
        CHECK(user_metadata_2->identity() == identity);
        CHECK(user_metadata_2->is_valid());
    }

    SECTION("enabled without custom encryption key") {
#if REALM_PLATFORM_APPLE
        static bool can_access_keychain = [] {
            bool can_access = keychain::create_new_metadata_realm_key() != none;
            if (!can_access) {
                std::cout << "Skipping keychain tests as the keychain is not accessible\n";
            }
            return can_access;
        }();
        if (!can_access_keychain) {
            return;
        }
        auto delete_key = util::make_scope_exit([=]() noexcept {
            keychain::delete_metadata_realm_encryption_key();
        });

        SECTION("automatically generates an encryption key for new files") {
            {
                SyncMetadataManager manager(metadata_path, true, none);
                manager.set_current_user_identity(identity0);
            }

            // Should be able to reopen and read data
            {
                SyncMetadataManager manager(metadata_path, true, none);
                REQUIRE(manager.get_current_user_identity() == identity0);
            }

            // Verify that the file is actually encrypted
            REQUIRE_EXCEPTION(Group(metadata_path), InvalidDatabase,
                              Catch::Matchers::ContainsSubstring("invalid mnemonic"));
        }

        SECTION("leaves existing unencrypted files unencrypted") {
            {
                SyncMetadataManager manager(metadata_path, false, none);
                manager.set_current_user_identity(identity0);
            }
            {
                SyncMetadataManager manager(metadata_path, true, none);
                REQUIRE(manager.get_current_user_identity() == identity0);
            }
            REQUIRE_NOTHROW(Group(metadata_path));
        }

        SECTION("recreates the file if the old encryption key was lost") {
            {
                SyncMetadataManager manager(metadata_path, true, none);
                manager.set_current_user_identity(identity0);
            }

            keychain::delete_metadata_realm_encryption_key();

            {
                // File should now be missing the data
                SyncMetadataManager manager(metadata_path, true, none);
                REQUIRE(manager.get_current_user_identity() == none);
            }
            // New file should be encrypted
            REQUIRE_EXCEPTION(Group(metadata_path), InvalidDatabase,
                              Catch::Matchers::ContainsSubstring("invalid mnemonic"));
        }
#else
        REQUIRE_EXCEPTION(SyncMetadataManager(metadata_path, true, none), InvalidArgument,
                          "Metadata Realm encryption was specified, but no encryption key was provided.");
#endif
    }
}

#ifndef SWIFT_PACKAGE // The SPM build currently doesn't copy resource files
TEST_CASE("sync metadata: can open old metadata realms", "[sync][metadata]") {
    util::try_make_dir(base_path);
    auto close = util::make_scope_exit([=]() noexcept {
        util::try_remove_dir_recursive(base_path);
    });

    const std::string provider_type = "https://realm.example.org";
    const auto identity = "metadata migration test";
    const std::string sample_token = "metadata migration token";

    const auto access_token_1 = encode_fake_jwt("access token 1", 456, 123);
    const auto access_token_2 = encode_fake_jwt("access token 2", 456, 124);
    const auto refresh_token_1 = encode_fake_jwt("refresh token 1", 456, 123);
    const auto refresh_token_2 = encode_fake_jwt("refresh token 2", 456, 124);

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
            SyncMetadataManager manager(metadata_path, false);
            auto user_metadata = manager.get_or_make_user_metadata(identity);
            user_metadata->set_access_token(sample_token);
        }
#endif

        // Open the metadata Realm directly and grab the schema version from it
        Realm::Config config;
        config.path = metadata_path;
        auto realm = Realm::get_shared_realm(config);
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
        SyncMetadataManager manager(metadata_path, false);
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        REQUIRE(user_metadata->identity() == identity);
        REQUIRE(user_metadata->access_token() == sample_token);
    }

    SECTION("open schema version 5") {
        File::copy(test_util::get_test_resource_path() + "sync-metadata-v5.realm", metadata_path);
        SyncMetadataManager manager(metadata_path, false);
        auto user_metadata = manager.get_or_make_user_metadata(identity);
        REQUIRE(user_metadata->identity() == identity);
        REQUIRE(user_metadata->access_token() == sample_token);
    }

    SECTION("open schema version 6") {
        using State = SyncUser::State;
        File::copy(test_util::get_test_resource_path() + "sync-metadata-v6.realm", metadata_path);
        SyncMetadataManager manager(metadata_path, false);

        SyncUserIdentity id_1{"identity 1", "a"};
        SyncUserIdentity id_2{"identity 2", "b"};
        SyncUserIdentity id_shared{"shared identity", "shared"};
        const std::vector<SyncUserIdentity> all_ids = {id_1, id_shared, id_2};
        const std::vector<std::string> realm_files = {"file 1", "file 2", "file 3"};

        auto check_user = [&](const char* user_id, State state, const std::string& access_token,
                              const std::string& refresh_token, const std::vector<std::string>& uuids) {
            auto user = manager.get_or_make_user_metadata(user_id, false);
            CAPTURE(user_id);
            REQUIRE(user);
            CHECK(user->state() == state);
            CHECK(user->access_token() == access_token);
            CHECK(user->refresh_token() == refresh_token);
            CHECK(user->legacy_identities() == uuids);
            CHECK(user->identities() == all_ids);
            CHECK(user->realm_file_paths() == realm_files);
        };

        REQUIRE_FALSE(manager.get_or_make_user_metadata("removed user", false));
        check_user("first logged in, second logged out", State::LoggedIn, access_token_1, refresh_token_1,
                   {"1", "2"});
        check_user("first logged in, second removed", State::LoggedIn, access_token_1, refresh_token_1, {"3", "4"});
        check_user("second logged in, first logged out", State::LoggedIn, access_token_2, refresh_token_2,
                   {"5", "6"});
        check_user("second logged in, first removed", State::LoggedIn, access_token_2, refresh_token_2, {"7", "8"});
        check_user("both logged in, first newer", State::LoggedIn, access_token_2, refresh_token_1, {"9", "10"});
        check_user("both logged in, second newer", State::LoggedIn, access_token_2, refresh_token_2, {"11", "12"});
    }
}
#endif // SWIFT_PACKAGE
