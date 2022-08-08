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
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <realm/object-store/shared_realm.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/util/file.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

TEST_CASE("sync_file: percent-encoding APIs", "[sync]") {
    SECTION("does not encode a string that has no restricted characters") {
        const std::string expected = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
        auto actual = make_percent_encoded_string(expected);
        REQUIRE(actual == expected);
    }

    SECTION("properly encodes a sample Realm URL") {
        const std::string expected = "realms%3A%2F%2Fexample.com%2F%7E%2Ffoo_bar%2Fuser-realm";
        const std::string raw_string = "realms://example.com/~/foo_bar/user-realm";
        auto actual = make_percent_encoded_string(raw_string);
        REQUIRE(actual == expected);
    }

    SECTION("properly decodes a sample Realm URL") {
        const std::string expected = "realms://example.com/~/foo_bar/user-realm";
        const std::string encoded_string = "realms%3A%2F%2Fexample.com%2F%7E%2Ffoo_bar%2Fuser-realm";
        auto actual = make_raw_string(encoded_string);
        REQUIRE(actual == expected);
    }

    SECTION("properly encodes non-latin characters") {
        const std::string expected = "%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82";
        const std::string raw_string = "\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82";
        auto actual = make_percent_encoded_string(raw_string);
        REQUIRE(actual == expected);
    }

    SECTION("properly decodes non-latin characters") {
        const std::string expected = "\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82";
        const std::string encoded_string = "%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82";
        auto actual = make_raw_string(encoded_string);
        REQUIRE(actual == expected);
    }
}

TEST_CASE("sync_file: URL manipulation APIs", "[sync]") {
    SECTION("properly concatenates a path when the path has a trailing slash") {
        const std::string expected = "/foo/bar";
        const std::string path = "/foo";
        const std::string component = "bar";
        auto actual = file_path_by_appending_component(path, component);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a path when the component has a leading slash") {
        const std::string expected = "/foo/bar";
        const std::string path = "/foo";
        const std::string component = "bar";
        auto actual = file_path_by_appending_component(path, component);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a path when both arguments have slashes") {
        const std::string expected = "/foo/bar";
        const std::string path = "/foo";
        const std::string component = "bar";
        auto actual = file_path_by_appending_component(path, component);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a directory path when the component doesn't have a trailing slash") {
        const std::string expected = "/foo/bar/";
        const std::string path = "/foo";
        const std::string component = "bar";
        auto actual = file_path_by_appending_component(path, component, FilePathType::Directory);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a directory path when the component has a trailing slash") {
        const std::string expected = "/foo/bar/";
        const std::string path = "/foo";
        const std::string component = "bar";
        auto actual = file_path_by_appending_component(path, component, FilePathType::Directory);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates an extension when the path has a trailing dot") {
        const std::string expected = "/foo.management";
        const std::string path = "/foo.";
        const std::string component = "management";
        auto actual = file_path_by_appending_extension(path, component);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a path when the extension has a leading dot") {
        const std::string expected = "/foo.management";
        const std::string path = "/foo";
        const std::string component = ".management";
        auto actual = file_path_by_appending_extension(path, component);
        REQUIRE(fs::path(actual) == expected);
    }

    SECTION("properly concatenates a path when both arguments have dots") {
        const std::string expected = "/foo.management";
        const std::string path = "/foo.";
        const std::string component = ".management";
        auto actual = file_path_by_appending_extension(path, component);
        REQUIRE(fs::path(actual) == expected);
    }
}

TEST_CASE("sync_file: SyncFileManager APIs", "[sync]") {
    TestSyncManager tsm;

    const std::string identity = "abcdefghi";
    const std::string local_identity = "123456789";
    const std::string app_id = "test_app_id*$#@!%1";
    const std::string partition_str = random_string(10);
    const std::string partition = bson::Bson(partition_str).to_string();
    const std::string expected_clean_app_id = "test_app_id%2A%24%23%40%21%251";
    const auto manager_base_path = fs::path{tsm.base_file_path()}.make_preferred() / "file-manager";
    util::try_make_dir(manager_base_path.string());
    const auto manager_path = manager_base_path / "mongodb-realm" / expected_clean_app_id;
    auto manager = SyncFileManager(manager_base_path.string(), app_id);

    SECTION("Realm path APIs") {
        auto relative_path = "s_" + partition_str;
        ExpectedRealmPaths expected_paths(manager_base_path.string(), app_id, identity, local_identity, partition);

        SECTION("getting a Realm path") {
            auto actual = manager.realm_file_path(identity, local_identity, relative_path, partition);
            REQUIRE(expected_paths.current_preferred_path == actual);
        }

        SECTION("deleting a Realm for a valid user") {
            manager.realm_file_path(identity, local_identity, relative_path, partition);
            // Create the required files
            REQUIRE(create_dummy_realm(expected_paths.current_preferred_path));
            REQUIRE(File::exists(expected_paths.current_preferred_path));
            REQUIRE(File::exists(expected_paths.current_preferred_path + ".lock"));
            REQUIRE_DIR_EXISTS(expected_paths.current_preferred_path + ".management");
            // Delete the Realm
            REQUIRE(manager.remove_realm(identity, local_identity, relative_path, partition));
            // Ensure the files don't exist anymore
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path + ".lock"));
            REQUIRE_DIR_DOES_NOT_EXIST(expected_paths.current_preferred_path + ".management");
        }

        SECTION("deleting a Realm for an invalid user") {
            REQUIRE(!manager.remove_realm("invalid_user", "invalid_ident", relative_path, partition));
        }

        SECTION("hashed path is used if it already exists") {
            util::try_make_dir(manager_path.string());

            REQUIRE(!File::exists(expected_paths.fallback_hashed_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            REQUIRE(create_dummy_realm(expected_paths.fallback_hashed_path));
            REQUIRE(File::exists(expected_paths.fallback_hashed_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            auto actual = manager.realm_file_path(identity, local_identity, relative_path, partition);
            REQUIRE(actual == expected_paths.fallback_hashed_path);
            REQUIRE(File::exists(expected_paths.fallback_hashed_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            manager.remove_user_realms(identity, {expected_paths.fallback_hashed_path});
            REQUIRE(!File::exists(expected_paths.fallback_hashed_path));
        }

        SECTION("legacy local identity path is detected and used") {
            util::try_make_dir(manager_path.string());
            util::try_make_dir((manager_path / local_identity).string());
            REQUIRE(!File::exists(expected_paths.legacy_local_id_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            REQUIRE(create_dummy_realm(expected_paths.legacy_local_id_path));
            REQUIRE(File::exists(expected_paths.legacy_local_id_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));

            auto actual = manager.realm_file_path(identity, local_identity, relative_path, partition);
            REQUIRE(actual == expected_paths.legacy_local_id_path);
            REQUIRE(File::exists(expected_paths.legacy_local_id_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            manager.remove_user_realms(identity, {expected_paths.legacy_local_id_path});
            REQUIRE(!File::exists(expected_paths.legacy_local_id_path));
        }

        SECTION("legacy sync paths are detected and used") {
            REQUIRE(!File::exists(expected_paths.legacy_sync_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            for (auto& dir : expected_paths.legacy_sync_directories_to_make) {
                util::try_make_dir(dir);
            }
            REQUIRE(create_dummy_realm(expected_paths.legacy_sync_path));
            REQUIRE(File::exists(expected_paths.legacy_sync_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            auto actual = manager.realm_file_path(identity, local_identity, relative_path, partition);
            REQUIRE(actual == expected_paths.legacy_sync_path);
            REQUIRE(File::exists(expected_paths.legacy_sync_path));
            REQUIRE(!File::exists(expected_paths.current_preferred_path));
            manager.remove_user_realms(identity, {expected_paths.legacy_sync_path});
            REQUIRE(!File::exists(expected_paths.legacy_sync_path));
        }

        SECTION("paths have a fallback hashed location if the preferred path is too long") {
            const std::string long_path_name = std::string(500, 'a');
            REQUIRE(long_path_name.length() > 255); // linux name length limit
            auto actual = manager.realm_file_path(identity, local_identity, long_path_name, partition);
            REQUIRE(actual.length() < 500);
            REQUIRE(create_dummy_realm(actual));
            REQUIRE(File::exists(actual));
            manager.remove_user_realms(identity, {actual});
            REQUIRE(!File::exists(actual));
        }
    }

    SECTION("Utility path APIs") {
        const auto metadata_dir = manager_path / "server-utility" / "metadata";

        SECTION("getting the metadata path") {
            REQUIRE(manager.metadata_path() == metadata_dir / "sync_metadata.realm");
        }

        SECTION("removing the metadata Realm") {
            manager.metadata_path();
            REQUIRE_DIR_PATH_EXISTS(metadata_dir);
            manager.remove_metadata_realm();
            REQUIRE_DIR_PATH_DOES_NOT_EXIST(metadata_dir);
        }
    }
}
