////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "object.hpp"
#include "impl/object_accessor_impl.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "property.hpp"
#include "schema.hpp"
#include "sync/sync_permission.hpp"

#include "sync_test_utils.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include <unistd.h>

using namespace realm;

TEST_CASE("`Permission` class", "[sync]") {
    SECTION("paths_are_equivalent() properly returns true") {
        // Identical paths and identical users for tilde-paths.
        CHECK(Permission::paths_are_equivalent("/~/foo", "/~/foo", "user1", "user1"));
        // Identical paths for non-tilde paths.
        CHECK(Permission::paths_are_equivalent("/user2/foo", "/user2/foo", "user1", "user1"));
        CHECK(Permission::paths_are_equivalent("/user2/foo", "/user2/foo", "user1", "user2"));
        // First path can be turned into second path.
        CHECK(Permission::paths_are_equivalent("/~/foo", "/user1/foo", "user1", "user2"));
        // Second path can be turned into first path.
        CHECK(Permission::paths_are_equivalent("/user1/foo", "/~/foo", "user2", "user1"));
    }

    SECTION("paths_are_equivalent() properly returns false") {
        // Different tilde-paths.
        CHECK(!Permission::paths_are_equivalent("/~/foo", "/~/bar", "user1", "user1"));
        // Different non-tilde paths.
        CHECK(!Permission::paths_are_equivalent("/user1/foo", "/user2/bar", "user1", "user1"));
        // Identical paths and different users for tilde-paths.
        CHECK(!Permission::paths_are_equivalent("/~/foo", "/~/foo", "user1", "user2"));
        // First path cannot be turned into second path.
        CHECK(!Permission::paths_are_equivalent("/~/foo", "/user1/foo", "user2", "user2"));
        // Second path cannot be turned into first path.
        CHECK(!Permission::paths_are_equivalent("/user1/foo", "/~/foo", "user2", "user2"));
    }
}

static const std::string base_path = tmp_dir() + "realm_objectstore_sync_permissions/";

TEST_CASE("Object-level Permissions") {
    reset_test_directory(base_path);
    TestSyncManager init_sync_manager(base_path);

    SyncServer server{StartImmediately{false}};

    SyncTestFile config{server, "default"};
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int}
        }},
    };

    auto create_object = [](auto&& r) -> Table& {
        r->begin_transaction();
        auto table = r->read_group().get_table("class_object");
        table->create_object();
        r->commit_transaction();
        return *table;
    };

    SECTION("Non-sync Realms") {
        SECTION("permit all operations") {
            config.sync_config = nullptr;
            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(*table.begin()) == ComputedPrivileges::AllObject);
        }
    }

    SECTION("Full sync Realms") {
        SECTION("permit all operations") {
            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(*table.begin()) == ComputedPrivileges::AllObject);
        }
    }
}
