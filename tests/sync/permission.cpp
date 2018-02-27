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

constexpr const char* result_sets_type_name = "__ResultSets";

static void update_schema(Group& group, Property matches_property)
{
    Schema current_schema;
    std::string table_name = ObjectStore::table_name_for_object_type(result_sets_type_name);
    if (group.has_table(table_name))
        current_schema = {ObjectSchema{group, result_sets_type_name}};

    Schema desired_schema({
        ObjectSchema(result_sets_type_name, {
            {"matches_property", PropertyType::String},
            {"query", PropertyType::String},
            {"status", PropertyType::Int},
            {"error_message", PropertyType::String},
            {"query_parse_counter", PropertyType::Int},
            std::move(matches_property)
        })
    });
    auto required_changes = current_schema.compare(desired_schema);
    if (!required_changes.empty())
        ObjectStore::apply_additive_changes(group, required_changes, true);
}

static void subscribe_to_all(std::shared_ptr<Realm> const& r)
{
    using namespace std::string_literals;

    r->begin_transaction();
    update_schema(r->read_group(),
                  Property("object_matches", PropertyType::Object|PropertyType::Array, "object"));
    ObjectSchema schema{r->read_group(), result_sets_type_name};

    CppContext context;
    auto obj = Object::create<util::Any>(context, r, schema, AnyDict{
        {"matches_property", "object_matches"s},
        {"query", "TRUEPREDICATE"s},
        {"status", int64_t(0)},
        {"error_message", ""s},
        {"query_parse_counter", int64_t(0)},
    }, false);

    r->commit_transaction();

    while (any_cast<int64_t>(obj.get_property_value<util::Any>(context, "status")) != 1) {
        wait_for_download(*r);
        r->refresh();
    }
}

TEST_CASE("Object-level Permissions") {
    SyncManager::shared().configure_file_system(tmp_dir(), SyncManager::MetadataMode::NoEncryption);

    SyncServer server{StartImmediately{false}};

    SyncTestFile config{server, "default"};
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object", {
            {"value", PropertyType::Int}
        }},
    };

    auto create_object = [](auto&& r) -> Table& {
        r->begin_transaction();
        auto& table = *r->read_group().get_table("class_object");
        sync::create_object(r->read_group(), table);
        r->commit_transaction();
        return table;
    };

    SECTION("Non-sync Realms") {
        SECTION("permit all operations") {
            config.sync_config = nullptr;
            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(table[0]) == ComputedPrivileges::AllObject);
        }
    }

    SECTION("Non-partial sync Realms") {
        SECTION("permit all operations") {
            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(table[0]) == ComputedPrivileges::AllObject);
        }
    }

    SECTION("Partial-sync Realms") {
        SECTION("permit all operations prior to first sync") {
            config.sync_config->is_partial = true;

            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(table[0]) == ComputedPrivileges::AllObject);
        }

        SECTION("continue to permit all operations after syncing locally-created data") {
            config.sync_config->is_partial = true;

            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);
            server.start();

            wait_for_upload(*r);
            wait_for_download(*r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(table[0]) == ComputedPrivileges::AllObject);
        }

        SECTION("permit all operations on a downloaded Realm created as a non-partial Realm when logged in as an admin") {
            server.start();
            {
                auto r = Realm::get_shared_realm(config);
                create_object(r);
                wait_for_upload(*r);
            }

            SyncTestFile config2{server, "default", util::none, true};
            config2.automatic_change_notifications = false;
            auto r = Realm::get_shared_realm(config2);
            wait_for_download(*r);
            subscribe_to_all(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(r->read_group().get_table("class_object")->get(0)) == ComputedPrivileges::AllObject);
        }

        SECTION("permit nothing on pre-existing types in a downloaded Realm created as a non-partial Realm") {
            server.start();
            {
                auto r = Realm::get_shared_realm(config);
                create_object(r);
                wait_for_upload(*r);
            }

            SyncTestFile config2{server, "default", util::none, true};
            config2.automatic_change_notifications = false;
            config2.sync_config->user->set_is_admin(false);
            auto r = Realm::get_shared_realm(config2);
            wait_for_download(*r);
            subscribe_to_all(r);

            // should have no objects as we don't have read permission
            CHECK(r->read_group().get_table("class_object")->size() == 0);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::None);
        }
    }
}
