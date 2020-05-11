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

#include "sync_test_utils.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"
#include <unistd.h>

using namespace realm;

constexpr const char* result_sets_type_name = "__ResultSets";

static void update_schema(Group& group, Property matches_property)
{
    Schema current_schema;
    std::string table_name = ObjectStore::table_name_for_object_type(result_sets_type_name);
    if (group.has_table(table_name))
        current_schema = {ObjectSchema{group, result_sets_type_name, TableKey()}};

    Schema desired_schema({
        ObjectSchema(result_sets_type_name, {
            {"name", PropertyType::String},
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
    ObjectSchema schema{r->read_group(), result_sets_type_name, TableKey()};

    CppContext context;
    auto obj = Object::create<util::Any>(context, r, schema, AnyDict{
        {"name", ""s},
        {"matches_property", "object_matches"s},
        {"query", "TRUEPREDICATE"s},
        {"status", int64_t(0)},
        {"error_message", ""s},
        {"query_parse_counter", int64_t(0)},
        {"matches_count", int64_t(0)},
        {"created_at", Timestamp(0, 0)},
        {"updated_at", Timestamp(0, 0)},
        {"expires_at", Timestamp()},
        {"time_to_live", {}},
    });

    r->commit_transaction();

    while (any_cast<int64_t>(obj.get_property_value<util::Any>(context, "status")) != 1) {
        wait_for_download(*r);
        r->refresh();
    }
}

TEST_CASE("Object-level Permissions") {
    TestSyncManager init_sync_manager;

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

    SECTION("Query-based sync Realms") {
        SECTION("permit all operations prior to first sync") {
            config.sync_config->is_partial = true;

            auto r = Realm::get_shared_realm(config);
            auto& table = create_object(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(*table.begin()) == ComputedPrivileges::AllObject);
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
            CHECK(r->get_privileges(*table.begin()) == ComputedPrivileges::AllObject);
        }

        SECTION("permit all operations on a downloaded Realm created as a Full Realm when logged in as an admin") {
            server.start();
            {
                auto r = Realm::get_shared_realm(config);
                create_object(r);
                wait_for_upload(*r);
            }

            SyncTestFile config2{server, "default", true};
            config2.automatic_change_notifications = false;
            auto r = Realm::get_shared_realm(config2);
            wait_for_download(*r);
            subscribe_to_all(r);

            CHECK(r->get_privileges() == ComputedPrivileges::AllRealm);
            CHECK(r->get_privileges("object") == ComputedPrivileges::AllClass);
            CHECK(r->get_privileges(*r->read_group().get_table("class_object")->begin()) == ComputedPrivileges::AllObject);
        }

        SECTION("permit nothing on pre-existing types in a downloaded Realm created as a Full Realm") {
            server.start();
            {
                auto r = Realm::get_shared_realm(config);
                create_object(r);
                wait_for_upload(*r);
            }

            SyncTestFile config2{server, "default", true};
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

        SECTION("automatically add newly created users to 'everyone'") {
            using namespace std::string_literals;

            config.schema = Schema{
                {"__User", {
                    {"id", PropertyType::String, Property::IsPrimary{true}}
                }},
            };
            config.sync_config->is_partial = true;
            auto r = Realm::get_shared_realm(config);
            r->begin_transaction();

            CppContext c;
            auto user = Object::create<util::Any>(c, r, *r->schema().find("__User"), AnyDict{{"id", "test user"s}});

            auto role_table = r->read_group().get_table("class___Role");
            REQUIRE(role_table);
            auto obj_key = role_table->find_first_string(role_table->get_column_key("name"), "everyone");
            REQUIRE(obj_key);
            auto everyone = role_table->get_object(obj_key);
            REQUIRE(everyone.get_linklist(role_table->get_column_key("members")).find_first(user.obj().get_key()) != npos);

            r->commit_transaction();
        }

        SECTION("automatically create private roles for newly-created users") {
            using namespace std::string_literals;

            config.schema = Schema{
                {"__User", {
                    {"id", PropertyType::String, Property::IsPrimary{true}}
                }},
            };
            config.sync_config->is_partial = true;
            auto r = Realm::get_shared_realm(config);
            r->begin_transaction();

            auto validate_user_role = [](const Object& user) {
                auto user_table = user.obj().get_table();
                REQUIRE(user_table);
                ObjKey key = user.obj().get<ObjKey>(user_table->get_column_key("role"));
                REQUIRE(key);

                auto role_table = user.realm()->read_group().get_table("class___Role");
                REQUIRE(role_table);
                auto role = role_table->get_object(key);
                auto members = role.get_linklist(role_table->get_column_key("members"));
                REQUIRE(members.size() == 1);
                REQUIRE(members.find_first(user.obj().get_key()) != npos);
            };

            SECTION("logged-in user") {
                auto user_table = r->read_group().get_table("class___User");
                REQUIRE(user_table);
                REQUIRE(user_table->size() == 1);
                validate_user_role(Object(r, "__User", 0));
            }

            SECTION("manually created user") {
                CppContext c;
                auto user = Object::create<util::Any>(c, r, *r->schema().find("__User"), AnyDict{{"id", "test user"s}});
                validate_user_role(user);
            }
            r->commit_transaction();
        }
    }

    SECTION("schema change error reporting") {
        config.sync_config->is_partial = true;
        // Create the Realm with an admin user
        server.start();
        {
            auto r = Realm::get_shared_realm(config);
            create_object(r);

            // FIXME: required due to https://github.com/realm/realm-sync/issues/2071
            wait_for_upload(*r);
            wait_for_download(*r);

            // Revoke modifySchema permission for all users
            r->begin_transaction();
            TableRef permission_table = r->read_group().get_table("class___Permission");
            auto col = permission_table->get_column_key("canModifySchema");
            for (auto& o : *permission_table)
                o.set(col, false);
            r->commit_transaction();
            wait_for_upload(*r);
        }

        SyncTestFile nonadmin{server, "default", true, "user2"};
        nonadmin.automatic_change_notifications = false;
        nonadmin.sync_config->user->set_is_admin(false);
        auto bind_session_handler = nonadmin.sync_config->bind_session_handler;
        nonadmin.sync_config->bind_session_handler = [](auto, auto, auto) { };
        auto log_in = [&](auto& realm) {
            auto session = SyncManager::shared().get_session(nonadmin.path, *nonadmin.sync_config);
            bind_session_handler("", *nonadmin.sync_config, session);
            wait_for_upload(realm);
            wait_for_download(realm);
        };

        SECTION("reverted column insertion") {
            nonadmin.schema = Schema{
                {"object", {
                    {"value", PropertyType::Int},
                    {"value 2", PropertyType::Int}
                }},
            };
            auto r = Realm::get_shared_realm(nonadmin);
            r->invalidate();

            SECTION("no active read transaction") {
                log_in(*r);
                REQUIRE_THROWS_WITH(r->read_group(),
                                    Catch::Matchers::Contains("Property 'object.value 2' has been removed."));
            }

            SECTION("notify()") {
                r->read_group();
                log_in(*r);
                REQUIRE_THROWS_WITH(r->notify(),
                                    Catch::Matchers::Contains("Property 'object.value 2' has been removed."));
            }

            SECTION("refresh()") {
                r->read_group();
                log_in(*r);
                REQUIRE_THROWS_WITH(r->refresh(),
                                    Catch::Matchers::Contains("Property 'object.value 2' has been removed."));
            }

            SECTION("begin_transaction()") {
                r->read_group();
                log_in(*r);
                REQUIRE_THROWS_WITH(r->begin_transaction(),
                                    Catch::Matchers::Contains("Property 'object.value 2' has been removed."));
            }
        }

        SECTION("reverted table insertion") {
            nonadmin.schema = Schema{
                {"object", {
                    {"value", PropertyType::Int},
                }},
                {"object 2", {
                    {"value", PropertyType::Int},
                }},
            };
            auto r = Realm::get_shared_realm(nonadmin);
            r->read_group();
            log_in(*r);
            REQUIRE_THROWS_WITH(r->notify(),
                                Catch::Matchers::Contains("Class 'object 2' has been removed."));
        }
    }
}
