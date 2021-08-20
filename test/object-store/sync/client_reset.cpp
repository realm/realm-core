////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#include <catch2/catch.hpp>

#include "collection_fixtures.hpp"
#include "util/baas_admin_api.hpp"
#include "sync/sync_test_utils.hpp"
#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <iostream>

namespace realm {

struct PartitionPair {
    std::string property_name;
    std::string value;
};

TableRef get_table(Realm& realm, StringData object_type)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_type);
}

Obj create_object(Realm& realm, StringData object_type, PartitionPair partition,
                  util::Optional<int64_t> primary_key = util::none)
{
    auto table = get_table(realm, object_type);
    REQUIRE(table);
    static int64_t pk = 0;
    FieldValues values = {{table->get_column_key(partition.property_name), partition.value}};
    return table->create_object_with_primary_key(primary_key ? *primary_key : pk++, std::move(values));
}


TEST_CASE("sync: client reset", "[client reset]") {
    if (!util::EventLoop::has_implementation())
        return;

    Schema schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
        {"link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
        {"pk link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
        {"link origin",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"link", PropertyType::Object | PropertyType::Nullable, "link target"},
             {"pk link", PropertyType::Object | PropertyType::Nullable, "pk link target"},
             {"list", PropertyType::Object | PropertyType::Array, "link target"},
             {"pk list", PropertyType::Object | PropertyType::Array, "pk link target"},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
    };
    const PartitionPair partition{"realm_id", "foo"};
#if REALM_ENABLE_AUTH_TESTS
    std::unique_ptr<app::GenericNetworkTransport> (*factory)() = [] {
        return std::unique_ptr<app::GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    REQUIRE(!base_url.empty());
    AppCreateConfig app_create_config = default_app_config(base_url);
    app_create_config.schema = schema;
    AppSession app_session = create_app(app_create_config);

    auto app_config = app::App::Config{app_session.client_app_id,
                                       factory,
                                       base_url,
                                       util::none,
                                       util::Optional<std::string>("A Local App Version"),
                                       util::none,
                                       "Object Store Platform Tests",
                                       "Object Store Platform Version Blah",
                                       "An sdk version"};

    TestSyncManager sync_manager(TestSyncManager::Config(app_config, &app_session), {});
    auto app = sync_manager.app();
    create_user_and_login(app);
    SyncTestFile config(app->current_user(), partition.value, schema);
    create_user_and_login(app);
    SyncTestFile config2(app->current_user(), partition.value, schema);
    auto make_reset = [&]() -> std::unique_ptr<reset_utils::TestClientReset> {
        return reset_utils::make_baas_client_reset(config, config2, sync_manager);
    };

#else
    TestSyncManager sync_manager;
    SyncTestFile config(sync_manager.app(), "default");
    SyncTestFile config2(sync_manager.app(), "default");
    config.schema = schema;
    auto make_reset = [&]() -> std::unique_ptr<reset_utils::TestClientReset> {
        return reset_utils::make_test_server_client_reset(config, config2, sync_manager);
    };
#endif

    SECTION("should trigger error callback when mode is manual") {
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        std::atomic<bool> called{false};
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(error.is_client_reset_requested());
            called = true;
        };

        make_reset()->run();

        util::EventLoop::main().run_until([&] {
            return called.load();
        });
    }

    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError) {
        FAIL("Error handler should not have been called");
    };

    SECTION("should discard local changeset when mode is discard") {
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        make_reset()
            ->on_post_reset([&](SharedRealm realm) {
                REQUIRE_THROWS(realm->refresh());
                CHECK(ObjectStore::table_for_object_type(realm->read_group(), "object")->begin()->get<Int>("value") ==
                      4);
                realm->close();
                SharedRealm r_after;
                REQUIRE_NOTHROW(r_after = Realm::get_shared_realm(config));
                CHECK(
                    ObjectStore::table_for_object_type(r_after->read_group(), "object")->begin()->get<Int>("value") ==
                    6);
            })
            ->run();
    }

    SECTION("should honor encryption key for downloaded Realm") {
        config.encryption_key.resize(64, 'a');
        config.sync_config->realm_encryption_key = std::array<char, 64>();
        config.sync_config->realm_encryption_key->fill('a');
        config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

        make_reset()
            ->on_post_reset([&](SharedRealm realm) {
                realm->close();
                SharedRealm r_after;
                REQUIRE_NOTHROW(r_after = Realm::get_shared_realm(config));
                CHECK(
                    ObjectStore::table_for_object_type(r_after->read_group(), "object")->begin()->get<Int>("value") ==
                    6);
            })
            ->run();
    }

    SECTION("add table in discarded transaction") {
        make_reset()
            ->setup([&](SharedRealm realm) {
                auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
                REQUIRE(!table);
            })
            ->make_local_changes([&](SharedRealm realm) {
                realm->update_schema(
                    {
                        {"object2",
                         {
                             {"_id", PropertyType::Int, Property::IsPrimary{true}},
                             {"value2", PropertyType::Int},
                             {"realm_id", PropertyType::String | PropertyType::Nullable},
                         }},
                    },
                    0, nullptr, nullptr, true);
                create_object(*realm, "object2", partition);
            })
            ->on_post_reset([&](SharedRealm realm) {
                // test local realm that changes were persisted
                REQUIRE_THROWS(realm->refresh());
                auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
                REQUIRE(table);
                REQUIRE(table->size() == 1);
                // test reset realm that changes were overwritten
                realm = Realm::get_shared_realm(config);
                table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
                REQUIRE(!table);
            })
            ->run();
    }

    SECTION("add column in discarded transaction") {
        make_reset()
            ->make_local_changes([](SharedRealm realm) {
                realm->update_schema(
                    {
                        {"object",
                         {
                             {"_id", PropertyType::Int, Property::IsPrimary{true}},
                             {"value2", PropertyType::Int},
                             {"realm_id", PropertyType::String | PropertyType::Nullable},
                         }},
                    },
                    0, nullptr, nullptr, true);
                ObjectStore::table_for_object_type(realm->read_group(), "object")->begin()->set("value2", 123);
            })
            ->on_post_reset([&](SharedRealm realm) {
                // test local realm that changes were persisted
                REQUIRE_THROWS(realm->refresh());
                auto table = ObjectStore::table_for_object_type(realm->read_group(), "object");
                REQUIRE(table->get_column_count() == 4);
                REQUIRE(table->begin()->get<Int>("value2") == 123);
                REQUIRE_THROWS(realm->refresh());
                // test resync'd realm that changes were overwritten
                realm = Realm::get_shared_realm(config);
                table = ObjectStore::table_for_object_type(realm->read_group(), "object");
                REQUIRE(table);
                REQUIRE(table->get_column_count() == 3);
                REQUIRE(!bool(table->get_column_key("value2")));
            })
            ->run();
    }

    SECTION("seamless loss") {
        config.cache = false;
        config.automatic_change_notifications = false;
        config.sync_config->client_resync_mode = ClientResyncMode::SeamlessLoss;

        Results results;
        Object object;
        CollectionChangeSet object_changes, results_changes;
        NotificationToken object_token, results_token;
        auto setup_listeners = [&](SharedRealm realm) {
            results = Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "object"))
                          .sort({{{"value", true}}});
            if (results.size() >= 1) {
                REQUIRE(results.get<Obj>(0).get<Int>("value") == 4);

                auto obj = *ObjectStore::table_for_object_type(realm->read_group(), "object")->begin();
                REQUIRE(obj.get<Int>("value") == 4);
                object = Object(realm, obj);
                object_token =
                    object.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                        REQUIRE_FALSE(err);
                        object_changes = std::move(changes);
                    });
            }
            results_token =
                results.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                    REQUIRE_FALSE(err);
                    results_changes = std::move(changes);
                });
        };
        std::unique_ptr<reset_utils::TestClientReset> test_reset = make_reset();

        SECTION("modify") {
            test_reset
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));

                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(object.obj().get<Int>("value") == 6);
                    REQUIRE_INDICES(results_changes.modifications, 0);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.modifications, 0);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        }

        SECTION("delete and insert new") {
            constexpr int64_t new_value = 42;
            test_reset
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    table->clear();
                    auto obj = create_object(*remote, "object", partition);
                    auto col = obj.get_table()->get_column_key("value");
                    obj.set(col, new_value);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == new_value);
                    CHECK(!object.is_valid());
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions, 0);
                    REQUIRE_INDICES(results_changes.deletions, 0);
                    REQUIRE_INDICES(object_changes.modifications);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions, 0);
                })
                ->run();
        }

        SECTION("delete and insert same pk is reported as modification") {
            constexpr int64_t new_value = 42;
            test_reset
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    Mixed orig_pk = table->begin()->get_primary_key();
                    table->clear();
                    auto obj = create_object(*remote, "object", partition, {orig_pk.get_int()});
                    REQUIRE(obj.get_primary_key() == orig_pk);
                    auto col = obj.get_table()->get_column_key("value");
                    obj.set(col, new_value);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == new_value);
                    CHECK(object.is_valid());
                    CHECK(object.obj().get<Int>("value") == new_value);
                    REQUIRE_INDICES(results_changes.modifications, 0);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.modifications, 0);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        }

        SECTION("insert in discarded transaction is deleted") {
            constexpr int64_t new_value = 42;
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    auto obj = create_object(*local, "object", partition);
                    auto col = obj.get_table()->get_column_key("value");
                    REQUIRE(table->size() == 2);
                    obj.set(col, new_value);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 2);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(object.is_valid());
                    CHECK(object.obj().get<Int>("value") == 6);
                    REQUIRE_INDICES(results_changes.modifications, 0);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions, 1);
                    REQUIRE_INDICES(object_changes.modifications, 0);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        }

        SECTION("delete in discarded transaction is recovered") {
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    table->clear();
                    REQUIRE(table->size() == 0);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 0);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(!object.is_valid());
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions, 0);
                    REQUIRE_INDICES(results_changes.deletions);
                })
                ->run();
        }

        SECTION("extra local table is removed") {
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    local->update_schema(
                        {
                            {"object2",
                             {
                                 {"_id", PropertyType::Int | PropertyType::Nullable, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                    create_object(*local, "object2", partition, {1});
                    create_object(*local, "object2", partition, {2});
                })
                ->on_post_reset([](SharedRealm realm) {
                    REQUIRE_THROWS_CONTAINING(realm->refresh(),
                                              "Unsupported schema changes were made by another client or process");
                })
                ->run();
        }

        SECTION("extra local column is removed") {
            test_reset
                ->make_local_changes([](SharedRealm local) {
                    local->update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"array", PropertyType::Int | PropertyType::Array},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                    auto table = ObjectStore::table_for_object_type(local->read_group(), "object");
                    table->begin()->set(table->get_column_key("value2"), 123);
                })
                ->on_post_reset([](SharedRealm realm) {
                    REQUIRE_THROWS_CONTAINING(realm->refresh(),
                                              "Unsupported schema changes were made by another client or process");
                })
                ->run();
        }

        SECTION("compatible schema changes in both remote and local transactions") {
            test_reset
                ->make_local_changes([](SharedRealm local) {
                    local->update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->make_remote_changes([](SharedRealm remote) {
                    remote->update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"link", PropertyType::Object | PropertyType::Nullable, "object"},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->on_post_reset([](SharedRealm realm) {
                    REQUIRE_NOTHROW(realm->refresh());
                    auto table = ObjectStore::table_for_object_type(realm->read_group(), "object2");
                    REQUIRE(table->get_column_count() == 3);
                    REQUIRE(bool(table->get_column_key("link")));
                })
                ->run();
        }

        SECTION("incompatible schema changes in remote and local transactions") {
            test_reset
                ->make_local_changes([](SharedRealm local) {
                    local->update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Float},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->make_remote_changes([](SharedRealm remote) {
                    remote->update_schema(
                        {
                            {"object",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->on_post_reset([](SharedRealm realm) {
                    REQUIRE_THROWS_WITH(
                        realm->refresh(),
                        Catch::Matchers::Contains("Property 'object.value2' has been changed from 'float' to 'int'"));
                })
                ->run();
        }

        SECTION("list operations") {
            ObjKey k0, k1, k2;
            test_reset->setup([&](SharedRealm realm) {
                k0 = create_object(*realm, "link target", partition).set("value", 1).get_key();
                k1 = create_object(*realm, "link target", partition).set("value", 2).get_key();
                k2 = create_object(*realm, "link target", partition).set("value", 3).get_key();
                Obj o = create_object(*realm, "link origin", partition);
                auto list = o.get_linklist(o.get_table()->get_column_key("list"));
                list.add(k0);
                list.add(k1);
                list.add(k2);
            });
            auto check_links = [&](auto& realm) {
                auto table = get_table(*realm, "link origin");
                REQUIRE(table->size() == 1);
                auto list = table->begin()->get_linklist(table->get_column_key("list"));
                REQUIRE(list.size() == 3);
                REQUIRE(list.get_object(0).template get<Int>("value") == 1);
                REQUIRE(list.get_object(1).template get<Int>("value") == 2);
                REQUIRE(list.get_object(2).template get<Int>("value") == 3);
            };

            SECTION("list insertions in local transaction") {
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        auto table = get_table(*local, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.add(k0);
                        list.insert(0, k2);
                        list.insert(0, k1);
                    })
                    ->on_post_reset([&](SharedRealm realm) {
                        REQUIRE_NOTHROW(realm->refresh());
                        check_links(realm);
                    })
                    ->run();
            }

            SECTION("list deletions in local transaction") {
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        auto table = get_table(*local, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.remove(1);
                    })
                    ->on_post_reset([&](SharedRealm realm) {
                        REQUIRE_NOTHROW(realm->refresh());
                        check_links(realm);
                    })
                    ->run();
            }

            SECTION("list clear in local transaction") {
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        auto table = get_table(*local, "link origin");
                        auto list = table->begin()->get_linklist(table->get_column_key("list"));
                        list.clear();
                    })
                    ->on_post_reset([&](SharedRealm realm) {
                        REQUIRE_NOTHROW(realm->refresh());
                        check_links(realm);
                    })
                    ->run();
            }
        }

        SECTION("conflicting primary key creations") {
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    table->clear();
                    create_object(*local, "object", partition, {1}).set("value", 4);
                    create_object(*local, "object", partition, {2}).set("value", 5);
                    create_object(*local, "object", partition, {3}).set("value", 6);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    table->clear();
                    create_object(*remote, "object", partition, {1}).set("value", 4);
                    create_object(*remote, "object", partition, {2}).set("value", 7);
                    create_object(*remote, "object", partition, {5}).set("value", 8);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 3);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 3);
                    // here we rely on results being sorted by "value"
                    CHECK(results.get<Obj>(0).get<Int>("_id") == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(results.get<Obj>(1).get<Int>("_id") == 2);
                    CHECK(results.get<Obj>(1).get<Int>("value") == 7);
                    CHECK(results.get<Obj>(2).get<Int>("_id") == 5);
                    CHECK(results.get<Obj>(2).get<Int>("value") == 8);
                    CHECK(object.is_valid());
                    REQUIRE_INDICES(results_changes.modifications, 1);
                    REQUIRE_INDICES(results_changes.insertions, 2);
                    REQUIRE_INDICES(results_changes.deletions, 2);
                    REQUIRE_INDICES(object_changes.modifications);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        }

        auto get_key_for_object_with_value = [&](TableRef table, int64_t value) -> ObjKey {
            REQUIRE(table);
            auto target = std::find_if(table->begin(), table->end(), [&](auto& it) -> bool {
                return it.template get<Int>("value") == value;
            });
            if (target == table->end()) {
                return {};
            }
            return target->get_key();
        };

        SECTION("link to remotely deleted object") {
            test_reset
                ->setup([&](SharedRealm realm) {
                    auto k0 = create_object(*realm, "link target", partition).set("value", 1).get_key();
                    create_object(*realm, "link target", partition).set("value", 2);
                    create_object(*realm, "link target", partition).set("value", 3);

                    Obj o = create_object(*realm, "link origin", partition);
                    o.set("link", k0);
                })
                ->make_local_changes([&](SharedRealm local) {
                    auto target_table = get_table(*local, "link target");
                    auto key_of_second_target = get_key_for_object_with_value(target_table, 2);
                    REQUIRE(key_of_second_target);
                    auto table = get_table(*local, "link origin");
                    table->begin()->set("link", key_of_second_target);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "link target");
                    auto key_of_second_target = get_key_for_object_with_value(table, 2);
                    table->remove_object(key_of_second_target);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(realm->refresh());
                    auto origin = get_table(*realm, "link origin");
                    auto target = get_table(*realm, "link target");
                    REQUIRE(origin->size() == 1);
                    REQUIRE(target->size() == 2);
                    REQUIRE(get_key_for_object_with_value(target, 1));
                    REQUIRE(get_key_for_object_with_value(target, 3));
                    auto key = origin->begin()->get<ObjKey>("link");
                    auto obj = target->get_object(key);
                    REQUIRE(obj.get<Int>("value") == 1);
                })
                ->run();
        }

        SECTION("add remotely deleted object to list") {
            ObjKey k0, k1, k2;
            test_reset
                ->setup([&](SharedRealm realm) {
                    k0 = create_object(*realm, "link target", partition).set("value", 1).get_key();
                    k1 = create_object(*realm, "link target", partition).set("value", 2).get_key();
                    k2 = create_object(*realm, "link target", partition).set("value", 3).get_key();
                    Obj o = create_object(*realm, "link origin", partition);
                    o.get_linklist("list").add(k0);
                })
                ->make_local_changes([&](SharedRealm local) {
                    auto key = get_key_for_object_with_value(get_table(*local, "link target"), 2);
                    auto table = get_table(*local, "link origin");
                    auto list = table->begin()->get_linklist("list");
                    list.add(key);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "link target");
                    auto key = get_key_for_object_with_value(table, 2);
                    REQUIRE(key);
                    table->remove_object(key);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(realm->refresh());
                    auto table = get_table(*realm, "link origin");
                    auto target_table = get_table(*realm, "link target");
                    REQUIRE(table->size() == 1);
                    REQUIRE(target_table->size() == 2);
                    REQUIRE(get_key_for_object_with_value(target_table, 1));
                    REQUIRE(get_key_for_object_with_value(target_table, 3));
                    auto list = table->begin()->get_linklist("list");
                    REQUIRE(list.size() == 1);
                    REQUIRE(list.get_object(0).get<Int>("value") == 1);
                })
                ->run();
        }
    }
}

namespace cf = realm::collection_fixtures;
TEMPLATE_TEST_CASE("client reset types", "[client reset][seamless loss]", cf::MixedVal, cf::Int, cf::Bool, cf::Float,
                   cf::Double, cf::String, cf::Binary, cf::Date, cf::OID, cf::Decimal, cf::UUID,
                   cf::BoxedOptional<cf::Int>, cf::BoxedOptional<cf::Bool>, cf::BoxedOptional<cf::Float>,
                   cf::BoxedOptional<cf::Double>, cf::BoxedOptional<cf::OID>, cf::BoxedOptional<cf::UUID>,
                   cf::UnboxedOptional<cf::String>, cf::UnboxedOptional<cf::Binary>, cf::UnboxedOptional<cf::Date>,
                   cf::UnboxedOptional<cf::Decimal>)
{
    auto values = TestType::values();
    using T = typename TestType::Type;

    if (!util::EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
        {"test type",
         {{"_id", PropertyType::Int, Property::IsPrimary{true}},
          {"value", TestType::property_type()},
          {"list", PropertyType::Array | TestType::property_type()},
          {"dictionary", PropertyType::Dictionary | TestType::property_type()},
          {"set", PropertyType::Set | TestType::property_type()}}},
    };

    SyncTestFile config2(init_sync_manager.app(), "default");

    config.cache = false;
    config.automatic_change_notifications = false;
    config.sync_config->client_resync_mode = ClientResyncMode::SeamlessLoss;

    Results results;
    Object object;
    CollectionChangeSet object_changes, results_changes;
    NotificationToken object_token, results_token;
    auto setup_listeners = [&](SharedRealm realm) {
        results = Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "test type"))
                      .sort({{{"_id", true}}});
        if (results.size() >= 1) {
            auto obj = *ObjectStore::table_for_object_type(realm->read_group(), "test type")->begin();
            object = Object(realm, obj);
            object_token = object.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
                REQUIRE_FALSE(err);
                object_changes = std::move(changes);
            });
        }
        results_token = results.add_notification_callback([&](CollectionChangeSet changes, std::exception_ptr err) {
            REQUIRE_FALSE(err);
            results_changes = std::move(changes);
        });
    };

    auto check_list = [&](Obj obj, std::vector<T> expected) {
        ColKey col = obj.get_table()->get_column_key("list");
        auto actual = obj.get_list_values<T>(col);
        REQUIRE(actual == expected);
    };

    auto check_dictionary = [&](Obj obj, std::vector<std::pair<std::string, Mixed>> expected) {
        ColKey col = obj.get_table()->get_column_key("dictionary");
        Dictionary dict = obj.get_dictionary(col);
        REQUIRE(dict.size() == expected.size());
        for (auto pair : expected) {
            auto it = dict.find(pair.first);
            REQUIRE(it != dict.end());
            REQUIRE((*it).second == pair.second);
        }
    };

    auto check_set = [&](Obj obj, std::vector<Mixed> expected) {
        ColKey col = obj.get_table()->get_column_key("set");
        SetBasePtr set = obj.get_setbase_ptr(col);
        REQUIRE(set->size() == expected.size());
        for (auto value : expected) {
            auto ndx = set->find_any(value);
            REQUIRE(ndx != realm::not_found);
        }
    };

    // The following can be used to perform the client reset proper, but these tests
    // are only intended to check the transfer_group logic for different types,
    // so to save local test time, we call it directly instead.
#if TEST_DURATION > 0
    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_test_server_client_reset(config, config2, init_sync_manager);
#else
    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);
#endif

    SECTION("property") {
        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        int64_t pk_val = 0;
        T initial_value = values[0];

        auto set_value = [](SharedRealm realm, T value) {
            auto table = get_table(*realm, "test type");
            REQUIRE(table);
            REQUIRE(table->size() == 1);
            ColKey col = table->get_column_key("value");
            table->begin()->set<T>(col, value);
        };
        auto check_value = [](Obj obj, T value) {
            ColKey col = obj.get_table()->get_column_key("value");
            REQUIRE(obj.get<T>(col) == value);
        };

        test_reset->setup([&pk_val, &initial_value](SharedRealm realm) {
            auto table = get_table(*realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("value");
            obj.set<T>(col, initial_value);
        });

        auto reset_property = [&](T local_state, T remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    set_value(local_realm, local_state);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    set_value(remote_realm, remote_state);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
                    CHECK(object.is_valid());
                    check_value(results.get<Obj>(0), local_state);
                    check_value(object.obj(), local_state);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));

                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    check_value(results.get<Obj>(0), remote_state);
                    check_value(object.obj(), remote_state);
                    if (local_state == remote_state) {
                        REQUIRE_INDICES(results_changes.modifications);
                        REQUIRE_INDICES(object_changes.modifications);
                    }
                    else {
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                    }
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        };

        SECTION("modify") {
            reset_property(values[0], values[1]);
        }
        SECTION("modify opposite") {
            reset_property(values[1], values[0]);
        }
        // verify whatever other test values are provided (type bool only has two)
        for (size_t i = 2; i < values.size(); ++i) {
            SECTION(util::format("modify to value: %1", i)) {
                reset_property(values[0], values[i]);
            }
        }
    }

    SECTION("lists") {
        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        int64_t pk_val = 0;
        // MSVC doesn't seem to automatically capture a templated variable so
        // the following lambda is explicit about it's captures
        T initial_list_value = values[0];
        test_reset->setup([&pk_val, &initial_list_value](SharedRealm realm) {
            auto table = get_table(*realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("list");
            obj.template set_list_values<T>(col, {initial_list_value});
        });

        auto reset_list = [&](std::vector<T> local_state, std::vector<T> remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    auto table = get_table(*local_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("list");
                    table->begin()->template set_list_values<T>(col, local_state);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    auto table = get_table(*remote_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("list");
                    table->begin()->template set_list_values<T>(col, remote_state);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
                    CHECK(object.is_valid());
                    check_list(results.get<Obj>(0), local_state);
                    check_list(object.obj(), local_state);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));

                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    check_list(results.get<Obj>(0), remote_state);
                    check_list(object.obj(), remote_state);
                    if (local_state == remote_state) {
                        REQUIRE_INDICES(results_changes.modifications);
                        REQUIRE_INDICES(object_changes.modifications);
                    }
                    else {
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                    }
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        };

        SECTION("modify") {
            reset_list({values[0]}, {values[1]});
        }
        SECTION("modify opposite") {
            reset_list({values[1]}, {values[0]});
        }
        SECTION("empty remote") {
            reset_list({values[1], values[0], values[1]}, {});
        }
        SECTION("empty local") {
            reset_list({}, {values[0], values[1]});
        }
        SECTION("empty both") {
            reset_list({}, {});
        }
        SECTION("equal suffix") {
            reset_list({values[0], values[0], values[1]}, {values[0], values[1]});
        }
        SECTION("equal prefix") {
            reset_list({values[0]}, {values[0], values[1], values[1]});
        }
        SECTION("equal lists") {
            reset_list({values[0]}, {values[0]});
        }
        SECTION("equal middle") {
            reset_list({values[0], values[1], values[0]}, {values[1], values[1], values[1]});
        }
    }

    SECTION("dictionary") {
        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        int64_t pk_val = 0;
        std::string dict_key = "hello";
        test_reset->setup([&](SharedRealm realm) {
            auto table = get_table(*realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("dictionary");
            Dictionary dict = obj.get_dictionary(col);
            dict.insert(dict_key, Mixed{values[0]});
        });

        auto reset_dictionary = [&](std::vector<std::pair<std::string, Mixed>> local_state,
                                    std::vector<std::pair<std::string, Mixed>> remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    auto table = get_table(*local_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto pair : local_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end(); ++it) {
                        auto found = std::any_of(local_state.begin(), local_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            dict.erase(it);
                        }
                    }
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    auto table = get_table(*remote_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto pair : remote_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end(); ++it) {
                        auto found = std::any_of(remote_state.begin(), remote_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            dict.erase(it);
                        }
                    }
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
                    CHECK(object.is_valid());
                    check_dictionary(results.get<Obj>(0), local_state);
                    check_dictionary(object.obj(), local_state);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    check_dictionary(results.get<Obj>(0), remote_state);
                    check_dictionary(object.obj(), remote_state);
                    if (local_state == remote_state) {
                        REQUIRE_INDICES(results_changes.modifications);
                        REQUIRE_INDICES(object_changes.modifications);
                    }
                    else {
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                    }
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        };

        SECTION("modify") {
            reset_dictionary({{dict_key, Mixed{values[0]}}}, {{dict_key, Mixed{values[1]}}});
        }
        SECTION("modify opposite") {
            reset_dictionary({{dict_key, Mixed{values[1]}}}, {{dict_key, Mixed{values[0]}}});
        }
        SECTION("empty remote") {
            reset_dictionary({{dict_key, Mixed{values[1]}}}, {});
        }
        SECTION("empty local") {
            reset_dictionary({}, {{dict_key, Mixed{values[1]}}});
        }
        SECTION("extra values on remote") {
            reset_dictionary({{dict_key, Mixed{values[0]}}}, {{dict_key, Mixed{values[0]}},
                                                              {"world", Mixed{values[1]}},
                                                              {"foo", Mixed{values[1]}},
                                                              {"aaa", Mixed{values[0]}}});
        }
    }

    SECTION("set") {
        int64_t pk_val = 0;

        auto reset_set = [&](std::vector<Mixed> local_state, std::vector<Mixed> remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    auto table = get_table(*local_realm, "test type");
                    REQUIRE(table);
                    ColKey col = table->get_column_key("set");
                    SetBasePtr set = table->begin()->get_setbase_ptr(col);
                    for (size_t i = set->size(); i > 0; --i) {
                        Mixed si = set->get_any(i - 1);
                        if (std::find(local_state.begin(), local_state.end(), si) == local_state.end()) {
                            set->erase_any(si);
                        }
                    }
                    for (auto e : local_state) {
                        set->insert_any(e);
                    }
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    auto table = get_table(*remote_realm, "test type");
                    REQUIRE(table);
                    ColKey col = table->get_column_key("set");
                    SetBasePtr set = table->begin()->get_setbase_ptr(col);
                    for (size_t i = set->size(); i > 0; --i) {
                        Mixed si = set->get_any(i - 1);
                        if (std::find(remote_state.begin(), remote_state.end(), si) == remote_state.end()) {
                            set->erase_any(si);
                        }
                    }
                    for (auto e : remote_state) {
                        set->insert_any(e);
                    }
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("_id") == pk_val);
                    CHECK(object.is_valid());
                    check_set(results.get<Obj>(0), local_state);
                    check_set(object.obj(), local_state);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    check_set(results.get<Obj>(0), remote_state);
                    check_set(object.obj(), remote_state);
                    if (local_state == remote_state) {
                        REQUIRE_INDICES(results_changes.modifications);
                        REQUIRE_INDICES(object_changes.modifications);
                    }
                    else {
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                    }
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                })
                ->run();
        };

        REQUIRE(values.size() >= 2);
        REQUIRE(values[0] != values[1]);
        test_reset->setup([&](SharedRealm realm) {
            auto table = get_table(*realm, "test type");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            ColKey col = table->get_column_key("set");
            SetBasePtr set = obj.get_setbase_ptr(col);
            set->insert_any(Mixed{values[0]});
        });

        SECTION("modify") {
            reset_set({Mixed{values[0]}}, {Mixed{values[1]}});
        }
        SECTION("modify opposite") {
            reset_set({Mixed{values[1]}}, {Mixed{values[0]}});
        }
        SECTION("empty remote") {
            reset_set({Mixed{values[1]}, Mixed{values[0]}}, {});
        }
        SECTION("empty local") {
            reset_set({}, {Mixed{values[0]}, Mixed{values[1]}});
        }
        SECTION("empty both") {
            reset_set({}, {});
        }
        SECTION("equal suffix") {
            reset_set({Mixed{values[0]}, Mixed{values[1]}}, {Mixed{values[1]}});
        }
        SECTION("equal prefix") {
            reset_set({Mixed{values[0]}}, {Mixed{values[1]}, Mixed{values[0]}});
        }
        SECTION("equal lists") {
            reset_set({Mixed{values[0]}, Mixed{values[1]}}, {Mixed{values[0]}, Mixed{values[1]}});
        }
    }
}


#if REALM_ENABLE_AUTH_TESTS

TEST_CASE("app: client reset integration", "[sync][app][client reset]") {
    std::unique_ptr<app::GenericNetworkTransport> (*factory)() = [] {
        return std::unique_ptr<app::GenericNetworkTransport>(new IntTestTransport);
    };
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());
    const std::string partition = "foo";

    Schema schema = {{"source",
                      {
                          {valid_pk_name, PropertyType::ObjectId | PropertyType::Nullable, true},
                          {"source_int", PropertyType::Int},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }},
                     {"dest",
                      {
                          {valid_pk_name, PropertyType::ObjectId | PropertyType::Nullable, true},
                          {"dest_int", PropertyType::Int},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }},
                     {"object",
                      {
                          {"_id", PropertyType::Int, Property::IsPrimary{true}},
                          {"value", PropertyType::Int},
                          {"realm_id", PropertyType::String | PropertyType::Nullable},
                      }}};

    AppCreateConfig app_create_config = default_app_config(base_url);
    app_create_config.schema = schema;
    AppSession app_session = create_app(app_create_config);

    auto app_config = app::App::Config{app_session.client_app_id,
                                       factory,
                                       base_url,
                                       util::none,
                                       util::Optional<std::string>("A Local App Version"),
                                       util::none,
                                       "Object Store Platform Tests",
                                       "Object Store Platform Version Blah",
                                       "An sdk version"};

    auto base_path = util::make_temp_dir() + app_config.app_id;
    util::try_remove_dir_recursive(base_path);
    util::try_make_dir(base_path);

    auto setup_and_get_config = [&base_path, &schema, &partition](std::shared_ptr<app::App> app,
                                                                  std::string local_path) -> realm::Realm::Config {
        realm::Realm::Config config;
        config.sync_config = std::make_shared<realm::SyncConfig>(app->current_user(), bson::Bson(partition));
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        config.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
            std::cerr << error.message << std::endl;
            abort();
        };
        config.schema_version = 1;
        config.path = base_path + "/" + local_path;
        config.schema = schema;
        return config;
    };

    //    auto get_source_objects = [&](realm::SharedRealm r, std::shared_ptr<SyncSession> session) -> Results {
    //        wait_for_sync_changes(session);
    //        return realm::Results(r, r->read_group().get_table("class_source"));
    //    };
    //    CppContext c;
    //    int64_t counter = 0;
    //    auto create_one_source_object = [&](realm::SharedRealm r) {
    //        r->begin_transaction();
    //        auto object = Object::create(c, r, "source",
    //                                     util::Any(realm::AnyDict{{valid_pk_name, util::Any(ObjectId::gen())},
    //                                                              {"source_int", counter++},
    //                                                              {"realm_id", std::string(partition)}}),
    //                                     CreatePolicy::ForceCreate);
    //
    //        r->commit_transaction();
    //    };
    //
    //    auto create_one_dest_object = [&](realm::SharedRealm r) -> ObjLink {
    //        r->begin_transaction();
    //        auto obj = Object::create(c, r, "dest",
    //                                  util::Any(realm::AnyDict{{valid_pk_name, util::Any(ObjectId::gen())},
    //                                                           {"dest_int", counter++},
    //                                                           {"realm_id", std::string(partition)}}),
    //                                  CreatePolicy::ForceCreate);
    //        r->commit_transaction();
    //        return ObjLink{obj.obj().get_table()->get_key(), obj.obj().get_key()};
    //    };
    //
    //    auto require_links_to_match_ids = [&](std::vector<Obj> links, std::vector<int64_t> expected) {
    //        std::vector<int64_t> actual;
    //        for (auto obj : links) {
    //            actual.push_back(obj.get<Int>("dest_int"));
    //        }
    //        std::sort(actual.begin(), actual.end());
    //        std::sort(expected.begin(), expected.end());
    //        REQUIRE(actual == expected);
    //    };


    SECTION("manual client reset should trigger the error callback") {
        TestSyncManager sync_manager(TestSyncManager::Config(app_config, &app_session), {});
        auto app = sync_manager.app();

        create_user_and_login(app);
        auto config = setup_and_get_config(app, "r1.realm");
        auto config2 = setup_and_get_config(app, "r2.realm");
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        std::atomic<bool> called{false};
        config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            // ignore "end of input" and other sync errors that might occur when the sync service is killed
            if (error.is_client_reset_requested()) {
                called = true;
            }
        };
        {
            auto r1 = realm::Realm::get_shared_realm(config);
            auto session1 = app->current_user()->session_for_on_disk_path(r1->config().path);
            wait_for_download(*r1);
        }

        reset_utils::make_baas_client_reset(config, config2, sync_manager)->run();

        auto r1 = realm::Realm::get_shared_realm(config);
        auto session1 = app->current_user()->session_for_on_disk_path(r1->config().path);

        util::EventLoop::main().run_until([&] {
            return called.load();
        });
    }
}

#endif // REALM_ENABLE_AUTH_TESTS

} // namespace realm
