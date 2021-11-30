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
#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/sync/noinst/client_reset_operation.hpp>

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

struct ThreadSafeSyncError {
    void operator=(const realm::SyncError& e)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_error = e;
    }
    operator bool() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return bool(m_error);
    }
    realm::util::Optional<realm::SyncError> value() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_error;
    }

private:
    mutable std::mutex m_mutex;
    realm::util::Optional<realm::SyncError> m_error;
};

namespace Catch {
template <>
struct StringMaker<ThreadSafeSyncError> {
    static std::string convert(const ThreadSafeSyncError& err)
    {
        auto value = err.value();
        if (!value) {
            return "No SyncError";
        }
        return realm::util::format("SyncError(%1), is_fatal: %2, with message: '%3'", value->error_code,
                                   value->is_fatal, value->message);
    }
};
} // namespace Catch

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
    static int64_t pk = 1; // TestClientReset creates an object with pk 0 so start with something else
    FieldValues values = {{table->get_column_key(partition.property_name), partition.value}};
    return table->create_object_with_primary_key(primary_key ? *primary_key : pk++, std::move(values));
}


TEST_CASE("sync: client reset", "[client reset]") {
    if (!util::EventLoop::has_implementation())
        return;

    const PartitionPair partition{"realm_id", "foo"};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"pk link target",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"link origin",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"link", PropertyType::Object | PropertyType::Nullable, "link target"},
             {"pk link", PropertyType::Object | PropertyType::Nullable, "pk link target"},
             {"list", PropertyType::Object | PropertyType::Array, "link target"},
             {"pk list", PropertyType::Object | PropertyType::Array, "pk link target"},
             partition_prop,
         }},
    };
#if REALM_ENABLE_AUTH_TESTS
    std::string base_url = get_base_url();
    REQUIRE(!base_url.empty());
    auto server_app_config = minimal_app_config(base_url, "client_reset_tests", schema);
    server_app_config.partition_key = partition_prop;
    AppSession app_session = create_app(server_app_config);
    auto app_config = get_config(instance_of<SynchronousTestTransport>, app_session);

    TestSyncManager sync_manager(TestSyncManager::Config(app_config, &app_session), {});
    auto app = sync_manager.app();
    auto get_valid_config = [&]() -> SyncTestFile {
        create_user_and_log_in(app);
        return SyncTestFile(app->current_user(), partition.value, schema);
    };
    SyncTestFile local_config = get_valid_config();
    SyncTestFile remote_config = get_valid_config();
    auto make_reset = [&](Realm::Config config_local,
                          Realm::Config config_remote) -> std::unique_ptr<reset_utils::TestClientReset> {
        return reset_utils::make_baas_client_reset(config_local, config_remote, sync_manager);
    };

#else
    TestSyncManager sync_manager;
    auto get_valid_config = [&]() -> SyncTestFile {
        return SyncTestFile(sync_manager.app(), "default");
    };
    SyncTestFile local_config = get_valid_config();
    local_config.schema = schema;
    SyncTestFile remote_config = get_valid_config();
    remote_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        CAPTURE(err.message);
        CAPTURE(remote_config.path);
        // There is a race in the test code of the sync test server where somehow the
        // remote Realm is also reset sometimes. We ignore it as it shouldn't affect the result.
    };
    auto make_reset = [&](Realm::Config config_local,
                          Realm::Config config_remote) -> std::unique_ptr<reset_utils::TestClientReset> {
        return reset_utils::make_test_server_client_reset(config_local, config_remote, sync_manager);
    };
#endif

    // this is just for ease of debugging
    local_config.path = local_config.path + ".local";
    remote_config.path = remote_config.path + ".remote";

    SECTION("should trigger error callback when mode is manual") {
        local_config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        ThreadSafeSyncError err;
        local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            err = error;
        };

        make_reset(local_config, remote_config)
            ->on_post_reset([&](SharedRealm) {
                util::EventLoop::main().run_until([&] {
                    return bool(err);
                });
            })
            ->run();

        REQUIRE(err);
        REQUIRE(err.value()->is_client_reset_requested());
    }

    local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        CAPTURE(err.message);
        CAPTURE(local_config.path);
        FAIL("Error handler should not have been called");
    };

    SECTION("discard local") {
        local_config.cache = false;
        local_config.automatic_change_notifications = false;
        local_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        const std::string fresh_path = realm::_impl::ClientResetOperation::get_fresh_path_for(local_config.path);
        size_t before_callback_invoctions = 0;
        size_t after_callback_invocations = 0;
        std::mutex mtx;
        local_config.sync_config->notify_before_client_reset = [&](SharedRealm before) {
            std::lock_guard<std::mutex> lock(mtx);
            ++before_callback_invoctions;
            REQUIRE(before);
            REQUIRE(before->is_frozen());
            REQUIRE(before->read_group().get_table("class_object"));
            REQUIRE(before->config().path == local_config.path);
            REQUIRE(util::File::exists(local_config.path));
        };
        local_config.sync_config->notify_after_client_reset = [&](SharedRealm before, SharedRealm after) {
            std::lock_guard<std::mutex> lock(mtx);
            ++after_callback_invocations;
            REQUIRE(before);
            REQUIRE(before->is_frozen());
            REQUIRE(before->read_group().get_table("class_object"));
            REQUIRE(before->config().path == local_config.path);
            REQUIRE(after);
            REQUIRE(!after->is_frozen());
            REQUIRE(after->read_group().get_table("class_object"));
            REQUIRE(after->config().path == local_config.path);
            REQUIRE(after->current_transaction_version() > before->current_transaction_version());
        };

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
        std::unique_ptr<reset_utils::TestClientReset> test_reset = make_reset(local_config, remote_config);

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

                    CHECK(before_callback_invoctions == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(object.obj().get<Int>("value") == 6);
                    REQUIRE_INDICES(results_changes.modifications, 0);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.modifications, 0);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                    // make sure that the reset operation has cleaned up after itself
                    REQUIRE(util::File::exists(local_config.path));
                    REQUIRE_FALSE(util::File::exists(fresh_path));
                })
                ->run();

            SECTION("a Realm can be reset twice") {
                // keep the Realm to reset (config) the same, but change out the remote (config2)
                // to a new path because otherwise it will be reset as well which we don't want
                SyncTestFile config3 = get_valid_config();
                test_reset = make_reset(local_config, config3);
                test_reset
                    ->setup([&](SharedRealm realm) {
                        // after a reset we already start with a value of 6
                        TableRef table = get_table(*realm, "object");
                        REQUIRE(table->size() == 1);
                        REQUIRE(table->begin()->get<Int>("value") == 6);
                        REQUIRE_NOTHROW(advance_and_notify(*object.get_realm()));
                        CHECK(object.obj().get<Int>("value") == 6);
                        object_changes = {};
                        results_changes = {};
                    })
                    ->on_post_local_changes([&](SharedRealm) {
                        // advance the object's realm because the one passed here is different
                        REQUIRE_NOTHROW(advance_and_notify(*object.get_realm()));
                        // 6 -> 4
                        CHECK(results.size() == 1);
                        CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                        CHECK(object.obj().get<Int>("value") == 4);
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(results_changes.insertions);
                        REQUIRE_INDICES(results_changes.deletions);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.insertions);
                        REQUIRE_INDICES(object_changes.deletions);
                        object_changes = {};
                        results_changes = {};
                    })
                    ->on_post_reset([&](SharedRealm) {
                        REQUIRE_NOTHROW(advance_and_notify(*object.get_realm()));
                        CHECK(before_callback_invoctions == 2);
                        CHECK(after_callback_invocations == 2);
                        // 4 -> 6
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
        }

        SECTION("an interrupted reset can recover on the next session") {
            struct SessionInterruption : public std::runtime_error {
                using std::runtime_error::runtime_error;
            };
            try {
                test_reset
                    ->on_post_local_changes([&](SharedRealm) {
                        throw SessionInterruption("fake interruption during reset");
                    })
                    ->run();
            }
            catch (const SessionInterruption&) {
                REQUIRE(before_callback_invoctions == 0);
                REQUIRE(after_callback_invocations == 0);
                test_reset.reset();
                auto realm = Realm::get_shared_realm(local_config);
                timed_sleeping_wait_for(
                    [&]() -> bool {
                        std::lock_guard<std::mutex> lock(mtx);
                        realm->begin_transaction();
                        TableRef table = get_table(*realm, "object");
                        REQUIRE(table);
                        REQUIRE(table->size() == 1);
                        auto col = table->get_column_key("value");
                        int64_t value = table->begin()->get<Int>(col);
                        realm->cancel_transaction();
                        return value == 6;
                    },
                    std::chrono::seconds(20));
            }
            auto session = sync_manager.app()->sync_manager()->get_existing_session(local_config.path);
            if (session) {
                session->shutdown_and_wait();
            }
            {
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(before_callback_invoctions == 1);
                REQUIRE(after_callback_invocations == 1);
            }
        }

        SECTION("failing to download a fresh copy results in an error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            std::string fresh_path = realm::_impl::ClientResetOperation::get_fresh_path_for(local_config.path);
            util::File f(fresh_path, util::File::Mode::mode_Write);
            f.write("a non empty file");
            f.sync();
            f.close();

            REQUIRE(!err);
            make_reset(local_config, remote_config)
                ->on_post_reset([&](SharedRealm) {
                    util::EventLoop::main().run_until([&] {
                        return bool(err);
                    });
                })
                ->run();
            REQUIRE(err);
            REQUIRE(err.value()->is_client_reset_requested());
        }

        SECTION("should honor encryption key for downloaded Realm") {
            local_config.encryption_key.resize(64, 'a');

            make_reset(local_config, remote_config)
                ->on_post_reset([&](SharedRealm realm) {
                    realm->close();
                    SharedRealm r_after;
                    REQUIRE_NOTHROW(r_after = Realm::get_shared_realm(local_config));
                    CHECK(ObjectStore::table_for_object_type(r_after->read_group(), "object")
                              ->begin()
                              ->get<Int>("value") == 6);
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
                    int64_t different_pk = table->begin()->get_primary_key().get_int() + 1;
                    table->clear();
                    auto obj = create_object(*remote, "object", partition, {different_pk});
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

        SECTION("extra local table creates a client reset error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            make_reset(local_config, remote_config)
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
                ->on_post_reset([&](SharedRealm realm) {
                    util::EventLoop::main().run_until([&] {
                        return bool(err);
                    });
                    REQUIRE_NOTHROW(realm->refresh());
                })
                ->run();
            REQUIRE(err);
            REQUIRE(err.value()->is_client_reset_requested());
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 0);
        }

        SECTION("extra local column creates a client reset error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            make_reset(local_config, remote_config)
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
                ->on_post_reset([&](SharedRealm realm) {
                    util::EventLoop::main().run_until([&] {
                        return bool(err);
                    });
                    REQUIRE_NOTHROW(realm->refresh());
                })
                ->run();

            REQUIRE(err);
            REQUIRE(err.value()->is_client_reset_requested());
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 0);
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
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            make_reset(local_config, remote_config)
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
                ->on_post_reset([&](SharedRealm realm) {
                    util::EventLoop::main().run_until([&] {
                        return bool(err);
                    });
                    REQUIRE_NOTHROW(realm->refresh());
                })
                ->run();
            REQUIRE(err);
            REQUIRE(err.value()->is_client_reset_requested());
        }

        SECTION("primary key type cannot be changed") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };

            make_reset(local_config, remote_config)
                ->make_local_changes([](SharedRealm local) {
                    local->update_schema(
                        {
                            {"new table",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->make_remote_changes([](SharedRealm remote) {
                    remote->update_schema(
                        {
                            {"new table",
                             {
                                 {"_id", PropertyType::String, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    util::EventLoop::main().run_until([&] {
                        return bool(err);
                    });
                    REQUIRE_NOTHROW(realm->refresh());
                })
                ->run();
            REQUIRE(err);
            REQUIRE(err.value()->is_client_reset_requested());
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
TEMPLATE_TEST_CASE("client reset types", "[client reset][discard local]", cf::MixedVal, cf::Int, cf::Bool, cf::Float,
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
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
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
    config2.schema = config.schema;

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
#if 0
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
        SECTION("modify complex") {
            std::vector<std::pair<std::string, Mixed>> local;
            local.emplace_back("adam", Mixed(values[0]));
            local.emplace_back("bernie", Mixed(values[0]));
            local.emplace_back("david", Mixed(values[0]));
            local.emplace_back("eric", Mixed(values[0]));
            local.emplace_back("frank", Mixed(values[1]));
            std::vector<std::pair<std::string, Mixed>> remote;
            remote.emplace_back("adam", Mixed(values[0]));
            remote.emplace_back("bernie", Mixed(values[1]));
            remote.emplace_back("carl", Mixed(values[0]));
            remote.emplace_back("david", Mixed(values[1]));
            remote.emplace_back("frank", Mixed(values[0]));
            reset_dictionary(local, remote);
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

TEMPLATE_TEST_CASE("client reset collections of links", "[client reset][discard local][collections]",
                   cf::ListOfObjects, cf::ListOfMixedLinks, cf::SetOfObjects, cf::SetOfMixedLinks,
                   cf::DictionaryOfObjects, cf::DictionaryOfMixedLinks)
{
    if (!util::EventLoop::has_implementation())
        return;

    const std::string valid_pk_name = "_id";
    const auto partition = random_string(100);
    const std::string collection_prop_name = "collection";
    TestType test_type(collection_prop_name, "dest");
    Schema schema = {
        {"source",
         {{valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
          {"realm_id", PropertyType::String | PropertyType::Nullable},
          test_type.property()}},
        {"dest",
         {
             {valid_pk_name, PropertyType::Int | PropertyType::Nullable, true},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
        {"object",
         {
             {valid_pk_name, PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
    };

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = schema;
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

    SyncTestFile config2(init_sync_manager.app(), "default");
    config2.schema = schema;

    // The following can be used to perform the client reset proper, but these tests
    // are only intended to check the transfer_group logic for different types,
    // so to save local test time, we call it directly instead.
#if 0
    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_test_server_client_reset(config, config2, init_sync_manager);
#else
    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);
#endif

    CppContext c;
    auto create_one_source_object = [&](realm::SharedRealm r, int64_t val, std::vector<ObjLink> links = {}) {
        auto object = Object::create(
            c, r, "source",
            util::Any(realm::AnyDict{{valid_pk_name, util::Any(val)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);

        for (auto link : links) {
            test_type.add_link(object.obj(), link);
        }
    };

    auto create_one_dest_object = [&](realm::SharedRealm r, int64_t val) -> ObjLink {
        auto obj = Object::create(
            c, r, "dest",
            util::Any(realm::AnyDict{{valid_pk_name, util::Any(val)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);
        return ObjLink{obj.obj().get_table()->get_key(), obj.obj().get_key()};
    };

    auto require_links_to_match_ids = [&](std::vector<Obj> links, std::vector<int64_t> expected) {
        std::vector<int64_t> actual;
        for (auto obj : links) {
            actual.push_back(obj.get<Int>(valid_pk_name));
        }
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        REQUIRE(actual == expected);
    };

    Results results;
    Object object;
    CollectionChangeSet object_changes, results_changes;
    NotificationToken object_token, results_token;
    auto setup_listeners = [&](SharedRealm realm) {
        results =
            Results(realm, ObjectStore::table_for_object_type(realm->read_group(), "source")).sort({{{"_id", true}}});
        if (results.size() >= 1) {
            auto obj = *ObjectStore::table_for_object_type(realm->read_group(), "source")->begin();
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
    auto set_links = [&](SharedRealm realm, std::vector<int64_t>& link_pks) {
        TableRef src_table = get_table(*realm, "source");
        REQUIRE(src_table->size() == 1);
        TableRef dst_table = get_table(*realm, "dest");
        std::vector<Obj> linked_objects = test_type.get_links(*src_table->begin());
        if (is_array(test_type.property().type)) {
            // order matters for lists, leave it be if they are identical,
            // otherwise clear and add everything in the correct order
            bool equal = std::equal(linked_objects.begin(), linked_objects.end(), link_pks.begin(), link_pks.end(),
                                    [&](const Obj& obj, const int64_t& pk) {
                                        return obj.get_primary_key().template get<int64_t>() == pk;
                                    });
            if (!equal) {
                test_type.clear_collection(*src_table->begin());
                for (size_t i = 0; i < link_pks.size(); ++i) {
                    ObjKey dst_key = dst_table->get_objkey_from_primary_key(Mixed{link_pks[i]});
                    test_type.add_link(*src_table->begin(), ObjLink{dst_table->get_key(), dst_key});
                }
            }
        }
        else {
            for (auto lnk : linked_objects) {
                int64_t lnk_pk = lnk.get_primary_key().get<int64_t>();
                if (std::find(link_pks.begin(), link_pks.end(), lnk_pk) == link_pks.end()) {
                    test_type.remove_link(*src_table->begin(), ObjLink{lnk.get_table()->get_key(), lnk.get_key()});
                }
            }
            REQUIRE(dst_table);
            for (int64_t lnk_pk : link_pks) {
                if (std::find_if(linked_objects.begin(), linked_objects.end(), [lnk_pk](auto& lnk) {
                        return lnk.get_primary_key().template get<int64_t>() == lnk_pk;
                    }) == linked_objects.end()) {
                    ObjKey dst_key = dst_table->get_objkey_from_primary_key(Mixed{lnk_pk});
                    REQUIRE(dst_key);
                    test_type.add_link(*src_table->begin(), ObjLink{dst_table->get_key(), dst_key});
                }
            }
        }
    };

    SECTION("integration testing") {
        auto reset_collection = [&](std::vector<int64_t> local_pk_links, std::vector<int64_t> remote_pk_links) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    set_links(local_realm, local_pk_links);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    set_links(remote_realm, remote_pk_links);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    auto linked_objects = test_type.get_links(results.get(0));
                    require_links_to_match_ids(linked_objects, local_pk_links);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    object_changes = {};
                    results_changes = {};
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    auto linked_objects = test_type.get_links(results.get(0));
                    require_links_to_match_ids(linked_objects, remote_pk_links);
                    if (!is_array(test_type.property().type)) {
                        // order should not matter except for lists
                        std::sort(local_pk_links.begin(), local_pk_links.end());
                        std::sort(remote_pk_links.begin(), remote_pk_links.end());
                    }
                    if (local_pk_links == remote_pk_links) {
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

        constexpr int64_t source_pk = 0;
        constexpr int64_t dest_pk_1 = 1;
        constexpr int64_t dest_pk_2 = 2;
        constexpr int64_t dest_pk_3 = 3;
        test_reset->setup([&](SharedRealm realm) {
            test_type.reset_test_state();
            // add a container collection with three valid links
            ObjLink dest1 = create_one_dest_object(realm, dest_pk_1);
            ObjLink dest2 = create_one_dest_object(realm, dest_pk_2);
            ObjLink dest3 = create_one_dest_object(realm, dest_pk_3);
            create_one_source_object(realm, source_pk, {dest1, dest2, dest3});
        });

        SECTION("both empty") {
            reset_collection({}, {});
        }
        SECTION("remove all") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {});
        }
        SECTION("no change") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("remove middle link") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_1, dest_pk_3});
        }
        SECTION("remove first link") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_2, dest_pk_3});
        }
        SECTION("remove last link") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_1, dest_pk_2});
        }
        SECTION("remove outside links") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_2});
        }
        SECTION("additive") {
            reset_collection({}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("add middle") {
            reset_collection({dest_pk_1, dest_pk_3}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("add first") {
            reset_collection({dest_pk_2, dest_pk_3}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("add last") {
            reset_collection({dest_pk_1, dest_pk_2}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("add outside") {
            reset_collection({dest_pk_2}, {dest_pk_1, dest_pk_2, dest_pk_3});
        }
        SECTION("reversed order") {
            reset_collection({dest_pk_1, dest_pk_2, dest_pk_3}, {dest_pk_3, dest_pk_2, dest_pk_1});
        }
    }
}

TEST_CASE("client reset with embedded object", "[client reset][discard local][embedded objects]") {
    if (!util::EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

    ObjectSchema shared_class = {"object",
                                 {
                                     {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                     {"value", PropertyType::Int},
                                 }};

    config.schema = Schema{
        shared_class,
        {"TopLevel",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"array_of_objs", PropertyType::Object | PropertyType::Array, "EmbeddedObject"},
             {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject"},
             {"embedded_dict", PropertyType::Object | PropertyType::Dictionary | PropertyType::Nullable,
              "EmbeddedObject"},
         }},
        {"EmbeddedObject",
         ObjectSchema::IsEmbedded{true},
         {
             {"array", PropertyType::Int | PropertyType::Array},
             {"name", PropertyType::String | PropertyType::Nullable},
             {"link_to_embedded_object2", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject2"},
         }},
        {"EmbeddedObject2",
         ObjectSchema::IsEmbedded{true},
         {
             {"notes", PropertyType::String | PropertyType::Dictionary | PropertyType::Nullable},
             {"date", PropertyType::Date},
             {"top_level_link", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
         }},
    };
    struct SecondLevelEmbeddedContent {
        std::vector<std::pair<std::string, std::string>> dict_values = {{"key A", random_string(10)},
                                                                        {"key B", random_string(10)}};
        Timestamp datetime = Timestamp{random_int(), 0};
        util::Optional<Mixed> pk_of_linked_object;
    };
    struct EmbeddedContent {
        std::string name = random_string(10);
        std::vector<Int> array_vals = {random_int(), random_int(), random_int()};
        util::Optional<SecondLevelEmbeddedContent> second_level = SecondLevelEmbeddedContent();
    };
    struct TopLevelContent {
        util::Optional<EmbeddedContent> link_value = EmbeddedContent();
        std::vector<EmbeddedContent> array_values{3};
        std::vector<std::pair<std::string, util::Optional<EmbeddedContent>>> dict_values = {
            {"foo", {{}}}, {"bar", {{}}}, {"baz", {{}}}};
    };

    SyncTestFile config2(init_sync_manager.app(), "default");
    config2.schema = config.schema;

    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);

    auto set_embedded = [](Obj embedded, const EmbeddedContent& value) {
        embedded.set<StringData>("name", value.name);
        ColKey list_col = embedded.get_table()->get_column_key("array");
        embedded.set_list_values<Int>(list_col, value.array_vals);
        ColKey link2_col = embedded.get_table()->get_column_key("link_to_embedded_object2");
        if (value.second_level) {
            Obj second = embedded.get_linked_object(link2_col);
            if (!second) {
                second = embedded.create_and_set_linked_object(link2_col);
            }
            second.set("date", value.second_level->datetime);
            ColKey top_link_col = second.get_table()->get_column_key("top_level_link");
            if (value.second_level->pk_of_linked_object) {
                TableRef top_table = second.get_table()->get_opposite_table(top_link_col);
                ObjKey top_link = top_table->find_primary_key(*(value.second_level->pk_of_linked_object));
                second.set(top_link_col, top_link);
            }
            else {
                second.set_null(top_link_col);
            }
            Dictionary dict = second.get_dictionary("notes");
            for (auto it = dict.begin(); it != dict.end(); ++it) {
                if (std::find_if(value.second_level->dict_values.begin(), value.second_level->dict_values.end(),
                                 [&](auto& pair) {
                                     return pair.first == (*it).first.get_string();
                                 }) == value.second_level->dict_values.end()) {
                    dict.erase(it);
                }
            }
            for (auto& it : value.second_level->dict_values) {
                dict.insert(it.first, it.second);
            }
        }
        else {
            embedded.set_null(link2_col);
        }
    };

    auto check_embedded = [](Obj embedded, const EmbeddedContent& value) {
        REQUIRE(embedded.get_any("name").get<StringData>() == value.name);
        ColKey list_col = embedded.get_table()->get_column_key("array");
        REQUIRE(embedded.get_list_values<Int>(list_col) == value.array_vals);

        ColKey link2_col = embedded.get_table()->get_column_key("link_to_embedded_object2");
        Obj second = embedded.get_linked_object(link2_col);
        if (value.second_level) {
            REQUIRE(second);
            REQUIRE(second.get<Timestamp>("date") == value.second_level->datetime);
            ColKey top_link_col = second.get_table()->get_column_key("top_level_link");
            ObjKey actual_link = second.get<ObjKey>(top_link_col);
            if (!value.second_level->pk_of_linked_object) {
                REQUIRE(!actual_link);
            }
            else {
                REQUIRE(actual_link);
                TableRef top_table = second.get_table()->get_opposite_table(top_link_col);
                Obj actual_top_obj = top_table->get_object(actual_link);
                REQUIRE(actual_top_obj.get_primary_key() == *(value.second_level->pk_of_linked_object));
            }
            Dictionary dict = second.get_dictionary("notes");
            REQUIRE(dict.size() == value.second_level->dict_values.size());
            for (auto& pair : value.second_level->dict_values) {
                util::Optional<Mixed> actual = dict.try_get(pair.first);
                REQUIRE(actual);
                REQUIRE(actual->get_string() == pair.second);
            }
        }
        else {
            REQUIRE(!second);
        }
    };

    auto set_content = [&](Obj obj, const TopLevelContent& content) {
        ColKey link_col = obj.get_table()->get_column_key("embedded_obj");
        if (!content.link_value) {
            obj.set_null(link_col);
        }
        else {
            Obj embedded_link = obj.get_linked_object(link_col);
            if (!embedded_link) {
                embedded_link = obj.create_and_set_linked_object(link_col);
            }
            set_embedded(embedded_link, *content.link_value);
        }
        auto list = obj.get_linklist("array_of_objs");
        for (size_t i = 0; i < content.array_values.size(); ++i) {
            Obj link;
            if (i >= list.size()) {
                link = list.create_and_insert_linked_object(list.size());
            }
            else {
                link = list.get_object(i);
            }
            set_embedded(link, content.array_values[i]);
        }
        auto dict = obj.get_dictionary("embedded_dict");
        for (auto it = dict.begin(); it != dict.end(); ++it) {
            if (std::find_if(content.dict_values.begin(), content.dict_values.end(), [&](auto& dict_val) {
                    return (*it).first == dict_val.first;
                }) != content.dict_values.end()) {
                dict.erase(it);
            }
        }
        for (size_t i = 0; i < content.dict_values.size(); ++i) {
            if (content.dict_values[i].second) {
                Obj embedded = dict.create_and_insert_linked_object(content.dict_values[i].first);
                set_embedded(embedded, *(content.dict_values[i].second));
            }
            else {
                dict.insert(content.dict_values[i].first, Mixed{});
            }
        }
    };
    auto check_content = [&](Obj obj, const TopLevelContent& content) {
        Obj embedded_link = obj.get_linked_object("embedded_obj");
        if (content.link_value) {
            REQUIRE(embedded_link);
            check_embedded(embedded_link, *content.link_value);
        }
        else {
            REQUIRE(!embedded_link);
        }
        auto list = obj.get_linklist("array_of_objs");
        REQUIRE(list.size() == content.array_values.size());
        for (size_t i = 0; i < content.array_values.size(); ++i) {
            Obj link = list.get_object(i);
            check_embedded(link, content.array_values[i]);
        }
        auto dict = obj.get_dictionary("embedded_dict");
        REQUIRE(dict.size() == content.dict_values.size());
        for (auto& val : content.dict_values) {
            Obj embedded = dict.get_object(val.first);
            if (val.second) {
                check_embedded(embedded, *val.second);
            }
            else {
                REQUIRE(!embedded);
            }
        }
    };

    auto reset_embedded_object = [&](TopLevelContent local_content, TopLevelContent remote_content) {
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                Obj obj = *table->begin();
                set_content(obj, local_content);
            })
            ->make_remote_changes([&](SharedRealm remote) {
                TableRef table = get_table(*remote, "TopLevel");
                REQUIRE(table->size() == 1);
                Obj obj = *table->begin();
                set_content(obj, remote_content);
            })
            ->on_post_reset([&](SharedRealm local) {
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                Obj obj = *table->begin();
                check_content(obj, remote_content);
            })
            ->run();
    };

    ObjectId pk_val = ObjectId::gen();
    test_reset->setup([&pk_val](SharedRealm realm) {
        auto table = get_table(*realm, "TopLevel");
        REQUIRE(table);
        auto obj = table->create_object_with_primary_key(pk_val);
        Obj embedded_link = obj.create_and_set_linked_object(table->get_column_key("embedded_obj"));
        embedded_link.set<String>("name", "initial name");
    });

    SECTION("no change") {
        TopLevelContent state;
        reset_embedded_object(state, state);
    }
    SECTION("modify every embedded property") {
        TopLevelContent local, remote;
        reset_embedded_object(local, remote);
    }
    SECTION("nullify embedded links") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.link_value.reset();
        for (auto& val : remote.dict_values) {
            val.second.reset();
        }
        remote.array_values.clear();
        reset_embedded_object(local, remote);
    }
    SECTION("populate embedded links") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.link_value.reset();
        for (auto& val : local.dict_values) {
            val.second.reset();
        }
        local.array_values.clear();
        reset_embedded_object(local, remote);
    }
    SECTION("add additional embedded objects") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.dict_values.push_back({"new key1", {EmbeddedContent{}}});
        remote.dict_values.push_back({"new key2", {EmbeddedContent{}}});
        remote.dict_values.push_back({"new key3", {}});
        remote.array_values.push_back({EmbeddedContent{}});
        remote.array_values.push_back({});
        remote.array_values.push_back({EmbeddedContent{}});
        reset_embedded_object(local, remote);
    }
    SECTION("remove some embedded objects") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.dict_values.push_back({"new key1", {EmbeddedContent{}}});
        local.dict_values.push_back({"new key2", {EmbeddedContent{}}});
        local.dict_values.push_back({"new key3", {}});
        local.array_values.push_back({EmbeddedContent{}});
        local.array_values.push_back({});
        local.array_values.push_back({EmbeddedContent{}});
        reset_embedded_object(local, remote);
    }
    SECTION("add a top level link cycle") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.link_value->second_level->pk_of_linked_object = Mixed{pk_val};
        reset_embedded_object(local, remote);
    }
    SECTION("remove a top level link cycle") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.link_value->second_level->pk_of_linked_object = Mixed{pk_val};
        reset_embedded_object(local, remote);
    }
    SECTION("server adds embedded object classes") {
        SyncTestFile config2(init_sync_manager.app(), "default");
        config2.schema = config.schema;
        config.schema = Schema{shared_class};
        test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        TopLevelContent remote_content;

        test_reset
            ->make_remote_changes([&](SharedRealm remote) {
                TableRef table = get_table(*remote, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                REQUIRE(table->size() == 1);
                set_content(obj, remote_content);
            })
            ->on_post_reset([&](SharedRealm local) {
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                Obj obj = *table->begin();
                check_content(obj, remote_content);
            })
            ->run();
    }
    SECTION("client adds embedded object classes") {
        SyncTestFile config2(init_sync_manager.app(), "default");
        config2.schema = Schema{shared_class};
        test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        TopLevelContent local_content;
        test_reset->make_local_changes([&](SharedRealm local) {
            TableRef table = get_table(*local, "TopLevel");
            auto obj = table->create_object_with_primary_key(pk_val);
            REQUIRE(table->size() == 1);
            set_content(obj, local_content);
        });
        REQUIRE_THROWS_WITH(test_reset->run(), "Client reset cannot recover when classes have been removed: "
                                               "{EmbeddedObject, EmbeddedObject2, TopLevel}");
    }
}

} // namespace realm
