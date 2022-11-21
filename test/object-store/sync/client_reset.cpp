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

#include <catch2/catch_all.hpp>

#include "collection_fixtures.hpp"
#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"
#include "util/event_loop.hpp"
#include "util/index_helpers.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/util/flat_map.hpp>
#include <realm/util/overload.hpp>

#include <external/mpark/variant.hpp>

#include <algorithm>
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

using namespace realm;

namespace realm {
class TestHelper {
public:
    static DBRef& get_db(SharedRealm const& shared_realm)
    {
        return Realm::Internal::get_db(*shared_realm);
    }
};
} // namespace realm

namespace {
TableRef get_table(Realm& realm, StringData object_type)
{
    return ObjectStore::table_for_object_type(realm.read_group(), object_type);
}
} // anonymous namespace

#if REALM_ENABLE_AUTH_TESTS

namespace cf = realm::collection_fixtures;
using reset_utils::create_object;

TEST_CASE("sync: client reset", "[client reset]") {
    if (!util::EventLoop::has_implementation())
        return;

    const reset_utils::Partition partition{"realm_id", random_string(20)};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"link target",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"pk link target",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
        {"link origin",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"link", PropertyType::Object | PropertyType::Nullable, "link target"},
             {"pk link", PropertyType::Object | PropertyType::Nullable, "pk link target"},
             {"list", PropertyType::Object | PropertyType::Array, "link target"},
             {"pk list", PropertyType::Object | PropertyType::Array, "pk link target"},
             partition_prop,
         }},
    };
    std::string base_url = get_base_url();
    REQUIRE(!base_url.empty());
    auto server_app_config = minimal_app_config(base_url, "client_reset_tests", schema);
    server_app_config.partition_key = partition_prop;
    TestAppSession test_app_session(create_app(server_app_config));
    auto app = test_app_session.app();
    auto get_valid_config = [&]() -> SyncTestFile {
        create_user_and_log_in(app);
        return SyncTestFile(app->current_user(), partition.value, schema);
    };
    SyncTestFile local_config = get_valid_config();
    SyncTestFile remote_config = get_valid_config();
    auto make_reset = [&](Realm::Config config_local,
                          Realm::Config config_remote) -> std::unique_ptr<reset_utils::TestClientReset> {
        return reset_utils::make_baas_client_reset(config_local, config_remote, test_app_session);
    };

    // this is just for ease of debugging
    local_config.path = local_config.path + ".local";
    remote_config.path = remote_config.path + ".remote";

    SECTION("a client reset in manual mode can be handled") {
        std::string orig_path, recovery_path;
        local_config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        ThreadSafeSyncError err;
        local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            REQUIRE(error.is_client_reset_requested());
            REQUIRE(error.user_info.size() >= 2);
            auto orig_path_it = error.user_info.find(SyncError::c_original_file_path_key);
            auto recovery_path_it = error.user_info.find(SyncError::c_recovery_file_path_key);
            REQUIRE(orig_path_it != error.user_info.end());
            REQUIRE(recovery_path_it != error.user_info.end());
            orig_path = orig_path_it->second;
            recovery_path = recovery_path_it->second;
            REQUIRE(util::File::exists(orig_path));
            REQUIRE(!util::File::exists(recovery_path));
            bool did_reset_files = test_app_session.app()->sync_manager()->immediately_run_file_actions(orig_path);
            REQUIRE(did_reset_files);
            REQUIRE(!util::File::exists(orig_path));
            REQUIRE(util::File::exists(recovery_path));
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
        SyncError error = *err.value();
        REQUIRE(error.is_client_reset_requested());
        REQUIRE(!util::File::exists(orig_path));
        REQUIRE(util::File::exists(recovery_path));
        local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
            CAPTURE(err.message);
            CAPTURE(local_config.path);
            FAIL("Error handler should not have been called");
        };
        auto post_reset_realm = Realm::get_shared_realm(local_config);
        wait_for_download(*post_reset_realm); // this should now succeed without any sync errors
        REQUIRE(util::File::exists(orig_path));
    }

    local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        CAPTURE(err.message);
        CAPTURE(local_config.path);
        FAIL("Error handler should not have been called");
    };

    local_config.cache = false;
    local_config.automatic_change_notifications = false;
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
    local_config.sync_config->notify_after_client_reset = [&](SharedRealm before, ThreadSafeReference after_ref,
                                                              bool) {
        std::lock_guard<std::mutex> lock(mtx);
        SharedRealm after = Realm::get_shared_realm(std::move(after_ref), util::Scheduler::make_default());
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

            auto obj = results.get<Obj>(0);
            REQUIRE(obj.get<Int>("value") == 4);
            object = Object(realm, obj);
            object_token = object.add_notification_callback([&](CollectionChangeSet changes) {
                object_changes = std::move(changes);
            });
        }
        results_token = results.add_notification_callback([&](CollectionChangeSet changes) {
            results_changes = std::move(changes);
        });
    };

    SECTION("recovery") {
        local_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
        std::unique_ptr<reset_utils::TestClientReset> test_reset = make_reset(local_config, remote_config);
        SECTION("modify an existing object") {
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
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(object.obj().get<Int>("value") == 4);
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.modifications);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                    // make sure that the reset operation has cleaned up after itself
                    REQUIRE(util::File::exists(local_config.path));
                    REQUIRE_FALSE(util::File::exists(fresh_path));
                })
                ->run();
        }
        SECTION("modify a deleted object") {
            ObjectId pk = ObjectId::gen();
            test_reset
                ->setup([&](SharedRealm realm) {
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    auto obj = create_object(*realm, "object", {pk}, partition);
                    auto col = obj.get_table()->get_column_key("value");
                    obj.set(col, 100);
                })
                ->make_local_changes([&](SharedRealm realm) {
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 2);
                    ObjKey key = table->get_objkey_from_primary_key(pk);
                    REQUIRE(key);
                    Obj obj = table->get_object(key);
                    obj.set("value", 200);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 2);
                    ObjKey key = table->get_objkey_from_primary_key(pk);
                    REQUIRE(key);
                    table->remove_object(key);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 2);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(results.get<Obj>(1).get<Int>("value") == 200);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(before_callback_invoctions == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(object.obj().get<Int>("value") == 4);
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions, 1); // the deletion "wins"
                    REQUIRE_INDICES(object_changes.modifications);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                    // make sure that the reset operation has cleaned up after itself
                    REQUIRE(util::File::exists(local_config.path));
                    REQUIRE_FALSE(util::File::exists(fresh_path));
                })
                ->run();
        }
        SECTION("insert") {
            int64_t new_value = 42;
            test_reset
                ->make_local_changes([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ObjectId different_pk = ObjectId::gen();
                    auto obj = create_object(*realm, "object", {different_pk}, partition);
                    auto col = obj.get_table()->get_column_key("value");
                    obj.set(col, new_value);
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 2);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(before_callback_invoctions == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 2);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(results.get<Obj>(1).get<Int>("value") == new_value);
                    CHECK(object.obj().get<Int>("value") == 4);
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                    REQUIRE_INDICES(object_changes.modifications);
                    REQUIRE_INDICES(object_changes.insertions);
                    REQUIRE_INDICES(object_changes.deletions);
                    // make sure that the reset operation has cleaned up after itself
                    REQUIRE(util::File::exists(local_config.path));
                    REQUIRE_FALSE(util::File::exists(fresh_path));
                })
                ->run();
        }

        SECTION("delete") {
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
                    CHECK(results.size() == 0);
                    CHECK(!object.is_valid());
                    REQUIRE_INDICES(results_changes.modifications);
                    REQUIRE_INDICES(results_changes.insertions);
                    REQUIRE_INDICES(results_changes.deletions);
                })
                ->run();
        }

        SECTION("Simultaneous compatible schema changes are allowed") {
            const std::string new_table_name = "same new table name";
            const std::string existing_table_name = "preexisting table name";
            const std::string locally_added_table_name = "locally added table";
            const std::string remotely_added_table_name = "remotely added table";
            const Property pk_id = {"_id", PropertyType::ObjectId | PropertyType::Nullable,
                                    Property::IsPrimary{true}};
            const Property shared_added_property = {"added identical property",
                                                    PropertyType::UUID | PropertyType::Nullable};
            const Property locally_added_property = {"locally added property", PropertyType::ObjectId};
            const Property remotely_added_property = {"remotely added property",
                                                      PropertyType::Float | PropertyType::Nullable};
            ObjectId pk1 = ObjectId::gen();
            ObjectId pk2 = ObjectId::gen();
            auto verify_changes = [&](SharedRealm realm) {
                REQUIRE_NOTHROW(advance_and_notify(*realm));
                std::vector<std::string> tables_to_check = {existing_table_name, new_table_name,
                                                            locally_added_table_name, remotely_added_table_name};
                for (auto& table_name : tables_to_check) {
                    CAPTURE(table_name);
                    auto table = get_table(*realm, table_name);
                    REQUIRE(table);
                    REQUIRE(table->get_column_key(shared_added_property.name));
                    REQUIRE(table->get_column_key(locally_added_property.name));
                    REQUIRE(table->get_column_key(remotely_added_property.name));
                    auto sorted_results = table->get_sorted_view(table->get_column_key(pk_id.name));
                    REQUIRE(sorted_results.size() == 2);
                    REQUIRE(sorted_results.get_object(0).get_primary_key().get_object_id() == pk1);
                    REQUIRE(sorted_results.get_object(1).get_primary_key().get_object_id() == pk2);
                }
            };
            make_reset(local_config, remote_config)
                ->setup([&](SharedRealm before) {
                    before->update_schema(
                        {
                            {existing_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->make_local_changes([&](SharedRealm local) {
                    local->update_schema(
                        {
                            {new_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 locally_added_property,
                                 shared_added_property,
                             }},
                            {existing_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 locally_added_property,
                                 shared_added_property,
                             }},
                            {locally_added_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 locally_added_property,
                                 shared_added_property,
                                 remotely_added_property,
                             }},
                        },
                        0, nullptr, nullptr, true);

                    create_object(*local, new_table_name, {pk1}, partition);
                    create_object(*local, existing_table_name, {pk1}, partition);
                    create_object(*local, locally_added_table_name, {pk1}, partition);
                    create_object(*local, locally_added_table_name, {pk2}, partition);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    remote->update_schema(
                        {
                            {new_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 remotely_added_property,
                                 shared_added_property,
                             }},
                            {existing_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 remotely_added_property,
                                 shared_added_property,
                             }},
                            {remotely_added_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 remotely_added_property,
                                 locally_added_property,
                                 shared_added_property,
                             }},
                        },
                        0, nullptr, nullptr, true);

                    create_object(*remote, new_table_name, {pk2}, partition);
                    create_object(*remote, existing_table_name, {pk2}, partition);
                    create_object(*remote, remotely_added_table_name, {pk1}, partition);
                    create_object(*remote, remotely_added_table_name, {pk2}, partition);
                })
                ->on_post_reset([&](SharedRealm local) {
                    verify_changes(local);
                })
                ->run();
            auto remote = Realm::get_shared_realm(remote_config);
            wait_for_upload(*remote);
            wait_for_download(*remote);
            verify_changes(remote);
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("incompatible property changes are rejected") {
            const Property pk_id = {"_id", PropertyType::ObjectId | PropertyType::Nullable,
                                    Property::IsPrimary{true}};
            const std::string table_name = "new table";
            const std::string prop_name = "new_property";
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            make_reset(local_config, remote_config)
                ->make_local_changes([&](SharedRealm local) {
                    local->update_schema(
                        {
                            {table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 {prop_name, PropertyType::Float},
                             }},
                        },
                        0, nullptr, nullptr, true);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    remote->update_schema(
                        {
                            {table_name,
                             {
                                 pk_id,
                                 partition_prop,
                                 {prop_name, PropertyType::Int},
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
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 0);
        }
    } // end recovery section

    SECTION("discard local") {
        local_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
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
                ObjectId to_continue_reset = test_reset->get_pk_of_object_driving_reset();
                test_reset = make_reset(local_config, config3);
                test_reset->set_pk_of_object_driving_reset(to_continue_reset);
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

        SECTION("can be reset without notifiers") {
            local_config.sync_config->notify_before_client_reset = nullptr;
            local_config.sync_config->notify_after_client_reset = nullptr;
            make_reset(local_config, remote_config)->run();
            REQUIRE(before_callback_invoctions == 0);
            REQUIRE(after_callback_invocations == 0);
        }

        SECTION("callbacks are seeded with Realm instances even if the coordinator dies") {
            auto client_reset_harness = make_reset(local_config, remote_config);
            client_reset_harness->disable_wait_for_reset_completion();
            std::shared_ptr<SyncSession> session;
            client_reset_harness
                ->on_post_local_changes([&](SharedRealm local) {
                    // retain a reference so the sync session completes, even though the Realm is cleaned up
                    session = local->sync_session();
                })
                ->run();
            auto local_coordinator = realm::_impl::RealmCoordinator::get_existing_coordinator(local_config.path);
            REQUIRE(!local_coordinator);
            REQUIRE(before_callback_invoctions == 0);
            REQUIRE(after_callback_invocations == 0);
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return after_callback_invocations > 0;
                },
                std::chrono::seconds(60));
            // this test also relies on the test config above to verify the Realm instances in the callbacks
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("notifiers work if the session instance changes") {
            // run this test with ASAN to check for use after free
            size_t before_callback_invoctions_2 = 0;
            size_t after_callback_invocations_2 = 0;
            std::shared_ptr<SyncSession> session;
            std::unique_ptr<SyncConfig> config_copy;
            {
                SyncTestFile temp_config = get_valid_config();
                temp_config.persist();
                temp_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
                config_copy = std::make_unique<SyncConfig>(*temp_config.sync_config);
                config_copy->notify_before_client_reset = [&](SharedRealm) {
                    std::lock_guard<std::mutex> lock(mtx);
                    ++before_callback_invoctions_2;
                };
                config_copy->notify_after_client_reset = [&](SharedRealm, ThreadSafeReference, bool) {
                    std::lock_guard<std::mutex> lock(mtx);
                    ++after_callback_invocations_2;
                };

                temp_config.sync_config->notify_before_client_reset = [&](SharedRealm) {
                    std::lock_guard<std::mutex> lock(mtx);
                    ++before_callback_invoctions;
                    REQUIRE(session);
                    REQUIRE(config_copy);
                    session->update_configuration(*config_copy);
                };

                auto realm = Realm::get_shared_realm(temp_config);
                wait_for_upload(*realm);

                session = test_app_session.app()->sync_manager()->get_existing_session(temp_config.path);
                REQUIRE(session);
            }
            realm::SyncError synthetic(sync::make_error_code(sync::ProtocolError::bad_client_file),
                                       "A fake client reset error", true);
            synthetic.server_requests_action = sync::ProtocolErrorInfo::Action::ClientReset;
            SyncSession::OnlyForTesting::handle_error(*session, synthetic);

            session->revive_if_needed();
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return before_callback_invoctions > 0;
                },
                std::chrono::seconds(120));
            millisleep(500); // just make some space for the after callback to be attempted
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 0);
            REQUIRE(before_callback_invoctions_2 == 0);
            REQUIRE(after_callback_invocations_2 == 0);
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
            auto session = test_app_session.app()->sync_manager()->get_existing_session(local_config.path);
            if (session) {
                session->shutdown_and_wait();
            }
            {
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(before_callback_invoctions == 1);
                REQUIRE(after_callback_invocations == 1);
            }
        }

        SECTION("invalid files at the fresh copy path are cleaned up") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            std::string fresh_path = realm::_impl::ClientResetOperation::get_fresh_path_for(local_config.path);
            util::File f(fresh_path, util::File::Mode::mode_Write);
            f.write("a non empty file");
            f.sync();
            f.close();

            make_reset(local_config, remote_config)->run();
            REQUIRE(!err);
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("failing to download a fresh copy results in an error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            std::string fresh_path = realm::_impl::ClientResetOperation::get_fresh_path_for(local_config.path);
            // create a non-empty directory that we'll fail to delete
            util::make_dir(fresh_path);
            util::File(util::File::resolve("file", fresh_path), util::File::mode_Write);

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
                    ObjectId different_pk = ObjectId::gen();
                    table->clear();
                    auto obj = create_object(*remote, "object", {different_pk}, partition);
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
                    auto obj = create_object(*remote, "object", {orig_pk.get_object_id()}, partition);
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
                    auto obj = create_object(*local, "object", util::none, partition);
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
                                 {"_id", PropertyType::ObjectId | PropertyType::Nullable, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        0, nullptr, nullptr, true);
                    create_object(*local, "object2", ObjectId::gen(), partition);
                    create_object(*local, "object2", ObjectId::gen(), partition);
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
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                 {"value2", PropertyType::Int},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                            {"object2",
                             {
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
                                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
                k0 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 1).get_key();
                k1 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 2).get_key();
                k2 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 3).get_key();
                Obj o = create_object(*realm, "link origin", ObjectId::gen(), partition);
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
            ObjectId id1 = ObjectId::gen();
            ObjectId id2 = ObjectId::gen();
            ObjectId id3 = ObjectId::gen();
            ObjectId id4 = ObjectId::gen();
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    table->clear();
                    create_object(*local, "object", {id1}, partition).set("value", 4);
                    create_object(*local, "object", {id2}, partition).set("value", 5);
                    create_object(*local, "object", {id3}, partition).set("value", 6);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    table->clear();
                    create_object(*remote, "object", {id1}, partition).set("value", 4);
                    create_object(*remote, "object", {id2}, partition).set("value", 7);
                    create_object(*remote, "object", {id4}, partition).set("value", 8);
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
                    CHECK(results.get<Obj>(0).get<ObjectId>("_id") == id1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(results.get<Obj>(1).get<ObjectId>("_id") == id2);
                    CHECK(results.get<Obj>(1).get<Int>("value") == 7);
                    CHECK(results.get<Obj>(2).get<ObjectId>("_id") == id4);
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
                    auto k0 =
                        create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 1).get_key();
                    create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 2);
                    create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 3);

                    Obj o = create_object(*realm, "link origin", ObjectId::gen(), partition);
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
                    k0 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 1).get_key();
                    k1 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 2).get_key();
                    k2 = create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 3).get_key();
                    Obj o = create_object(*realm, "link origin", ObjectId::gen(), partition);
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
    } // end discard local section

    SECTION("cycle detection") {
        auto has_reset_cycle_flag = [](SharedRealm realm) -> util::Optional<_impl::client_reset::PendingReset> {
            auto db = TestHelper::get_db(realm);
            auto rt = db->start_read();
            return _impl::client_reset::has_pending_reset(rt);
        };
        ThreadSafeSyncError err;
        local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            err = error;
        };
        auto make_fake_previous_reset = [&local_config](ClientResyncMode type) {
            local_config.sync_config->notify_before_client_reset = [previous_type = type](SharedRealm realm) {
                auto db = TestHelper::get_db(realm);
                auto wt = db->start_write();
                _impl::client_reset::track_reset(wt, previous_type);
                wt->commit();
            };
        };
        SECTION("a normal reset adds and removes a cycle detection flag") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            local_config.sync_config->notify_before_client_reset = [&](SharedRealm realm) {
                auto flag = has_reset_cycle_flag(realm);
                REQUIRE(!flag);
                std::lock_guard<std::mutex> lock(mtx);
                ++before_callback_invoctions;
            };
            local_config.sync_config->notify_after_client_reset = [&](SharedRealm, ThreadSafeReference realm_ref,
                                                                      bool did_recover) {
                SharedRealm realm = Realm::get_shared_realm(std::move(realm_ref), util::Scheduler::make_default());
                auto flag = has_reset_cycle_flag(realm);
                REQUIRE(bool(flag));
                REQUIRE(flag->type == ClientResyncMode::Recover);
                REQUIRE(did_recover);
                std::lock_guard<std::mutex> lock(mtx);
                ++after_callback_invocations;
            };
            make_reset(local_config, remote_config)
                ->on_post_local_changes([&](SharedRealm realm) {
                    auto flag = has_reset_cycle_flag(realm);
                    REQUIRE(!flag);
                })
                ->run();
            REQUIRE(!err);
            REQUIRE(before_callback_invoctions == 1);
            REQUIRE(after_callback_invocations == 1);
        }
        SECTION("In DiscardLocal mode: a previous failed discard reset is detected and generates an error") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
            make_fake_previous_reset(ClientResyncMode::DiscardLocal);
            make_reset(local_config, remote_config)->run();
            timed_sleeping_wait_for([&]() -> bool {
                return !!err;
            });
            REQUIRE(err.value()->is_client_reset_requested());
        }
        SECTION("In Recover mode: a previous failed recover reset is detected and generates an error") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
            make_fake_previous_reset(ClientResyncMode::Recover);
            make_reset(local_config, remote_config)->run();
            timed_sleeping_wait_for([&]() -> bool {
                return !!err;
            });
            REQUIRE(err.value()->is_client_reset_requested());
        }
        SECTION("In Recover mode: a previous failed discard reset is detected and generates an error") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
            make_fake_previous_reset(ClientResyncMode::DiscardLocal);
            make_reset(local_config, remote_config)->run();
            timed_sleeping_wait_for([&]() -> bool {
                return !!err;
            });
            REQUIRE(err.value()->is_client_reset_requested());
        }
        SECTION("In RecoverOrDiscard mode: a previous failed discard reset is detected and generates an error") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            make_fake_previous_reset(ClientResyncMode::DiscardLocal);
            make_reset(local_config, remote_config)->run();
            timed_sleeping_wait_for([&]() -> bool {
                return !!err;
            });
            REQUIRE(err.value()->is_client_reset_requested());
        }
        const ObjectId added_pk = ObjectId::gen();
        auto has_added_object = [&](SharedRealm realm) -> bool {
            REQUIRE_NOTHROW(realm->refresh());
            auto table = get_table(*realm, "object");
            REQUIRE(table);
            ObjKey key = table->find_primary_key(added_pk);
            return !!key;
        };
        SECTION(
            "In RecoverOrDiscard mode: a previous failed recovery is detected and triggers a DiscardLocal reset") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            make_fake_previous_reset(ClientResyncMode::Recover);
            local_config.sync_config->notify_after_client_reset = [&](SharedRealm before,
                                                                      ThreadSafeReference after_ref,
                                                                      bool did_recover) {
                SharedRealm after = Realm::get_shared_realm(std::move(after_ref), util::Scheduler::make_default());

                REQUIRE(!did_recover);
                REQUIRE(has_added_object(before));
                REQUIRE(!has_added_object(after)); // discarded insert due to fallback to DiscardLocal mode
                std::lock_guard<std::mutex> lock(mtx);
                ++after_callback_invocations;
            };
            make_reset(local_config, remote_config)
                ->make_local_changes([&](SharedRealm realm) {
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    create_object(*realm, "object", {added_pk}, partition);
                })
                ->run();
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return after_callback_invocations > 0 || err;
                },
                std::chrono::seconds(120));
            REQUIRE(!err);
        }
        SECTION("In DiscardLocal mode: a previous failed recovery does not cause an error") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
            make_fake_previous_reset(ClientResyncMode::Recover);
            local_config.sync_config->notify_after_client_reset = [&](SharedRealm before,
                                                                      ThreadSafeReference after_ref,
                                                                      bool did_recover) {
                SharedRealm after = Realm::get_shared_realm(std::move(after_ref), util::Scheduler::make_default());

                REQUIRE(!did_recover);
                REQUIRE(has_added_object(before));
                REQUIRE(!has_added_object(after)); // not recovered
                std::lock_guard<std::mutex> lock(mtx);
                ++after_callback_invocations;
            };
            make_reset(local_config, remote_config)
                ->make_local_changes([&](SharedRealm realm) {
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    create_object(*realm, "object", {added_pk}, partition);
                })
                ->run();
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return after_callback_invocations > 0 || err;
                },
                std::chrono::seconds(120));
            REQUIRE(!err);
        }
    } // end cycle detection
    SECTION("The server can prohibit recovery") {
        const realm::AppSession& app_session = test_app_session.app_session();
        auto sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);
        auto sync_config = app_session.admin_api.get_config(app_session.server_app_id, sync_service);
        REQUIRE(!sync_config.recovery_is_disabled);
        constexpr bool recovery_is_disabled = true;
        app_session.admin_api.set_disable_recovery_to(app_session.server_app_id, sync_service.id, sync_config,
                                                      recovery_is_disabled);
        sync_config = app_session.admin_api.get_config(app_session.server_app_id, sync_service);
        REQUIRE(sync_config.recovery_is_disabled);

        SECTION("In Recover mode, a manual client reset is triggered") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
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
            SyncError error = *err.value();
            REQUIRE(error.is_client_reset_requested());
            REQUIRE(error.user_info.size() >= 2);
            REQUIRE(error.user_info.count(SyncError::c_original_file_path_key) == 1);
            REQUIRE(error.user_info.count(SyncError::c_recovery_file_path_key) == 1);
        }
        SECTION("In RecoverOrDiscard mode, DiscardLocal is selected") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            constexpr int64_t new_value = 123456;
            make_reset(local_config, remote_config)
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    auto obj = create_object(*local, "object", ObjectId::gen(), partition);
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
                    CHECK(results.size() == 1); // insert was discarded
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(object.is_valid());
                    CHECK(object.obj().get<Int>("value") == 6);
                })
                ->run();
        }
    } // end: The server can prohibit recovery
}

#endif // REALM_ENABLE_AUTH_TESTS

namespace cf = realm::collection_fixtures;
TEMPLATE_TEST_CASE("client reset types", "[client reset][local]", cf::MixedVal, cf::Int, cf::Bool, cf::Float,
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
    ClientResyncMode test_mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
    CAPTURE(test_mode);
    config.sync_config->client_resync_mode = test_mode;
    config.schema = Schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
            object_token = object.add_notification_callback([&](CollectionChangeSet changes) {
                object_changes = std::move(changes);
            });
        }
        results_token = results.add_notification_callback([&](CollectionChangeSet changes) {
            results_changes = std::move(changes);
        });
    };

    auto check_list = [&](Obj obj, std::vector<T>& expected) {
        ColKey col = obj.get_table()->get_column_key("list");
        auto actual = obj.get_list_values<T>(col);
        REQUIRE(actual == expected);
    };

    auto check_dictionary = [&](Obj obj, std::map<std::string, Mixed>& expected) {
        ColKey col = obj.get_table()->get_column_key("dictionary");
        Dictionary dict = obj.get_dictionary(col);
        REQUIRE(dict.size() == expected.size());
        for (auto& pair : expected) {
            auto it = dict.find(pair.first);
            REQUIRE(it != dict.end());
            REQUIRE((*it).second == pair.second);
        }
    };

    auto check_set = [&](Obj obj, std::set<Mixed>& expected) {
        ColKey col = obj.get_table()->get_column_key("set");
        SetBasePtr set = obj.get_setbase_ptr(col);
        REQUIRE(set->size() == expected.size());
        for (auto& value : expected) {
            auto ndx = set->find_any(value);
            CAPTURE(value);
            REQUIRE(ndx != realm::not_found);
        }
    };

    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);

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
                    T expected_state = (test_mode == ClientResyncMode::DiscardLocal) ? remote_state : local_state;
                    check_value(results.get<Obj>(0), expected_state);
                    check_value(object.obj(), expected_state);
                    if (local_state == expected_state) {
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

        auto reset_list = [&](std::vector<T>&& local_state, std::vector<T>&& remote_state) {
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
                    std::vector<T>& expected_state = remote_state;
                    if (test_mode == ClientResyncMode::Recover) {
                        expected_state = local_state;
                    }
                    check_list(results.get<Obj>(0), expected_state);
                    check_list(object.obj(), expected_state);
                    if (local_state == expected_state) {
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

        auto reset_dictionary = [&](std::map<std::string, Mixed>&& local_state,
                                    std::map<std::string, Mixed>&& remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    auto table = get_table(*local_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto& pair : local_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end();) {
                        auto found = std::any_of(local_state.begin(), local_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            it = dict.erase(it);
                        }
                        else {
                            ++it;
                        }
                    }
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    auto table = get_table(*remote_realm, "test type");
                    REQUIRE(table);
                    REQUIRE(table->size() == 1);
                    ColKey col = table->get_column_key("dictionary");
                    Dictionary dict = table->begin()->get_dictionary(col);
                    for (auto& pair : remote_state) {
                        dict.insert(pair.first, pair.second);
                    }
                    for (auto it = dict.begin(); it != dict.end();) {
                        auto found = std::any_of(remote_state.begin(), remote_state.end(), [&](auto pair) {
                            return Mixed{pair.first} == (*it).first && Mixed{pair.second} == (*it).second;
                        });
                        if (!found) {
                            it = dict.erase(it);
                        }
                        else {
                            ++it;
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

                    auto& expected_state = remote_state;
                    if (test_mode == ClientResyncMode::Recover) {
                        for (auto it : local_state) {
                            expected_state[it.first] = it.second;
                        }
                        if (local_state.find(dict_key) == local_state.end()) {
                            expected_state.erase(dict_key); // explict erasure of initial state occured
                        }
                    }
                    check_dictionary(results.get<Obj>(0), expected_state);
                    check_dictionary(object.obj(), expected_state);
                    if (local_state == expected_state) {
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
            std::map<std::string, Mixed> local;
            local.emplace(std::make_pair("adam", Mixed(values[0])));
            local.emplace(std::make_pair("bernie", Mixed(values[0])));
            local.emplace(std::make_pair("david", Mixed(values[0])));
            local.emplace(std::make_pair("eric", Mixed(values[0])));
            local.emplace(std::make_pair("frank", Mixed(values[1])));
            std::map<std::string, Mixed> remote;
            remote.emplace(std::make_pair("adam", Mixed(values[0])));
            remote.emplace(std::make_pair("bernie", Mixed(values[1])));
            remote.emplace(std::make_pair("carl", Mixed(values[0])));
            remote.emplace(std::make_pair("david", Mixed(values[1])));
            remote.emplace(std::make_pair("frank", Mixed(values[0])));
            reset_dictionary(std::move(local), std::move(remote));
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

        auto reset_set = [&](std::set<Mixed> local_state, std::set<Mixed> remote_state) {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    auto table = get_table(*local_realm, "test type");
                    REQUIRE(table);
                    ColKey col = table->get_column_key("set");
                    SetBasePtr set = table->begin()->get_setbase_ptr(col);
                    for (size_t i = set->size(); i > 0; --i) {
                        Mixed si = set->get_any(i - 1);
                        if (local_state.find(si) == local_state.end()) {
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
                        if (remote_state.find(si) == remote_state.end()) {
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
                    std::set<Mixed>& expected = remote_state;
                    if (test_mode == ClientResyncMode::Recover) {
                        bool do_erase_initial = remote_state.find(Mixed{values[0]}) == remote_state.end() ||
                                                local_state.find(Mixed{values[0]}) == local_state.end();
                        for (auto& e : local_state) {
                            expected.insert(e);
                        }
                        if (do_erase_initial) {
                            expected.erase(Mixed{values[0]}); // explicit erase of initial element occured
                        }
                    }
                    check_set(results.get<Obj>(0), expected);
                    check_set(object.obj(), expected);
                    if (local_state == expected) {
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

namespace test_instructions {

struct Add {
    Add(util::Optional<int64_t> key)
        : pk(key)
    {
    }
    util::Optional<int64_t> pk;
};

struct Remove {
    Remove(util::Optional<int64_t> key)
        : pk(key)
    {
    }
    util::Optional<int64_t> pk;
};

struct Clear {
};

struct RemoveObject {
    RemoveObject(std::string_view name, util::Optional<int64_t> key)
        : pk(key)
        , class_name(name)
    {
    }
    util::Optional<int64_t> pk;
    std::string_view class_name;
};

struct CreateObject {
    CreateObject(std::string_view name, util::Optional<int64_t> key)
        : pk(key)
        , class_name(name)
    {
    }
    util::Optional<int64_t> pk;
    std::string_view class_name;
};

struct Move {
    Move(size_t from_ndx, size_t to_ndx)
        : from(from_ndx)
        , to(to_ndx)
    {
    }
    size_t from;
    size_t to;
};

struct CollectionOperation {
    CollectionOperation(Add op)
        : m_op(op)
    {
    }
    CollectionOperation(Remove op)
        : m_op(op)
    {
    }
    CollectionOperation(RemoveObject op)
        : m_op(op)
    {
    }
    CollectionOperation(CreateObject op)
        : m_op(op)
    {
    }
    CollectionOperation(Clear op)
        : m_op(op)
    {
    }
    CollectionOperation(Move op)
        : m_op(op)
    {
    }
    void apply(collection_fixtures::LinkedCollectionBase* collection, Obj src_obj, TableRef dst_table)
    {
        mpark::visit(
            util::overload{
                [&](Add add_link) {
                    Mixed pk_to_add = add_link.pk ? Mixed{add_link.pk} : Mixed{};
                    ObjKey dst_key = dst_table->find_primary_key(pk_to_add);
                    REALM_ASSERT(dst_key);
                    collection->add_link(src_obj, ObjLink{dst_table->get_key(), dst_key});
                },
                [&](Remove remove_link) {
                    Mixed pk_to_remove = remove_link.pk ? Mixed{remove_link.pk} : Mixed{};
                    ObjKey dst_key = dst_table->find_primary_key(pk_to_remove);
                    REALM_ASSERT(dst_key);
                    bool did_remove = collection->remove_link(src_obj, ObjLink{dst_table->get_key(), dst_key});
                    REALM_ASSERT(did_remove);
                },
                [&](RemoveObject remove_object) {
                    Group* group = dst_table->get_parent_group();
                    Group::TableNameBuffer buffer;
                    TableRef table =
                        group->get_table(Group::class_name_to_table_name(remove_object.class_name, buffer));
                    REALM_ASSERT(table);
                    ObjKey dst_key = table->find_primary_key(Mixed{remove_object.pk});
                    REALM_ASSERT(dst_key);
                    table->remove_object(dst_key);
                },
                [&](CreateObject create_object) {
                    Group* group = dst_table->get_parent_group();
                    Group::TableNameBuffer buffer;
                    TableRef table =
                        group->get_table(Group::class_name_to_table_name(create_object.class_name, buffer));
                    REALM_ASSERT(table);
                    table->create_object_with_primary_key(Mixed{create_object.pk});
                },
                [&](Clear) {
                    collection->clear_collection(src_obj);
                },
                [&](Move move) {
                    collection->move(src_obj, move.from, move.to);
                }},
            m_op);
    }

private:
    mpark::variant<Add, Remove, Clear, RemoveObject, CreateObject, Move> m_op;
};

} // namespace test_instructions

TEMPLATE_TEST_CASE("client reset collections of links", "[client reset][local][links][collections]",
                   cf::ListOfObjects, cf::ListOfMixedLinks, cf::SetOfObjects, cf::SetOfMixedLinks,
                   cf::DictionaryOfObjects, cf::DictionaryOfMixedLinks)
{
    if (!util::EventLoop::has_implementation())
        return;

    using namespace test_instructions;
    const std::string valid_pk_name = "_id";
    const auto partition = random_string(100);
    const std::string collection_prop_name = "collection";
    TestType test_type(collection_prop_name, "dest");
    constexpr bool test_type_is_array = realm::is_any_v<TestType, cf::ListOfObjects, cf::ListOfMixedLinks>;
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
             {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"realm_id", PropertyType::String | PropertyType::Nullable},
         }},
    };

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    config.schema = schema;
    ClientResyncMode test_mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
    CAPTURE(test_mode);
    config.sync_config->client_resync_mode = test_mode;

    SyncTestFile config2(init_sync_manager.app(), "default");
    config2.schema = schema;

    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);

    CppContext c;
    auto create_one_source_object = [&](realm::SharedRealm r, int64_t val, std::vector<ObjLink> links = {}) {
        auto object = Object::create(
            c, r, "source",
            std::any(realm::AnyDict{{valid_pk_name, std::any(val)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);

        for (auto link : links) {
            test_type.add_link(object.obj(), link);
        }
    };

    auto create_one_dest_object = [&](realm::SharedRealm r, util::Optional<int64_t> val) -> ObjLink {
        std::any v;
        if (val) {
            v = std::any(*val);
        }
        auto obj = Object::create(
            c, r, "dest",
            std::any(realm::AnyDict{{valid_pk_name, std::move(v)}, {"realm_id", std::string(partition)}}),
            CreatePolicy::ForceCreate);
        return ObjLink{obj.obj().get_table()->get_key(), obj.obj().get_key()};
    };

    auto require_links_to_match_ids = [&](std::vector<Obj>& links, std::vector<util::Optional<int64_t>>& expected,
                                          bool sorted) {
        std::vector<util::Optional<int64_t>> actual;
        for (auto obj : links) {
            if (obj.is_null(valid_pk_name)) {
                actual.push_back(util::none);
            }
            else {
                actual.push_back(obj.get<Int>(valid_pk_name));
            }
        }
        if (sorted) {
            std::sort(actual.begin(), actual.end());
        }
        REQUIRE(actual == expected);
    };

    constexpr int64_t source_pk = 0;
    constexpr util::Optional<int64_t> dest_pk_1 = 1;
    constexpr util::Optional<int64_t> dest_pk_2 = 2;
    constexpr util::Optional<int64_t> dest_pk_3 = 3;
    constexpr util::Optional<int64_t> dest_pk_4 = 4;
    constexpr util::Optional<int64_t> dest_pk_5 = 5;

    Results results;
    Object object;
    CollectionChangeSet object_changes, results_changes;
    NotificationToken object_token, results_token;
    auto setup_listeners = [&](SharedRealm realm) {
        TableRef source_table = get_table(*realm, "source");
        ColKey id_col = source_table->get_column_key("_id");
        results = Results(realm, source_table->where().equal(id_col, source_pk));
        if (auto obj = results.first()) {
            object = Object(realm, *obj);
            object_token = object.add_notification_callback([&](CollectionChangeSet changes) {
                object_changes = std::move(changes);
            });
        }
        results_token = results.add_notification_callback([&](CollectionChangeSet changes) {
            results_changes = std::move(changes);
        });
    };

    auto get_source_object = [&](SharedRealm realm) -> Obj {
        TableRef src_table = get_table(*realm, "source");
        return src_table->try_get_object(src_table->find_primary_key(Mixed{source_pk}));
    };
    auto apply_instructions = [&](SharedRealm realm, std::vector<CollectionOperation>& instructions) {
        TableRef dst_table = get_table(*realm, "dest");
        for (auto& instruction : instructions) {
            Obj src_obj = get_source_object(realm);
            instruction.apply(&test_type, src_obj, dst_table);
        }
    };

    auto reset_collection =
        [&](std::vector<CollectionOperation>&& local_ops, std::vector<CollectionOperation>&& remote_ops,
            std::vector<util::Optional<int64_t>>&& expected_recovered_state, size_t num_expected_nulls = 0) {
            std::vector<util::Optional<int64_t>> remote_pks;
            std::vector<util::Optional<int64_t>> local_pks;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    apply_instructions(local_realm, local_ops);
                    Obj source_obj = get_source_object(local_realm);
                    if (source_obj) {
                        auto local_links = test_type.get_links(source_obj);
                        std::transform(local_links.begin(), local_links.end(), std::back_inserter(local_pks),
                                       [](auto obj) -> util::Optional<int64_t> {
                                           Mixed pk = obj.get_primary_key();
                                           return pk.is_null() ? util::none : util::make_optional(pk.get_int());
                                       });
                    }
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    apply_instructions(remote_realm, remote_ops);
                    Obj source_obj = get_source_object(remote_realm);
                    if (source_obj) {
                        auto remote_links = test_type.get_links(source_obj);
                        std::transform(remote_links.begin(), remote_links.end(), std::back_inserter(remote_pks),
                                       [](auto obj) -> util::Optional<int64_t> {
                                           Mixed pk = obj.get_primary_key();
                                           return pk.is_null() ? util::none : util::make_optional(pk.get_int());
                                       });
                    }
                })
                ->on_post_local_changes([&](SharedRealm realm) {
                    setup_listeners(realm);
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    object_changes = {};
                    results_changes = {};
                    REQUIRE_NOTHROW(advance_and_notify(*realm));
                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    auto linked_objects = test_type.get_links(results.get(0));
                    std::vector<util::Optional<int64_t>>& expected_links = remote_pks;
                    if (test_mode == ClientResyncMode::Recover) {
                        expected_links = expected_recovered_state;
                        size_t expected_size = expected_links.size();
                        if (!test_type.will_erase_removed_object_links()) {
                            // dictionary size will remain the same because the key is preserved with a null value
                            expected_size += num_expected_nulls;
                        }
                        CHECK(test_type.size_of_collection(results.get(0)) == expected_size);
                    }
                    if (!test_type_is_array) {
                        // order should not matter except for lists
                        std::sort(local_pks.begin(), local_pks.end());
                        std::sort(expected_links.begin(), expected_links.end());
                    }
                    require_links_to_match_ids(linked_objects, expected_links, !test_type_is_array);
                    if (local_pks == expected_links) {
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

    auto reset_collection_removing_source_object = [&](std::vector<CollectionOperation>&& local_ops,
                                                       std::vector<CollectionOperation>&& remote_ops) {
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                apply_instructions(local_realm, local_ops);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                apply_instructions(remote_realm, remote_ops);
            })
            ->on_post_reset([&](SharedRealm realm) {
                REQUIRE_NOTHROW(advance_and_notify(*realm));
                TableRef table = realm->read_group().get_table("class_source");
                REQUIRE(!table->find_primary_key(Mixed{source_pk}));
            })
            ->run();
    };

    test_reset->setup([&](SharedRealm realm) {
        test_type.reset_test_state();
        // add a container collection with three valid links
        ObjLink dest1 = create_one_dest_object(realm, dest_pk_1);
        ObjLink dest2 = create_one_dest_object(realm, dest_pk_2);
        ObjLink dest3 = create_one_dest_object(realm, dest_pk_3);
        create_one_dest_object(realm, dest_pk_4);
        create_one_dest_object(realm, dest_pk_5);
        create_one_source_object(realm, source_pk, {dest1, dest2, dest3});
    });

    SECTION("no changes") {
        reset_collection({}, {}, {dest_pk_1, dest_pk_2, dest_pk_3});
    }
    SECTION("remote removes all") {
        reset_collection({}, {{Remove{dest_pk_3}}, {Remove{dest_pk_2}}, {Remove{dest_pk_1}}}, {});
    }
    SECTION("local removes all") { // local client state wins
        reset_collection({{Remove{dest_pk_3}}, {Remove{dest_pk_2}}, {Remove{dest_pk_1}}}, {}, {});
    }
    SECTION("both remove all links") { // local client state wins
        reset_collection({{Remove{dest_pk_3}}, {Remove{dest_pk_2}}, {Remove{dest_pk_1}}},
                         {{Remove{dest_pk_3}}, {Remove{dest_pk_2}}, {Remove{dest_pk_1}}}, {});
    }
    SECTION("local removes first link") { // local client state wins
        reset_collection({{Remove{dest_pk_1}}}, {}, {dest_pk_2, dest_pk_3});
    }
    SECTION("local removes middle link") { // local client state wins
        reset_collection({{Remove{dest_pk_2}}}, {}, {dest_pk_1, dest_pk_3});
    }
    SECTION("local removes last link") { // local client state wins
        reset_collection({{Remove{dest_pk_3}}}, {}, {dest_pk_1, dest_pk_2});
    }
    SECTION("remote removes first link") {
        reset_collection({}, {{Remove{dest_pk_1}}}, {dest_pk_2, dest_pk_3});
    }
    SECTION("remote removes middle link") {
        reset_collection({}, {{Remove{dest_pk_2}}}, {dest_pk_1, dest_pk_3});
    }
    SECTION("remote removes last link") {
        reset_collection({}, {{Remove{dest_pk_3}}}, {dest_pk_1, dest_pk_2});
    }
    SECTION("local adds a link with a null pk value") {
        test_reset->setup([&](SharedRealm realm) {
            test_type.reset_test_state();
            create_one_dest_object(realm, util::none);
            create_one_source_object(realm, source_pk, {});
        });
        reset_collection({Add{util::none}}, {}, {util::none});
    }
    SECTION("removal of different links") {
        std::vector<util::Optional<int64_t>> expected = {dest_pk_2};
        if constexpr (test_type_is_array) {
            expected = {dest_pk_2, dest_pk_3}; // local client state wins
        }
        reset_collection({Remove{dest_pk_1}}, {Remove{dest_pk_3}}, std::move(expected));
    }
    SECTION("local addition") {
        reset_collection({Add{dest_pk_4}}, {}, {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4});
    }
    SECTION("remote addition") {
        reset_collection({}, {Add{dest_pk_4}}, {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4});
    }
    SECTION("both addition of different items") {
        reset_collection({Add{dest_pk_4}}, {Add{dest_pk_5}, Remove{dest_pk_5}, Add{dest_pk_5}},
                         {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4, dest_pk_5});
    }
    SECTION("both addition of same items") {
        std::vector<util::Optional<int64_t>> expected = {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4};
        if constexpr (test_type_is_array) {
            expected = {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4, dest_pk_4};
        }
        // dictionary has added the new link to the same key on both sides
        reset_collection({Add{dest_pk_4}}, {Add{dest_pk_4}}, std::move(expected));
    }
    SECTION("local add/delete, remote add/delete/add different") {
        reset_collection({Add{dest_pk_4}, Remove{dest_pk_4}}, {Add{dest_pk_5}, Remove{dest_pk_5}, Add{dest_pk_5}},
                         {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_5});
    }
    SECTION("remote add/delete, local add") {
        reset_collection({Add{dest_pk_4}}, {Add{dest_pk_5}, Remove{dest_pk_5}},
                         {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_4});
    }
    SECTION("local remove, remote add") {
        std::vector<util::Optional<int64_t>> expected = {dest_pk_1, dest_pk_3, dest_pk_4, dest_pk_5};
        if constexpr (test_type_is_array) {
            expected = {dest_pk_1, dest_pk_3}; // local client state wins
        }
        reset_collection({Remove{dest_pk_2}}, {Add{dest_pk_4}, Add{dest_pk_5}}, std::move(expected));
    }
    SECTION("local adds link to remotely deleted object") {
        reset_collection({Add{dest_pk_4}}, {RemoveObject{"dest", dest_pk_4}}, {dest_pk_1, dest_pk_2, dest_pk_3}, 1);
    }
    SECTION("local clear") {
        reset_collection({Clear{}}, {}, {});
    }
    SECTION("remote clear") {
        reset_collection({}, {Clear{}}, {});
    }
    SECTION("both clear") {
        reset_collection({Clear{}}, {Clear{}}, {});
    }
    SECTION("both clear and add") {
        reset_collection({Clear{}, Add{dest_pk_1}}, {Clear{}, Add{dest_pk_2}}, {dest_pk_1});
    }
    SECTION("both clear and add/remove/add/add") {
        reset_collection({Clear{}, Add{dest_pk_1}, Remove{dest_pk_1}, Add{dest_pk_2}, Add{dest_pk_3}},
                         {Clear{}, Add{dest_pk_1}, Remove{dest_pk_1}, Add{dest_pk_2}, Add{dest_pk_3}},
                         {dest_pk_2, dest_pk_3});
    }
    SECTION("local add to remotely deleted object") {
        reset_collection({Add{dest_pk_4}}, {Add{dest_pk_4}, RemoveObject{"dest", dest_pk_4}},
                         {dest_pk_1, dest_pk_2, dest_pk_3}, 1);
    }
    SECTION("remote adds link to locally deleted object with link") {
        reset_collection({Add{dest_pk_4}, RemoveObject{"dest", dest_pk_4}}, {Add{dest_pk_4}, Add{dest_pk_5}},
                         {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_5}, 1);
    }
    SECTION("remote adds link to locally deleted object without link") {
        reset_collection({RemoveObject{"dest", dest_pk_4}}, {Add{dest_pk_4}, Add{dest_pk_5}},
                         {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_5}, 1);
    }
    if (test_mode == ClientResyncMode::Recover) {
        SECTION("local adds a list item and removes source object, remote modifies list") {
            reset_collection_removing_source_object({Add{dest_pk_4}, RemoveObject{"source", source_pk}},
                                                    {Add{dest_pk_5}});
        }
        SECTION("local erases list item then removes source object, remote modifies list") {
            reset_collection_removing_source_object({Remove{dest_pk_1}, RemoveObject{"source", source_pk}},
                                                    {Add{dest_pk_5}});
        }
        SECTION("remote removes source object, recover local modifications") {
            reset_collection_removing_source_object({Add{dest_pk_4}, Clear{}}, {RemoveObject{"source", source_pk}});
        }
        SECTION("remote removes source object, local attempts to ccpy over list state") {
            reset_collection_removing_source_object({Remove{dest_pk_1}}, {RemoveObject{"source", source_pk}});
        }
        SECTION("remote removes source object, local adds it back and modifies it") {
            reset_collection({Add{dest_pk_4}, RemoveObject{"source", source_pk}, CreateObject{"source", source_pk},
                              Add{dest_pk_1}},
                             {RemoveObject{"source", source_pk}}, {dest_pk_1});
        }
    }
    else if (test_mode == ClientResyncMode::DiscardLocal) {
        SECTION("remote removes source object") {
            reset_collection_removing_source_object({Add{dest_pk_4}}, {RemoveObject{"source", source_pk}});
        }
    }
    if constexpr (test_type_is_array) {
        SECTION("local moves on non-added elements causes a diff which overrides server changes") {
            reset_collection({Move{0, 1}, Add{dest_pk_5}}, {Add{dest_pk_4}},
                             {dest_pk_2, dest_pk_1, dest_pk_3, dest_pk_5});
        }
        SECTION("local moves on added elements can be merged with remote moves") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{3, 4}}, {Move{0, 1}},
                             {dest_pk_2, dest_pk_1, dest_pk_3, dest_pk_5, dest_pk_4});
        }
        SECTION("local moves on added elements can be merged with remote additions") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{3, 4}}, {Add{dest_pk_1}, Add{dest_pk_2}},
                             {dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_5, dest_pk_4, dest_pk_1, dest_pk_2});
        }
        SECTION("local moves on added elements can be merged with remote deletions") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{3, 4}}, {Remove{dest_pk_1}, Remove{dest_pk_2}},
                             {dest_pk_3, dest_pk_5, dest_pk_4});
        }
        SECTION("local move (down) on added elements can be merged with remote deletions") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{4, 3}}, {Remove{dest_pk_1}, Remove{dest_pk_2}},
                             {dest_pk_3, dest_pk_5, dest_pk_4});
        }
        SECTION("local move with delete on added elements can be merged with remote deletions") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{3, 4}, Remove{dest_pk_5}},
                             {Remove{dest_pk_1}, Remove{dest_pk_2}}, {dest_pk_3, dest_pk_4});
        }
        SECTION("local move (down) with delete on added elements can be merged with remote deletions") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, Move{4, 3}, Remove{dest_pk_5}},
                             {Remove{dest_pk_1}, Remove{dest_pk_2}}, {dest_pk_3, dest_pk_4});
        }
    }
}

template <typename T>
void set_embedded_list(const std::vector<T>& array_values, LnkLst& list)
{
    for (size_t i = 0; i < array_values.size(); ++i) {
        Obj link;
        if (i >= list.size()) {
            link = list.create_and_insert_linked_object(list.size());
        }
        else {
            link = list.get_object(i);
        }
        array_values[i].assign_to(link);
    }
    if (list.size() > array_values.size()) {
        if (array_values.size() == 0) {
            list.clear();
        }
        else {
            list.remove(array_values.size(), list.size());
        }
    }
}

template <typename T>
void combine_array_values(std::vector<T>& from, const std::vector<T>& to)
{
    auto it = from.begin();
    for (auto val : to) {
        it = ++from.insert(it, val);
    }
}

TEST_CASE("client reset with embedded object", "[client reset][local][embedded objects]") {
    if (!util::EventLoop::has_implementation())
        return;

    TestSyncManager init_sync_manager;
    SyncTestFile config(init_sync_manager.app(), "default");
    config.cache = false;
    config.automatic_change_notifications = false;
    ClientResyncMode test_mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
    CAPTURE(test_mode);
    config.sync_config->client_resync_mode = test_mode;

    ObjectSchema shared_class = {"object",
                                 {
                                     {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
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
         ObjectSchema::ObjectType::Embedded,
         {
             {"array", PropertyType::Int | PropertyType::Array},
             {"name", PropertyType::String | PropertyType::Nullable},
             {"link_to_embedded_object2", PropertyType::Object | PropertyType::Nullable, "EmbeddedObject2"},
             {"array_of_seconds", PropertyType::Object | PropertyType::Array, "EmbeddedObject2"},
             {"int_value", PropertyType::Int},
         }},
        {"EmbeddedObject2",
         ObjectSchema::ObjectType::Embedded,
         {
             {"notes", PropertyType::String | PropertyType::Dictionary | PropertyType::Nullable},
             {"set_of_ids", PropertyType::Set | PropertyType::ObjectId | PropertyType::Nullable},
             {"date", PropertyType::Date},
             {"top_level_link", PropertyType::Object | PropertyType::Nullable, "TopLevel"},
         }},
    };
    struct SecondLevelEmbeddedContent {
        using DictType = util::FlatMap<std::string, std::string>;
        DictType dict_values = DictType::container_type{{"key A", random_string(10)}, {"key B", random_string(10)}};
        std::set<ObjectId> set_of_objects = {ObjectId::gen(), ObjectId::gen()};
        Timestamp datetime = Timestamp{random_int(), 0};
        util::Optional<Mixed> pk_of_linked_object;
        void apply_recovery_from(const SecondLevelEmbeddedContent& other)
        {
            datetime = other.datetime;
            pk_of_linked_object = other.pk_of_linked_object;
            for (auto it : other.dict_values) {
                dict_values[it.first] = it.second;
            }
            for (auto oid : other.set_of_objects) {
                set_of_objects.insert(oid);
            }
        }
        void test(const SecondLevelEmbeddedContent& other) const
        {
            REQUIRE(datetime == other.datetime);
            REQUIRE(pk_of_linked_object == other.pk_of_linked_object);
            REQUIRE(set_of_objects == other.set_of_objects);
            REQUIRE(dict_values.size() == other.dict_values.size());
            for (auto kv : dict_values) {
                INFO("dict_value: (" << kv.first << ", " << kv.second << ")");
                auto it = other.dict_values.find(kv.first);
                REQUIRE(it != other.dict_values.end());
                REQUIRE(it->second == kv.second);
            }
        }
        static SecondLevelEmbeddedContent get_from(Obj second)
        {
            REALM_ASSERT(second.is_valid());
            SecondLevelEmbeddedContent content{};
            content.datetime = second.get<Timestamp>("date");
            ColKey top_link_col = second.get_table()->get_column_key("top_level_link");
            ObjKey actual_link = second.get<ObjKey>(top_link_col);
            if (actual_link) {
                TableRef top_table = second.get_table()->get_opposite_table(top_link_col);
                Obj actual_top_obj = top_table->get_object(actual_link);
                content.pk_of_linked_object = Mixed{actual_top_obj.get_primary_key()};
            }
            Dictionary dict = second.get_dictionary("notes");
            content.dict_values.clear();
            for (auto it : dict) {
                content.dict_values.insert({it.first.get_string(), it.second.get_string()});
            }
            Set<ObjectId> set = second.get_set<ObjectId>("set_of_ids");
            content.set_of_objects.clear();
            for (auto oid : set) {
                content.set_of_objects.insert(oid);
            }
            return content;
        }
        void assign_to(Obj second) const
        {
            if (second.get<Timestamp>("date") != datetime) {
                second.set("date", datetime);
            }
            ColKey top_link_col = second.get_table()->get_column_key("top_level_link");
            if (pk_of_linked_object) {
                TableRef top_table = second.get_table()->get_opposite_table(top_link_col);
                ObjKey top_link = top_table->find_primary_key(*(pk_of_linked_object));
                second.set(top_link_col, top_link);
            }
            else {
                if (!second.is_null(top_link_col)) {
                    second.set_null(top_link_col);
                }
            }
            Dictionary dict = second.get_dictionary("notes");
            for (auto it = dict.begin(); it != dict.end(); ++it) {
                if (std::find_if(dict_values.begin(), dict_values.end(), [&](auto& pair) {
                        return pair.first == (*it).first.get_string();
                    }) == dict_values.end()) {
                    dict.erase(it);
                }
            }
            for (auto& it : dict_values) {
                auto existing = dict.find(it.first);
                if (existing == dict.end() || (*existing).second.get_string() != it.second) {
                    dict.insert(it.first, it.second);
                }
            }
            Set<ObjectId> set = second.get_set<ObjectId>("set_of_ids");
            if (set_of_objects.empty()) {
                set.clear();
            }
            else {
                std::vector<size_t> indices, to_remove;
                set.sort(indices);
                for (size_t ndx : indices) {
                    if (set_of_objects.count(set.get(ndx)) == 0) {
                        to_remove.push_back(ndx);
                    }
                }
                std::sort(to_remove.rbegin(), to_remove.rend());
                for (auto ndx : to_remove) {
                    set.erase(set.get(ndx));
                }
                for (auto oid : set_of_objects) {
                    if (set.find(oid) == realm::npos) {
                        set.insert(oid);
                    }
                }
            }
        }
    };

    struct EmbeddedContent {
        std::string name = random_string(10);
        int64_t int_value = random_int();
        std::vector<Int> array_vals = {random_int(), random_int(), random_int()};
        util::Optional<SecondLevelEmbeddedContent> second_level = SecondLevelEmbeddedContent();
        std::vector<SecondLevelEmbeddedContent> array_of_seconds = {};
        void apply_recovery_from(const EmbeddedContent& other)
        {
            name = other.name;
            int_value = other.int_value;
            combine_array_values(array_vals, other.array_vals);
            if (second_level && other.second_level) {
                second_level->apply_recovery_from(*other.second_level);
            }
            else {
                second_level = other.second_level;
            }
        }
        void test(const EmbeddedContent& other) const
        {
            INFO("Checking EmbeddedContent" << name);
            REQUIRE(name == other.name);
            REQUIRE(int_value == other.int_value);
            REQUIRE(array_vals == other.array_vals);
            REQUIRE(array_of_seconds.size() == other.array_of_seconds.size());
            for (size_t i = 0; i < array_of_seconds.size(); ++i) {
                array_of_seconds[i].test(other.array_of_seconds[i]);
            }
            if (!second_level) {
                REQUIRE(!other.second_level);
            }
            else {
                REQUIRE(!!other.second_level);
                second_level->test(*other.second_level);
            }
        }
        static util::Optional<EmbeddedContent> get_from(Obj embedded)
        {
            util::Optional<EmbeddedContent> value;
            if (embedded.is_valid()) {
                value = EmbeddedContent{};
                value->name = embedded.get_any("name").get<StringData>();
                value->int_value = embedded.get_any("int_value").get<Int>();
                ColKey list_col = embedded.get_table()->get_column_key("array");
                value->array_vals = embedded.get_list_values<Int>(list_col);

                ColKey link2_col = embedded.get_table()->get_column_key("link_to_embedded_object2");
                Obj second = embedded.get_linked_object(link2_col);
                value->second_level = util::none;
                if (second.is_valid()) {
                    value->second_level = SecondLevelEmbeddedContent::get_from(second);
                }
                auto list = embedded.get_linklist("array_of_seconds");
                for (size_t i = 0; i < list.size(); ++i) {
                    value->array_of_seconds.push_back(SecondLevelEmbeddedContent::get_from(list.get_object(i)));
                }
            }
            return value;
        }
        void assign_to(Obj embedded) const
        {
            if (embedded.get<StringData>("name") != name) {
                embedded.set<StringData>("name", name);
            }
            if (embedded.get<Int>("int_value") != int_value) {
                embedded.set<Int>("int_value", int_value);
            }
            ColKey list_col = embedded.get_table()->get_column_key("array");
            if (embedded.get_list_values<Int>(list_col) != array_vals) {
                embedded.set_list_values<Int>(list_col, array_vals);
            }
            ColKey link2_col = embedded.get_table()->get_column_key("link_to_embedded_object2");
            if (second_level) {
                Obj second = embedded.get_linked_object(link2_col);
                if (!second) {
                    second = embedded.create_and_set_linked_object(link2_col);
                }
                second_level->assign_to(second);
            }
            else {
                embedded.set_null(link2_col);
            }
            auto list = embedded.get_linklist("array_of_seconds");
            set_embedded_list(array_of_seconds, list);
        }
    };
    struct TopLevelContent {
        util::Optional<EmbeddedContent> link_value = EmbeddedContent();
        std::vector<EmbeddedContent> array_values{3};
        using DictType = util::FlatMap<std::string, util::Optional<EmbeddedContent>>;
        DictType dict_values = DictType::container_type{
            {"foo", EmbeddedContent()},
            {"bar", EmbeddedContent()},
            {"baz", EmbeddedContent()},
        };
        void apply_recovery_from(const TopLevelContent& other)
        {
            combine_array_values(array_values, other.array_values);
            for (auto it : other.dict_values) {
                dict_values[it.first] = it.second;
            }
            if (link_value && other.link_value) {
                link_value->apply_recovery_from(*other.link_value);
            }
            else if (link_value) {
                link_value = other.link_value;
            }
            // assuming starting from an initial value, if the link_value is null, then it was intentionally deleted.
        }
        void test(const TopLevelContent& other) const
        {
            if (link_value) {
                INFO("checking TopLevelContent.link_value");
                REQUIRE(!!other.link_value);
                link_value->test(*other.link_value);
            }
            else {
                REQUIRE(!other.link_value);
            }
            REQUIRE(array_values.size() == other.array_values.size());
            for (size_t i = 0; i < array_values.size(); ++i) {
                INFO("checking array_values: " << i);
                array_values[i].test(other.array_values[i]);
            }
            REQUIRE(dict_values.size() == other.dict_values.size());
            for (auto it : dict_values) {
                INFO("checking dict_values: " << it.first);
                auto found = other.dict_values.find(it.first);
                REQUIRE(found != other.dict_values.end());
                if (it.second) {
                    REQUIRE(!!found->second);
                    it.second->test(*found->second);
                }
                else {
                    REQUIRE(!found->second);
                }
            }
        }
        static TopLevelContent get_from(Obj obj)
        {
            TopLevelContent content;
            Obj embedded_link = obj.get_linked_object("embedded_obj");
            content.link_value = EmbeddedContent::get_from(embedded_link);
            auto list = obj.get_linklist("array_of_objs");
            content.array_values.clear();

            for (size_t i = 0; i < list.size(); ++i) {
                Obj link = list.get_object(i);
                content.array_values.push_back(*EmbeddedContent::get_from(link));
            }
            auto dict = obj.get_dictionary("embedded_dict");
            content.dict_values.clear();
            for (auto it : dict) {
                Obj link = dict.get_object(it.first.get_string());
                content.dict_values.insert({it.first.get_string(), EmbeddedContent::get_from(link)});
            }
            return content;
        }
        void assign_to(Obj obj) const
        {
            ColKey link_col = obj.get_table()->get_column_key("embedded_obj");
            if (!link_value) {
                obj.set_null(link_col);
            }
            else {
                Obj embedded_link = obj.get_linked_object(link_col);
                if (!embedded_link) {
                    embedded_link = obj.create_and_set_linked_object(link_col);
                }
                link_value->assign_to(embedded_link);
            }
            auto list = obj.get_linklist("array_of_objs");
            set_embedded_list(array_values, list);
            auto dict = obj.get_dictionary("embedded_dict");
            for (auto it = dict.begin(); it != dict.end();) {
                if (dict_values.find((*it).first.get_string()) == dict_values.end()) {
                    it = dict.erase(it);
                }
                else {
                    ++it;
                }
            }
            for (auto it : dict_values) {
                if (it.second) {
                    auto embedded = dict.get_object(it.first);
                    if (!embedded) {
                        embedded = dict.create_and_insert_linked_object(it.first);
                    }
                    it.second->assign_to(embedded);
                }
                else {
                    dict.insert(it.first, Mixed{});
                }
            }
        }
    };

    SyncTestFile config2(init_sync_manager.app(), "default");
    config2.schema = config.schema;

    std::unique_ptr<reset_utils::TestClientReset> test_reset =
        reset_utils::make_fake_local_client_reset(config, config2);

    auto get_top_object = [](SharedRealm realm) {
        advance_and_notify(*realm);
        TableRef table = get_table(*realm, "TopLevel");
        REQUIRE(table->size() == 1);
        Obj obj = *table->begin();
        return obj;
    };

    using StateList = std::vector<TopLevelContent>;
    auto reset_embedded_object = [&](StateList local_content, StateList remote_content,
                                     TopLevelContent expected_recovered) {
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                Obj obj = get_top_object(local_realm);
                for (auto& s : local_content) {
                    s.assign_to(obj);
                }
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                Obj obj = get_top_object(remote_realm);
                for (auto& s : remote_content) {
                    s.assign_to(obj);
                }
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                Obj obj = get_top_object(local_realm);
                TopLevelContent actual = TopLevelContent::get_from(obj);
                if (test_mode == ClientResyncMode::Recover) {
                    actual.test(expected_recovered);
                }
                else if (test_mode == ClientResyncMode::DiscardLocal) {
                    REQUIRE(remote_content.size() > 0);
                    actual.test(remote_content.back());
                }
                else {
                    REALM_UNREACHABLE();
                }
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

    SECTION("identical changes") {
        TopLevelContent state;
        TopLevelContent expected_recovered = state;
        expected_recovered.apply_recovery_from(state);
        reset_embedded_object({state}, {state}, expected_recovered);
    }
    SECTION("modify every embedded property") {
        TopLevelContent local, remote;
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("remote nullifies embedded links") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.link_value.reset();
        for (auto& val : remote.dict_values) {
            val.second.reset();
        }
        remote.array_values.clear();
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local nullifies embedded links") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.link_value.reset();
        for (auto& val : local.dict_values) {
            val.second.reset();
        }
        local.array_values.clear();
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("remote adds embedded objects") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.dict_values["new key1"] = {EmbeddedContent{}};
        remote.dict_values["new key2"] = {EmbeddedContent{}};
        remote.dict_values["new key3"] = {};
        remote.array_values.push_back({EmbeddedContent{}});
        remote.array_values.push_back({});
        remote.array_values.push_back({EmbeddedContent{}});
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local adds some embedded objects") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.dict_values["new key1"] = {EmbeddedContent{}};
        local.dict_values["new key2"] = {EmbeddedContent{}};
        local.dict_values["new key3"] = {};
        local.array_values.push_back({EmbeddedContent{}});
        local.array_values.push_back({});
        local.array_values.push_back({EmbeddedContent{}});
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("both add conflicting embedded objects") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.dict_values["new key1"] = {EmbeddedContent{}};
        local.dict_values["new key2"] = {EmbeddedContent{}};
        local.dict_values["new key3"] = {};
        local.array_values.push_back({EmbeddedContent{}});
        local.array_values.push_back({});
        local.array_values.push_back({EmbeddedContent{}});
        remote.dict_values["new key1"] = {EmbeddedContent{}};
        remote.dict_values["new key2"] = {EmbeddedContent{}};
        remote.dict_values["new key3"] = {};
        remote.array_values.push_back({EmbeddedContent{}});
        remote.array_values.push_back({});
        remote.array_values.push_back({EmbeddedContent{}});
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local modifies an embedded object which is removed by the remote") {
        TopLevelContent local, remote;
        local.link_value->name = "modified value";
        remote.link_value = util::none;
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local modifies a deep embedded object which is removed by the remote") {
        TopLevelContent local, remote;
        local.link_value->second_level->datetime = Timestamp{1, 1};
        remote.link_value = util::none;
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local modifies a deep embedded object which is removed at the second level by the remote") {
        TopLevelContent local, remote;
        local.link_value->second_level->datetime = Timestamp{1, 1};
        remote.link_value->second_level = util::none;
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("with shared initial state") {
        TopLevelContent initial;
        test_reset->setup([&](SharedRealm realm) {
            auto table = get_table(*realm, "TopLevel");
            REQUIRE(table);
            auto obj = table->create_object_with_primary_key(pk_val);
            initial.assign_to(obj);
        });
        TopLevelContent local = initial;
        TopLevelContent remote = initial;

        SECTION("local modifications to an embedded object through a dictionary which is removed by the remote are "
                "ignored") {
            local.dict_values["foo"]->name = "modified";
            local.dict_values["foo"]->second_level->datetime = Timestamp{1, 1};
            local.dict_values["foo"]->array_vals.push_back(random_int());
            local.dict_values["foo"]->array_vals.erase(local.dict_values["foo"]->array_vals.begin());
            local.dict_values["foo"]->second_level->dict_values.erase(
                local.dict_values["foo"]->second_level->dict_values.begin());
            local.dict_values["foo"]->second_level->set_of_objects.clear();
            remote.dict_values["foo"] = util::none;
            TopLevelContent expected_recovered = remote;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local modifications to an embedded object through a linklist element which is removed by the remote "
                "triggers a list copy") {
            local.array_values.begin()->name = "modified";
            local.array_values.begin()->second_level->datetime = Timestamp{1, 1};
            local.array_values.begin()->array_vals.push_back(random_int());
            local.array_values.begin()->array_vals.erase(local.array_values.begin()->array_vals.begin());
            local.array_values.begin()->second_level->dict_values.erase(
                local.array_values.begin()->second_level->dict_values.begin());
            local.array_values.begin()->second_level->set_of_objects.clear();
            remote.array_values.erase(remote.array_values.begin());
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local ArraySet to an embedded object through a deep link->linklist element which is removed by the "
                "remote "
                "triggers a list copy") {
            local.link_value->array_vals[0] = 12345;
            remote.link_value->array_vals.erase(remote.link_value->array_vals.begin());
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local ArrayErase to an embedded object through a deep link->linklist element which is removed by "
                "the remote "
                "triggers a list copy") {
            local.link_value->array_vals.erase(local.link_value->array_vals.begin());
            remote.link_value->array_vals.clear();
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local modifications to an embedded object through a linklist cleared by the remote triggers a list "
                "copy") {
            local.array_values.begin()->name = "modified";
            local.array_values.begin()->second_level->datetime = Timestamp{1, 1};
            local.array_values.begin()->array_vals.push_back(random_int());
            local.array_values.begin()->array_vals.erase(local.array_values.begin()->array_vals.begin());
            local.array_values.begin()->second_level->dict_values.erase(
                local.array_values.begin()->second_level->dict_values.begin());
            local.array_values.begin()->second_level->set_of_objects.clear();
            remote.array_values.clear();
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("moving preexisting list items triggers a list copy") {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    REQUIRE(list.size() == 3);
                    list.move(0, 1);
                    list.move(1, 2);
                    list.move(1, 0);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size()); // any change here is lost
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    TopLevelContent actual = TopLevelContent::get_from(obj);
                    if (test_mode == ClientResyncMode::Recover) {
                        TopLevelContent expected_recovered = local;
                        std::iter_swap(expected_recovered.array_values.begin(),
                                       expected_recovered.array_values.begin() + 1);
                        std::iter_swap(expected_recovered.array_values.begin() + 1,
                                       expected_recovered.array_values.begin() + 2);
                        std::iter_swap(expected_recovered.array_values.begin() + 1,
                                       expected_recovered.array_values.begin());
                        actual.test(expected_recovered);
                    }
                    else {
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("inserting new embedded objects into a list which has indices modified by the remote are recovered") {
            EmbeddedContent new_element1, new_element2;
            local.array_values.insert(local.array_values.end(), new_element1);
            local.array_values.insert(local.array_values.begin(), new_element2);
            remote.array_values.erase(remote.array_values.begin());
            remote.array_values.erase(remote.array_values.begin());
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    Obj obj = get_top_object(local);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(3);
                    new_element1.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(0);
                    new_element2.assign_to(embedded);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size() - 1);
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    TopLevelContent actual = TopLevelContent::get_from(obj);
                    if (test_mode == ClientResyncMode::Recover) {
                        TopLevelContent expected_recovered = remote;
                        expected_recovered.array_values.insert(expected_recovered.array_values.end(), new_element1);
                        expected_recovered.array_values.insert(expected_recovered.array_values.begin(), new_element2);
                        actual.test(expected_recovered);
                    }
                    else {
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("local list clear removes remotely inserted objects") {
            EmbeddedContent new_element_local, new_element_remote;
            local.array_values.clear();
            TopLevelContent local2 = local;
            local2.array_values.push_back(new_element_local);
            remote.array_values.erase(remote.array_values.begin());
            remote.array_values.push_back(new_element_remote); // lost via local.clear()
            TopLevelContent expected_recovered = local2;
            reset_embedded_object({local, local2}, {remote}, expected_recovered);
        }
        SECTION("local modification of a dictionary value which is removed by the remote") {
            local.dict_values["foo"] = EmbeddedContent{};
            remote.dict_values.erase("foo");
            TopLevelContent expected_recovered = remote;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local delete of a dictionary value which is removed by the remote") {
            local.dict_values.erase("foo");
            remote.dict_values.erase("foo");
            TopLevelContent expected_recovered = remote;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local delete of a dictionary value which is modified by the remote") {
            local.dict_values.erase("foo");
            remote.dict_values["foo"] = EmbeddedContent{};
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("both modify a dictionary value") {
            EmbeddedContent new_local, new_remote;
            local.dict_values["foo"] = new_local;
            remote.dict_values["foo"] = new_remote;
            TopLevelContent expected_recovered = remote;
            expected_recovered.dict_values["foo"]->apply_recovery_from(*local.dict_values["foo"]);
            // a verbatim list copy is triggered by modifications to items which were not just inserted
            expected_recovered.dict_values["foo"]->array_vals = local.dict_values["foo"]->array_vals;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        std::vector<std::string> keys = {"new key", "", "\0"};
        for (auto key : keys) {
            SECTION(util::format("both add the same dictionary key: '%1'", key)) {
                EmbeddedContent new_local, new_remote;
                local.dict_values[key] = new_local;
                remote.dict_values[key] = new_remote;
                TopLevelContent expected_recovered = remote;
                expected_recovered.dict_values[key]->apply_recovery_from(*local.dict_values[key]);
                // a verbatim list copy is triggered by modifications to items which were not just inserted
                expected_recovered.dict_values[key]->array_vals = local.dict_values[key]->array_vals;
                expected_recovered.dict_values[key]->second_level = local.dict_values[key]->second_level;
                reset_embedded_object({local}, {remote}, expected_recovered);
            }
        }
        SECTION("deep modifications to inserted and swaped list items are recovered") {
            EmbeddedContent local_added_at_begin, local_added_at_end, local_added_before_end, remote_added;
            size_t list_end = initial.array_values.size();
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    Obj obj = get_top_object(local);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_at_begin.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(list_end - 1);
                    local_added_before_end.assign_to(embedded); // this item is needed here so that move does not
                                                                // trigger a copy of the list
                    embedded = list.create_and_insert_linked_object(list_end);
                    local_added_at_end.assign_to(embedded);
                    local->commit_transaction();
                    local->begin_transaction();
                    list.swap(0,
                              list_end); // generates two move instructions, move(0, list_end), move(list_end - 1, 0)
                    local->commit_transaction();
                    local->begin_transaction();
                    local_added_at_end.name = "should be at begin now";
                    local_added_at_begin.name = "should be at end now";
                    local_added_at_end.assign_to(list.get_object(0));
                    local_added_at_begin.assign_to(list.get_object(list_end));
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size()); // individual ArrayErase instructions, not a clear.
                    remote_added.name = "remote added at zero, should end up in the middle of the list";
                    remote_added.assign_to(list.create_and_insert_linked_object(0));
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    if (test_mode == ClientResyncMode::Recover) {
                        auto list = obj.get_linklist("array_of_objs");
                        REQUIRE(list.size() == 4);
                        EmbeddedContent embedded_0 = *EmbeddedContent::get_from(list.get_object(0));
                        EmbeddedContent embedded_1 = *EmbeddedContent::get_from(list.get_object(1));
                        EmbeddedContent embedded_2 = *EmbeddedContent::get_from(list.get_object(2));
                        EmbeddedContent embedded_3 = *EmbeddedContent::get_from(list.get_object(3));
                        embedded_0.test(local_added_at_end); // local added at end, moved to 0
                        embedded_1.test(remote_added); // remote added at 0, bumped to 1 by recovered insert at 0
                        embedded_2.test(local_added_before_end); // local added at 2, not moved
                        embedded_3.test(local_added_at_begin);   // local added at 0, moved to end
                    }
                    else {
                        TopLevelContent actual = TopLevelContent::get_from(obj);
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("deep modifications to inserted and moved list items are recovered") {
            EmbeddedContent local_added_at_begin, local_added_at_end, remote_added;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_at_begin.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(list.size());
                    local_added_at_end.assign_to(embedded);
                    local_realm->commit_transaction();
                    advance_and_notify(*local_realm);
                    local_realm->begin_transaction();
                    list.move(list.size() - 1, 0);
                    local_realm->commit_transaction();
                    advance_and_notify(*local_realm);
                    local_realm->begin_transaction();
                    local_added_at_end.name = "added at end, moved to 0";
                    local_added_at_begin.name = "added at 0, bumped to 1";
                    local_added_at_end.assign_to(list.get_object(0));
                    local_added_at_begin.assign_to(list.get_object(1));
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size()); // individual ArrayErase instructions, not a clear.
                    remote_added.name = "remote added at zero, should end up at the end of the list";
                    remote_added.assign_to(list.create_and_insert_linked_object(0));
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    if (test_mode == ClientResyncMode::Recover) {
                        auto list = obj.get_linklist("array_of_objs");
                        REQUIRE(list.size() == 3);
                        EmbeddedContent embedded_0 = *EmbeddedContent::get_from(list.get_object(0));
                        EmbeddedContent embedded_1 = *EmbeddedContent::get_from(list.get_object(1));
                        EmbeddedContent embedded_2 = *EmbeddedContent::get_from(list.get_object(2));
                        embedded_0.test(local_added_at_end);   // local added at end, moved to 0
                        embedded_1.test(local_added_at_begin); // local added at begin, bumped up by move
                        embedded_2.test(
                            remote_added); // remote added at 0, bumped to 2 by recovered insert at 0 and move to 0
                    }
                    else {
                        TopLevelContent actual = TopLevelContent::get_from(obj);
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("removing an added list item does not trigger a list copy") {
            EmbeddedContent local_added_and_removed, local_added;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_and_removed.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(1);
                    local_added.assign_to(embedded);
                    local_realm->commit_transaction();
                    local_realm->begin_transaction();
                    list.remove(0);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size()); // individual ArrayErase instructions, not a clear.
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    TopLevelContent actual = TopLevelContent::get_from(obj);
                    if (test_mode == ClientResyncMode::Recover) {
                        TopLevelContent expected_recovered = remote;
                        expected_recovered.array_values.insert(expected_recovered.array_values.begin(), local_added);
                        actual.test(expected_recovered);
                    }
                    else {
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("removing a preexisting list item triggers a list copy") {
            EmbeddedContent remote_updated_item_0, local_added;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0);
                    list.remove(0);
                    auto embedded = list.create_and_insert_linked_object(1);
                    local_added.assign_to(embedded);
                    local = TopLevelContent::get_from(obj);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    // any change made to the list here is overwritten by the list copy
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(1, list.size()); // individual ArrayErase instructions, not a clear.
                    remote_updated_item_0.assign_to(list.get_object(0));
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    TopLevelContent actual = TopLevelContent::get_from(obj);
                    if (test_mode == ClientResyncMode::Recover) {
                        actual.test(local);
                    }
                    else {
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("adding and removing a list item when the remote removes the base object has no effect") {
            EmbeddedContent local_added_at_begin;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_at_begin.assign_to(embedded);
                    local_realm->commit_transaction();
                    local_realm->begin_transaction();
                    list.remove(0);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    // any change made to the list here is overwritten by the list copy
                    Obj obj = get_top_object(remote_realm);
                    obj.remove();
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    advance_and_notify(*local_realm);
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 0);
                })
                ->run();
        }
        SECTION("removing a preexisting list item when the remote removes the base object has no effect") {
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    // any change made to the list here is overwritten by the list copy
                    Obj obj = get_top_object(remote_realm);
                    obj.remove();
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    advance_and_notify(*local_realm);
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 0);
                })
                ->run();
        }
        SECTION("modifications to an embedded object are ignored when the base object is removed") {
            EmbeddedContent local_modifications;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    local_modifications.assign_to(list.get_object(0));
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    // any change made to the list here is overwritten by the list copy
                    Obj obj = get_top_object(remote_realm);
                    obj.remove();
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    advance_and_notify(*local_realm);
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 0);
                })
                ->run();
        }
        SECTION("changes made through two layers of embedded lists can be recovered") {
            EmbeddedContent local_added_at_0, local_added_at_1, remote_added;
            local_added_at_0.name = "added at 0, moved to 1";
            local_added_at_0.array_of_seconds = {{}, {}};
            local_added_at_1.name = "added at 1, bumped to 0";
            local_added_at_1.array_of_seconds = {{}, {}, {}};
            remote_added.array_of_seconds = {{}, {}};
            SecondLevelEmbeddedContent modified, inserted;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_at_0.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(1);
                    local_added_at_1.assign_to(embedded);
                    local_realm->commit_transaction();
                    local_realm->begin_transaction();
                    auto list_of_seconds = embedded.get_linklist("array_of_seconds");
                    list_of_seconds.move(0, 1);
                    std::iter_swap(local_added_at_1.array_of_seconds.begin(),
                                   local_added_at_1.array_of_seconds.begin() + 1);
                    local_realm->commit_transaction();
                    local_realm->begin_transaction();
                    list.move(0, 1);
                    local_realm->commit_transaction();
                    local_realm->begin_transaction();
                    modified.assign_to(list_of_seconds.get_object(0));
                    auto new_second = list_of_seconds.create_and_insert_linked_object(0);
                    inserted.assign_to(new_second);
                    local_added_at_1.array_of_seconds[0] = modified;
                    local_added_at_1.array_of_seconds.insert(local_added_at_1.array_of_seconds.begin(), inserted);
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    list.remove(0, list.size()); // individual ArrayErase instructions, not a clear.
                    remote_added.name = "remote added at zero, should end up at the end of the list";
                    remote_added.assign_to(list.create_and_insert_linked_object(0));
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    if (test_mode == ClientResyncMode::Recover) {
                        auto list = obj.get_linklist("array_of_objs");
                        REQUIRE(list.size() == 3);
                        EmbeddedContent embedded_0 = *EmbeddedContent::get_from(list.get_object(0));
                        EmbeddedContent embedded_1 = *EmbeddedContent::get_from(list.get_object(1));
                        EmbeddedContent embedded_2 = *EmbeddedContent::get_from(list.get_object(2));
                        embedded_0.test(local_added_at_1); // local added at end, moved to 0
                        embedded_1.test(local_added_at_0); // local added at begin, bumped up by move
                        embedded_2.test(remote_added);     // remote added at 0, bumped to 2 by recovered
                    }
                    else {
                        TopLevelContent actual = TopLevelContent::get_from(obj);
                        actual.test(remote);
                    }
                })
                ->run();
        }
        SECTION("insertions to a preexisting object through two layers of embedded lists triggers a list copy") {
            SecondLevelEmbeddedContent local_added, remote_added;
            test_reset
                ->make_local_changes([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    local_added.assign_to(
                        list.get_object(0).get_linklist("array_of_seconds").create_and_insert_linked_object(0));
                })
                ->make_remote_changes([&](SharedRealm remote_realm) {
                    Obj obj = get_top_object(remote_realm);
                    auto list = obj.get_linklist("array_of_objs");
                    remote_added.assign_to(
                        list.get_object(0).get_linklist("array_of_seconds").create_and_insert_linked_object(0));
                    list.move(0, 1);
                    remote = TopLevelContent::get_from(obj);
                })
                ->on_post_reset([&](SharedRealm local_realm) {
                    Obj obj = get_top_object(local_realm);
                    if (test_mode == ClientResyncMode::Recover) {
                        auto list = obj.get_linklist("array_of_objs");
                        REQUIRE(list.size() == 3);
                        EmbeddedContent embedded_0 = *EmbeddedContent::get_from(list.get_object(0));
                        EmbeddedContent embedded_1 = *EmbeddedContent::get_from(list.get_object(1));
                        EmbeddedContent embedded_2 = *EmbeddedContent::get_from(list.get_object(2));
                        REQUIRE(embedded_0.array_of_seconds.size() == 1);
                        embedded_0.array_of_seconds[0].test(local_added);
                        REQUIRE(embedded_1.array_of_seconds.size() ==
                                0); // remote changes overwritten by local list copy
                        REQUIRE(embedded_2.array_of_seconds.size() == 0);
                    }
                    else {
                        TopLevelContent actual = TopLevelContent::get_from(obj);
                        actual.test(remote);
                    }
                })
                ->run();
        }

        SECTION("modifications to a preexisting object through two layers of embedded lists triggers a list copy") {
            SecondLevelEmbeddedContent preexisting_item, local_modified, remote_added;
            initial.array_values[0].array_of_seconds.push_back(preexisting_item);
            const size_t initial_item_pos = initial.array_values[0].array_of_seconds.size() - 1;
            local = initial;
            remote = initial;
            local.array_values[0].array_of_seconds[initial_item_pos] = local_modified;
            remote.array_values[0].array_of_seconds.push_back(remote_added); // overwritten by local!
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }

        SECTION("add int") {
            auto add_to_dict_item = [&](SharedRealm realm, std::string key, int64_t addition) {
                Obj obj = get_top_object(realm);
                auto dict = obj.get_dictionary("embedded_dict");
                auto embedded = dict.get_object(key);
                REQUIRE(!!embedded);
                embedded.add_int("int_value", addition);
                return TopLevelContent::get_from(obj);
            };
            TopLevelContent expected_recovered;
            const std::string existing_key = "foo";

            test_reset->on_post_reset([&](SharedRealm local_realm) {
                Obj obj = get_top_object(local_realm);
                TopLevelContent actual = TopLevelContent::get_from(obj);
                actual.test(test_mode == ClientResyncMode::Recover ? expected_recovered : initial);
            });
            int64_t initial_value = initial.dict_values[existing_key]->int_value;
            std::mt19937_64 engine(std::random_device{}());
            std::uniform_int_distribution<int64_t> rng(-10'000'000'000, 10'000'000'000);

            int64_t addition = rng(engine);
            SECTION("local add_int to an existing dictionary item") {
                INFO("adding " << initial_value << " with " << addition);
                expected_recovered = initial;
                expected_recovered.dict_values[existing_key]->int_value += addition;
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        add_to_dict_item(local, existing_key, addition);
                    })
                    ->run();
            }
            SECTION("local and remote both create the same dictionary item and add to it") {
                int64_t remote_addition = rng(engine);
                INFO("adding " << initial_value << " with local " << addition << " and remote " << remote_addition);
                expected_recovered = initial;
                expected_recovered.dict_values[existing_key]->int_value += (addition + remote_addition);
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        add_to_dict_item(local, existing_key, addition);
                    })
                    ->make_remote_changes([&](SharedRealm remote) {
                        initial = add_to_dict_item(remote, existing_key, remote_addition);
                    })
                    ->run();
            }
            SECTION("local add_int on a dictionary item which the remote removed is ignored") {
                INFO("adding " << initial_value << " with " << addition);
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        add_to_dict_item(local, existing_key, addition);
                    })
                    ->make_remote_changes([&](SharedRealm remote_realm) {
                        Obj obj = get_top_object(remote_realm);
                        auto dict = obj.get_dictionary("embedded_dict");
                        dict.erase(Mixed{existing_key});
                        initial = TopLevelContent::get_from(obj);
                        expected_recovered = initial;
                    })
                    ->run();
            }
            SECTION("local add_int on a dictionary item when the entire root object is removed by the remote removed "
                    "is ignored") {
                INFO("adding " << initial_value << " with " << addition);
                test_reset
                    ->make_local_changes([&](SharedRealm local) {
                        add_to_dict_item(local, existing_key, addition);
                    })
                    ->make_remote_changes([&](SharedRealm remote_realm) {
                        Obj obj = get_top_object(remote_realm);
                        TableRef table = obj.get_table();
                        obj.remove();
                        REQUIRE(table->size() == 0);
                    })
                    ->on_post_reset([&](SharedRealm local_realm) {
                        advance_and_notify(*local_realm);
                        TableRef table = get_table(*local_realm, "TopLevel");
                        REQUIRE(table->size() == 0);
                    })
                    ->run();
            }
        }
    }
    SECTION("remote adds a top level link cycle") {
        TopLevelContent local;
        TopLevelContent remote = local;
        remote.link_value->second_level->pk_of_linked_object = Mixed{pk_val};
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        // the remote change exists because no local instruction set the value to anything (default)
        expected_recovered.link_value->second_level->pk_of_linked_object = Mixed{pk_val};
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("local adds a top level link cycle") {
        TopLevelContent local;
        TopLevelContent remote = local;
        local.link_value->second_level->pk_of_linked_object = Mixed{pk_val};
        TopLevelContent expected_recovered = remote;
        expected_recovered.apply_recovery_from(local);
        reset_embedded_object({local}, {remote}, expected_recovered);
    }
    SECTION("server adds embedded object classes") {
        SyncTestFile config2(init_sync_manager.app(), "default");
        config2.schema = config.schema;
        config.schema = Schema{shared_class};
        test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        TopLevelContent remote_content;

        test_reset
            ->make_remote_changes([&](SharedRealm remote) {
                advance_and_notify(*remote);
                TableRef table = get_table(*remote, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                REQUIRE(table->size() == 1);
                remote_content.assign_to(obj);
            })
            ->on_post_reset([&](SharedRealm local) {
                advance_and_notify(*local);
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                Obj obj = *table->begin();
                TopLevelContent actual = TopLevelContent::get_from(obj);
                actual.test(remote_content);
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
            local_content.assign_to(obj);
        });
        if (test_mode == ClientResyncMode::DiscardLocal) {
            REQUIRE_THROWS_WITH(test_reset->run(), "Client reset cannot recover when classes have been removed: "
                                                   "{EmbeddedObject, EmbeddedObject2, TopLevel}");
        }
        else {
            // In recovery mode, AddTable should succeed if the server is in dev mode, and fail
            // if the server is in production which in that case the changes will be rejected.
            // Since this is a fake reset, it always succeeds here.
            test_reset
                ->on_post_reset([&](SharedRealm local) {
                    TableRef table = get_table(*local, "TopLevel");
                    REQUIRE(table->size() == 1);
                })
                ->run();
        }
    }
}
