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

#include <collection_fixtures.hpp>
#include <util/event_loop.hpp>
#include <util/index_helpers.hpp>
#include <util/test_file.hpp>
#include <util/test_utils.hpp>
#include <util/sync/baas_admin_api.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/util/scheduler.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/pending_reset_store.hpp>
#include <realm/sync/network/websocket.hpp>

#include <realm/util/flat_map.hpp>
#include <realm/util/overload.hpp>

#include <catch2/catch_all.hpp>

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
        return realm::util::format("SyncError(%1), is_fatal: %2, with message: '%3'", value->status.code_string(),
                                   value->is_fatal, value->status.reason());
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

TEST_CASE("sync: large reset with recovery is restartable", "[sync][pbs][client reset][baas]") {
    const reset_utils::Partition partition{"realm_id", random_string(20)};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::String},
             partition_prop,
         }},
    };

    auto server_app_config = minimal_app_config("client_reset_tests", schema);
    server_app_config.partition_key = partition_prop;
    TestAppSession test_app_session(create_app(server_app_config));
    auto app = test_app_session.app();

    create_user_and_log_in(app);
    SyncTestFile realm_config(app->current_user(), partition.value, schema);
    realm_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    realm_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        if (err.status == ErrorCodes::ConnectionClosed) {
            return;
        }

        if (err.server_requests_action == sync::ProtocolErrorInfo::Action::Warning ||
            err.server_requests_action == sync::ProtocolErrorInfo::Action::Transient) {
            return;
        }

        FAIL(util::format("got error from server: %1", err.status));
    };

    auto realm = Realm::get_shared_realm(realm_config);
    std::vector<ObjectId> expected_obj_ids;
    {
        auto obj_id = ObjectId::gen();
        expected_obj_ids.push_back(obj_id);
        realm->begin_transaction();
        CppContext c(realm);
        Object::create(c, realm, "object",
                       std::any(AnyDict{{"_id", obj_id},
                                        {"value", std::string{"hello world"}},
                                        {partition.property_name, partition.value}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
        reset_utils::wait_for_object_to_persist_to_atlas(app->current_user(), test_app_session.app_session(),
                                                         "object", {{"_id", obj_id}});
        realm->sync_session()->pause();
    }

    reset_utils::trigger_client_reset(test_app_session.app_session(), realm);
    {
        SyncTestFile realm_config(app->current_user(), partition.value, schema);
        auto second_realm = Realm::get_shared_realm(realm_config);

        second_realm->begin_transaction();
        CppContext c(second_realm);
        for (size_t i = 0; i < 100; ++i) {
            auto obj_id = ObjectId::gen();
            expected_obj_ids.push_back(obj_id);
            Object::create(c, second_realm, "object",
                           std::any(AnyDict{{"_id", obj_id},
                                            {"value", random_string(1024 * 128)},
                                            {partition.property_name, partition.value}}));
        }
        second_realm->commit_transaction();

        wait_for_upload(*second_realm);
    }

    realm->sync_session()->resume();
    timed_wait_for([&] {
        return util::File::exists(_impl::client_reset::get_fresh_path_for(realm_config.path));
    });
    realm->sync_session()->pause();
    realm->sync_session()->resume();
    wait_for_upload(*realm);
    wait_for_download(*realm);

    realm->refresh();
    auto table = realm->read_group().get_table("class_object");
    REQUIRE(table->size() == expected_obj_ids.size());
    std::vector<ObjectId> found_object_ids;
    for (const auto& obj : *table) {
        found_object_ids.push_back(obj.get_primary_key().get_object_id());
    }

    std::stable_sort(expected_obj_ids.begin(), expected_obj_ids.end());
    std::stable_sort(found_object_ids.begin(), found_object_ids.end());
    REQUIRE(expected_obj_ids == found_object_ids);
}

TEST_CASE("sync: pending client resets are cleared when downloads are complete", "[sync][pbs][client reset][baas]") {
    const reset_utils::Partition partition{"realm_id", random_string(20)};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             partition_prop,
         }},
    };

    auto server_app_config = minimal_app_config("client_reset_tests", schema);
    server_app_config.partition_key = partition_prop;
    TestAppSession test_app_session(create_app(server_app_config));
    auto app = test_app_session.app();

    create_user_and_log_in(app);
    SyncTestFile realm_config(app->current_user(), partition.value, schema);
    realm_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    realm_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        if (err.server_requests_action == sync::ProtocolErrorInfo::Action::Warning ||
            err.server_requests_action == sync::ProtocolErrorInfo::Action::Transient) {
            return;
        }

        FAIL(util::format("got error from server: %1", err.status));
    };

    auto realm = Realm::get_shared_realm(realm_config);
    auto obj_id = ObjectId::gen();
    {
        realm->begin_transaction();
        CppContext c(realm);
        Object::create(
            c, realm, "object",
            std::any(AnyDict{{"_id", obj_id}, {"value", int64_t(5)}, {partition.property_name, partition.value}}));
        realm->commit_transaction();
        wait_for_upload(*realm);
    }
    wait_for_download(*realm, std::chrono::minutes(10));

    reset_utils::trigger_client_reset(test_app_session.app_session(), realm);

    wait_for_download(*realm, std::chrono::minutes(10));

    reset_utils::trigger_client_reset(test_app_session.app_session(), realm);

    wait_for_download(*realm, std::chrono::minutes(10));
}

TEST_CASE("sync: client reset", "[sync][pbs][client reset][baas]") {
    if (!util::EventLoop::has_implementation())
        return;

    const reset_utils::Partition partition{"realm_id", random_string(20)};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
             {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
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
    auto server_app_config = minimal_app_config("client_reset_tests", schema);
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

// TODO: remote-baas: This test fails consistently with Windows remote baas server - to be fixed in RCORE-1674
// This may be due to the realm file at `orig_path` not being deleted on Windows since it is still in use.
#ifndef _WIN32
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
            bool did_reset_files = test_app_session.app()->immediately_run_file_actions(orig_path);
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
            CAPTURE(err.status);
            CAPTURE(local_config.path);
            FAIL("Error handler should not have been called");
        };
        auto post_reset_realm = Realm::get_shared_realm(local_config);
        wait_for_download(*post_reset_realm); // this should now succeed without any sync errors
        REQUIRE(util::File::exists(orig_path));
    }
#endif

    local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError err) {
        CAPTURE(err.status);
        CAPTURE(local_config.path);
        FAIL("Error handler should not have been called");
    };

    local_config.cache = false;
    local_config.automatic_change_notifications = false;
    const std::string fresh_path = realm::_impl::client_reset::get_fresh_path_for(local_config.path);
    size_t before_callback_invocations = 0;
    size_t after_callback_invocations = 0;
    std::mutex mtx;
    local_config.sync_config->notify_before_client_reset = [&](SharedRealm before) {
        std::lock_guard<std::mutex> lock(mtx);
        ++before_callback_invocations;
        REQUIRE(before);
        REQUIRE(before->is_frozen());
        REQUIRE(before->read_group().get_table("class_object"));
        REQUIRE(before->config().path == local_config.path);
        REQUIRE_FALSE(before->schema().empty());
        REQUIRE(before->schema_version() != ObjectStore::NotVersioned);
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

                    CHECK(before_callback_invocations == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(object.get_obj().get<Int>("value") == 4);
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
                    CHECK(before_callback_invocations == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(object.get_obj().get<Int>("value") == 4);
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
                    CHECK(before_callback_invocations == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 2);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                    CHECK(results.get<Obj>(1).get<Int>("value") == new_value);
                    CHECK(object.get_obj().get<Int>("value") == 4);
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
                ->set_development_mode(true)
                ->setup([&](SharedRealm before) {
                    before->update_schema(
                        {
                            {existing_table_name,
                             {
                                 pk_id,
                                 partition_prop,
                             }},
                        },
                        1, nullptr, nullptr, true);
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
                        1, nullptr, nullptr, true);

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
            REQUIRE(before_callback_invocations == 1);
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
                ->set_development_mode(true)
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
                        1, nullptr, nullptr, true);
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
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 0);
        }

        SECTION("add remotely deleted object to list") {
            test_reset
                ->setup([&](SharedRealm realm) {
                    ObjKey k1 =
                        create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 1).get_key();
                    ObjKey k2 =
                        create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 2).get_key();
                    ObjKey k3 =
                        create_object(*realm, "link target", ObjectId::gen(), partition).set("value", 3).get_key();
                    Obj o = create_object(*realm, "link origin", ObjectId::gen(), partition);
                    auto list = o.get_linklist("list");
                    list.add(k1);
                    list.add(k2);
                    list.add(k3);
                    // 1, 2, 3
                })
                ->make_local_changes([&](SharedRealm local) {
                    auto key1 = get_key_for_object_with_value(get_table(*local, "link target"), 1);
                    auto key2 = get_key_for_object_with_value(get_table(*local, "link target"), 2);
                    auto key3 = get_key_for_object_with_value(get_table(*local, "link target"), 3);
                    auto table = get_table(*local, "link origin");
                    auto list = table->begin()->get_linklist("list");
                    REQUIRE(list.size() == 3);
                    list.insert(1, key2);
                    list.add(key2);
                    list.add(key3); // common suffix of key3
                    // 1, 2, 2, 3, 2, 3
                    // this set operation triggers the list copy because the index becomes ambiguous
                    list.set(0, key1);
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
                    REQUIRE(list.size() == 3); // 1, 3, 3
                    REQUIRE(list.get_object(0).get<Int>("value") == 1);
                    REQUIRE(list.get_object(1).get<Int>("value") == 3);
                    REQUIRE(list.get_object(2).get<Int>("value") == 3);
                })
                ->run();
        }

        SECTION("add_int on non-integer field") {
            ObjectId pk = ObjectId::gen();
            test_reset
                ->setup([&](SharedRealm realm) {
                    auto table = get_table(*realm, "object");
                    REQUIRE(table);
                    auto obj = create_object(*realm, "object", {pk}, partition);
                    auto col = obj.get_table()->get_column_key("any_mixed");
                    obj.set_any(col, 42);
                })
                ->make_local_changes([&](SharedRealm local) {
                    auto table = get_table(*local, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 2);
                    ObjKey key = table->get_objkey_from_primary_key(pk);
                    REQUIRE(key);
                    Obj obj = table->get_object(key);
                    obj.add_int("any_mixed", 200);
                })
                ->make_remote_changes([&](SharedRealm remote) {
                    auto table = get_table(*remote, "object");
                    REQUIRE(table);
                    REQUIRE(table->size() == 2);
                    ObjKey key = table->get_objkey_from_primary_key(pk);
                    REQUIRE(key);
                    Obj obj = table->get_object(key);
                    obj.set_any("any_mixed", "value");
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(realm->refresh());
                    auto table = get_table(*realm, "object");
                    REQUIRE(table->size() == 2);
                    ObjKey key = table->get_objkey_from_primary_key(pk);
                    REQUIRE(key);
                    Obj obj = table->get_object(key);
                    REQUIRE(obj);
                    REQUIRE(obj.get_any("any_mixed") == "value");
                })
                ->run();
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

                    CHECK(before_callback_invocations == 1);
                    CHECK(after_callback_invocations == 1);
                    CHECK(results.size() == 1);
                    CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                    CHECK(object.get_obj().get<Int>("value") == 6);
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
                        CHECK(object.get_obj().get<Int>("value") == 6);
                        object_changes = {};
                        results_changes = {};
                    })
                    ->on_post_local_changes([&](SharedRealm) {
                        // advance the object's realm because the one passed here is different
                        REQUIRE_NOTHROW(advance_and_notify(*object.get_realm()));
                        // 6 -> 4
                        CHECK(results.size() == 1);
                        CHECK(results.get<Obj>(0).get<Int>("value") == 4);
                        CHECK(object.get_obj().get<Int>("value") == 4);
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
                        CHECK(before_callback_invocations == 2);
                        CHECK(after_callback_invocations == 2);
                        // 4 -> 6
                        CHECK(results.size() == 1);
                        CHECK(results.get<Obj>(0).get<Int>("value") == 6);
                        CHECK(object.get_obj().get<Int>("value") == 6);
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
            REQUIRE(before_callback_invocations == 0);
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
            REQUIRE(before_callback_invocations == 0);
            REQUIRE(after_callback_invocations == 0);
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return after_callback_invocations > 0;
                },
                std::chrono::seconds(60));
            // this test also relies on the test config above to verify the Realm instances in the callbacks
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("notifiers work if the session instance changes") {
            // run this test with ASAN to check for use after free
            size_t before_callback_invocations_2 = 0;
            size_t after_callback_invocations_2 = 0;
            std::shared_ptr<SyncSession> session;
            std::unique_ptr<SyncConfig> config_copy;
            {
                SyncTestFile temp_config = get_valid_config();
                temp_config.persist();
                temp_config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
                config_copy = std::make_unique<SyncConfig>(*temp_config.sync_config);
                config_copy->notify_before_client_reset = [&](SharedRealm before_realm) {
                    std::lock_guard<std::mutex> lock(mtx);
                    REQUIRE(before_realm);
                    REQUIRE(before_realm->schema_version() != ObjectStore::NotVersioned);
                    ++before_callback_invocations_2;
                };
                config_copy->notify_after_client_reset = [&](SharedRealm, ThreadSafeReference, bool) {
                    std::lock_guard<std::mutex> lock(mtx);
                    ++after_callback_invocations_2;
                };

                temp_config.sync_config->notify_before_client_reset = [&](SharedRealm before_realm) {
                    std::lock_guard<std::mutex> lock(mtx);
                    ++before_callback_invocations;
                    REQUIRE(session);
                    REQUIRE(config_copy);
                    REQUIRE(before_realm);
                    REQUIRE(before_realm->schema_version() != ObjectStore::NotVersioned);
                    session->update_configuration(*config_copy);
                };

                auto realm = Realm::get_shared_realm(temp_config);
                wait_for_upload(*realm);

                session = test_app_session.sync_manager()->get_existing_session(temp_config.path);
                REQUIRE(session);
            }
            sync::SessionErrorInfo synthetic(Status{ErrorCodes::SyncClientResetRequired, "A fake client reset error"},
                                             sync::IsFatal{true});
            synthetic.server_requests_action = sync::ProtocolErrorInfo::Action::ClientReset;
            SyncSession::OnlyForTesting::handle_error(*session, std::move(synthetic));

            session->revive_if_needed();
            timed_sleeping_wait_for(
                [&]() -> bool {
                    std::lock_guard<std::mutex> lock(mtx);
                    return before_callback_invocations > 0;
                },
                std::chrono::seconds(120));
            millisleep(500); // just make some space for the after callback to be attempted
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 0);
            REQUIRE(before_callback_invocations_2 == 0);
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
                REQUIRE(before_callback_invocations == 0);
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
            auto session = test_app_session.sync_manager()->get_existing_session(local_config.path);
            if (session) {
                session->shutdown_and_wait();
            }
            {
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(before_callback_invocations == 1);
                REQUIRE(after_callback_invocations == 1);
            }
        }

        SECTION("an interrupted reset can recover on the next session restart") {
            test_reset->disable_wait_for_reset_completion();
            SharedRealm realm;
            test_reset
                ->on_post_local_changes([&](SharedRealm local) {
                    // retain a reference of the realm.
                    realm = local;
                })
                ->run();

            timed_wait_for([&] {
                return util::File::exists(_impl::client_reset::get_fresh_path_for(local_config.path));
            });

            // Restart the session before the client reset finishes.
            realm->sync_session()->restart_session();

            REQUIRE(!wait_for_upload(*realm));
            REQUIRE(!wait_for_download(*realm));
            realm->refresh();

            auto table = realm->read_group().get_table("class_object");
            REQUIRE(table->size() == 1);
            auto col = table->get_column_key("value");
            int64_t value = table->begin()->get<Int>(col);
            REQUIRE(value == 6);

            {
                std::lock_guard<std::mutex> lock(mtx);
                REQUIRE(before_callback_invocations == 1);
                REQUIRE(after_callback_invocations == 1);
            }
        }

        SECTION("invalid files at the fresh copy path are cleaned up") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            std::string fresh_path = realm::_impl::client_reset::get_fresh_path_for(local_config.path);
            {
                util::File f(fresh_path, util::File::Mode::mode_Write);
                f.write(0, "a non empty file");
            }

            make_reset(local_config, remote_config)->run();
            REQUIRE(!err);
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("failing to download a fresh copy results in an error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            std::string fresh_path = realm::_impl::client_reset::get_fresh_path_for(local_config.path);
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
                    CHECK(object.get_obj().get<Int>("value") == new_value);
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
                    CHECK(object.get_obj().get<Int>("value") == 6);
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
                ->set_development_mode(true)
                ->make_local_changes([&](SharedRealm local) {
                    local->update_schema(
                        {
                            {"object2",
                             {
                                 {"_id", PropertyType::ObjectId | PropertyType::Nullable, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        1, nullptr, nullptr, true);
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
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 0);
        }

        SECTION("extra local column creates a client reset error") {
            ThreadSafeSyncError err;
            local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                err = error;
            };
            make_reset(local_config, remote_config)
                ->set_development_mode(true)
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
                        1, nullptr, nullptr, true);
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
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 0);
        }

        SECTION("compatible schema changes in both remote and local transactions") {
            test_reset->set_development_mode(true)
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
                        1, nullptr, nullptr, true);
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
                ->set_development_mode(true)
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
                        1, nullptr, nullptr, true);
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
                ->set_development_mode(true)
                ->make_local_changes([](SharedRealm local) {
                    local->update_schema(
                        {
                            {"new table",
                             {
                                 {"_id", PropertyType::Int, Property::IsPrimary{true}},
                                 {"realm_id", PropertyType::String | PropertyType::Nullable},
                             }},
                        },
                        1, nullptr, nullptr, true);
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
        auto has_reset_cycle_flag = [](SharedRealm realm) -> util::Optional<sync::PendingReset> {
            auto db = TestHelper::get_db(realm);
            auto rd_tr = db->start_frozen();
            return sync::PendingResetStore::has_pending_reset(rd_tr);
        };
        auto logger = util::Logger::get_default_logger();
        ThreadSafeSyncError err;
        local_config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
            logger->error("Detected cycle detection error: %1", error.status);
            err = error;
        };
        auto make_fake_previous_reset = [&local_config](ClientResyncMode mode,
                                                        sync::ProtocolErrorInfo::Action action =
                                                            sync::ProtocolErrorInfo::Action::ClientReset) {
            local_config.sync_config->notify_before_client_reset = [mode, action](SharedRealm realm) {
                auto db = TestHelper::get_db(realm);
                auto wr_tr = db->start_write();
                sync::PendingResetStore::track_reset(
                    wr_tr, mode, action, {{ErrorCodes::SyncClientResetRequired, "Bad client file ident"}});
                wr_tr->commit();
            };
        };
        SECTION("a normal reset adds and removes a cycle detection flag") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            local_config.sync_config->notify_before_client_reset = [&](SharedRealm realm) {
                REQUIRE_FALSE(has_reset_cycle_flag(realm));
                std::lock_guard lock(mtx);
                ++before_callback_invocations;
            };
            local_config.sync_config->notify_after_client_reset = [&](SharedRealm, ThreadSafeReference realm_ref,
                                                                      bool did_recover) {
                SharedRealm realm = Realm::get_shared_realm(std::move(realm_ref), util::Scheduler::make_default());
                auto flag = has_reset_cycle_flag(realm);
                REQUIRE(bool(flag));
                REQUIRE(flag->mode == ClientResyncMode::Recover);
                REQUIRE(did_recover);
                std::lock_guard lock(mtx);
                ++after_callback_invocations;
            };
            make_reset(local_config, remote_config)
                ->on_post_local_changes([&](SharedRealm realm) {
                    REQUIRE_FALSE(has_reset_cycle_flag(realm));
                })
                ->run();
            REQUIRE(!err);
            REQUIRE(before_callback_invocations == 1);
            REQUIRE(after_callback_invocations == 1);
        }

        SECTION("a failed reset leaves a cycle detection flag") {
            local_config.sync_config->client_resync_mode = ClientResyncMode::Recover;
            make_reset(local_config, remote_config)
                ->make_local_changes([](SharedRealm realm) {
                    auto table = realm->read_group().get_table("class_object");
                    table->remove_column(table->add_column(type_Int, "new col"));
                })
                ->run();
            local_config.sync_config.reset();
            local_config.force_sync_history = true;
            auto realm = Realm::get_shared_realm(local_config);
            auto flag = has_reset_cycle_flag(realm);
            REQUIRE(flag);
            CHECK(flag->mode == ClientResyncMode::Recover);
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
                    CHECK(object.get_obj().get<Int>("value") == 6);
                })
                ->run();
        }
    } // end: The server can prohibit recovery
}

TEST_CASE("sync: Client reset during async open", "[sync][pbs][client reset][baas]") {
    const reset_utils::Partition partition{"realm_id", random_string(20)};
    Property partition_prop = {partition.property_name, PropertyType::String | PropertyType::Nullable};
    Schema schema{
        {"object",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"value", PropertyType::String},
             partition_prop,
         }},
    };

    auto server_app_config = minimal_app_config("client_reset_tests", schema);
    server_app_config.partition_key = partition_prop;
    TestAppSession test_app_session(create_app(server_app_config));
    auto app = test_app_session.app();

    create_user_and_log_in(app);
    SyncTestFile realm_config(app->current_user(), partition.value, std::nullopt,
                              [](std::shared_ptr<SyncSession>, SyncError) { /*noop*/ });
    realm_config.sync_config->client_resync_mode = ClientResyncMode::Recover;

    bool client_reset_triggered = false;
    realm_config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession> weak_sess,
                                                              const SyncClientHookData& event_data) mutable {
        auto sess = weak_sess.lock();
        if (!sess) {
            return SyncClientHookAction::NoAction;
        }
        if (sess->path() != realm_config.path) {
            return SyncClientHookAction::NoAction;
        }

        if (event_data.event != SyncClientHookEvent::DownloadMessageReceived) {
            return SyncClientHookAction::NoAction;
        }

        if (client_reset_triggered) {
            return SyncClientHookAction::NoAction;
        }
        client_reset_triggered = true;
        reset_utils::trigger_client_reset(test_app_session.app_session(), *sess);
        return SyncClientHookAction::SuspendWithRetryableError;
    };

    // Expected behaviour is that the frozen realm passed in the callback should have no
    // schema initialized if a client reset happens during an async open and the realm has never been opened before.
    // SDK's should handle any edge cases which require the use of a schema i.e
    // calling set_schema_subset(...)
    auto before_callback_called = util::make_promise_future<void>();
    realm_config.sync_config->notify_before_client_reset = [&](std::shared_ptr<Realm> realm) {
        CHECK(realm->schema_version() == ObjectStore::NotVersioned);
        before_callback_called.promise.emplace_value();
    };

    auto after_callback_called = util::make_promise_future<void>();
    realm_config.sync_config->notify_after_client_reset = [&](std::shared_ptr<Realm> realm, ThreadSafeReference,
                                                              bool) {
        CHECK(realm->schema_version() == ObjectStore::NotVersioned);
        after_callback_called.promise.emplace_value();
    };

    auto realm_task = Realm::get_synchronized_realm(realm_config);
    auto realm_pf = util::make_promise_future<SharedRealm>();
    realm_task->start([&](ThreadSafeReference ref, std::exception_ptr ex) {
        try {
            if (ex) {
                std::rethrow_exception(ex);
            }
            auto realm = Realm::get_shared_realm(std::move(ref));
            realm_pf.promise.emplace_value(std::move(realm));
        }
        catch (...) {
            realm_pf.promise.set_error(exception_to_status());
        }
    });
    auto realm = realm_pf.future.get();
    before_callback_called.future.get();
    after_callback_called.future.get();
}

#endif // REALM_ENABLE_AUTH_TESTS

namespace cf = realm::collection_fixtures;
TEMPLATE_TEST_CASE("client reset types", "[sync][pbs][client reset]", cf::MixedVal, cf::Int, cf::Bool, cf::Float,
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

    OfflineAppSession oas;
    SyncTestFile config(oas, "default");
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
          {"value", TestType::property_type},
          {"list", PropertyType::Array | TestType::property_type},
          {"dictionary", PropertyType::Dictionary | TestType::property_type},
          {"set", PropertyType::Set | TestType::property_type}}},
    };

    SyncTestFile config2(oas.app()->current_user(), "default");
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
                    check_value(object.get_obj(), local_state);
                })
                ->on_post_reset([&](SharedRealm realm) {
                    REQUIRE_NOTHROW(advance_and_notify(*realm));

                    CHECK(results.size() == 1);
                    CHECK(object.is_valid());
                    T expected_state = (test_mode == ClientResyncMode::DiscardLocal) ? remote_state : local_state;
                    check_value(results.get<Obj>(0), expected_state);
                    check_value(object.get_obj(), expected_state);
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
                    check_list(object.get_obj(), local_state);
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
                    check_list(object.get_obj(), expected_state);
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
                    check_dictionary(object.get_obj(), local_state);
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
                            expected_state.erase(dict_key); // explict erasure of initial state occurred
                        }
                    }
                    check_dictionary(results.get<Obj>(0), expected_state);
                    check_dictionary(object.get_obj(), expected_state);
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
                    check_set(object.get_obj(), local_state);
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
                            expected.erase(Mixed{values[0]}); // explicit erase of initial element occurred
                        }
                    }
                    check_set(results.get<Obj>(0), expected);
                    check_set(object.get_obj(), expected);
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

struct Clear {};

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

struct Insert {
    Insert(size_t index, util::Optional<int64_t> key)
        : ndx(index)
        , pk(key)
    {
    }
    size_t ndx;
    util::Optional<int64_t> pk;
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
    CollectionOperation(Insert op)
        : m_op(op)
    {
    }
    void apply(collection_fixtures::LinkedCollectionBase* collection, Obj src_obj, TableRef dst_table)
    {
        auto get_table = [&](std::string_view name) -> TableRef {
            Group* group = dst_table->get_parent_group();
            Group::TableNameBuffer buffer;
            TableRef table = group->get_table(Group::class_name_to_table_name(name, buffer));
            REALM_ASSERT(table);
            return table;
        };
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
                    TableRef table = get_table(remove_object.class_name);
                    ObjKey dst_key = table->find_primary_key(Mixed{remove_object.pk});
                    REALM_ASSERT(dst_key);
                    table->remove_object(dst_key);
                },
                [&](CreateObject create_object) {
                    TableRef table = get_table(create_object.class_name);
                    table->create_object_with_primary_key(Mixed{create_object.pk});
                },
                [&](Clear) {
                    collection->clear_collection(src_obj);
                },
                [&](Insert insert) {
                    Mixed pk_to_add = insert.pk ? Mixed{insert.pk} : Mixed{};
                    ObjKey dst_key = dst_table->find_primary_key(pk_to_add);
                    REALM_ASSERT(dst_key);
                    collection->insert(src_obj, insert.ndx, ObjLink{dst_table->get_key(), dst_key});
                },
                [&](Move move) {
                    collection->move(src_obj, move.from, move.to);
                }},
            m_op);
    }

private:
    mpark::variant<Add, Remove, Clear, RemoveObject, CreateObject, Move, Insert> m_op;
};

} // namespace test_instructions

TEMPLATE_TEST_CASE("client reset collections of links", "[sync][pbs][client reset][links][collections]",
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
    constexpr bool test_type_is_set = realm::is_any_v<TestType, cf::SetOfObjects, cf::SetOfMixedLinks>;
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

    OfflineAppSession oas;
    SyncTestFile config(oas, "default");
    config.automatic_change_notifications = false;
    config.schema = schema;
    ClientResyncMode test_mode = GENERATE(ClientResyncMode::DiscardLocal, ClientResyncMode::Recover);
    CAPTURE(test_mode);
    config.sync_config->client_resync_mode = test_mode;

    SyncTestFile config2(oas.app()->current_user(), "default");
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
            test_type.add_link(object.get_obj(), link);
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
        return ObjLink{obj.get_obj().get_table()->get_key(), obj.get_obj().get_key()};
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
                    Obj origin = results.get(0);
                    auto linked_objects = test_type.get_links(origin);
                    std::vector<util::Optional<int64_t>>& expected_links = remote_pks;
                    const size_t actual_size = test_type.size_of_collection(origin);
                    if (test_mode == ClientResyncMode::Recover) {
                        expected_links = expected_recovered_state;
                        size_t expected_size = expected_links.size();
                        if (!test_type.will_erase_removed_object_links()) {
                            // dictionary size will remain the same because the key is preserved with a null value
                            expected_size += num_expected_nulls;
                        }
                        CHECK(actual_size == expected_size);
                        if (actual_size != expected_size) {
                            std::vector<Obj> links = test_type.get_links(origin);
                            std::cout << "actual {";
                            for (auto link : links) {
                                std::cout << link.get_primary_key() << ", ";
                            }
                            std::cout << "}\n";
                        }
                    }
                    if (!test_type_is_array) {
                        // order should not matter except for lists
                        std::sort(local_pks.begin(), local_pks.end());
                        std::sort(expected_links.begin(), expected_links.end());
                    }
                    require_links_to_match_ids(linked_objects, expected_links, !test_type_is_array);
                    if (local_pks != expected_links) {
                        REQUIRE_INDICES(results_changes.modifications, 0);
                        REQUIRE_INDICES(object_changes.modifications, 0);
                    }
                    else {
                        REQUIRE_INDICES(results_changes.modifications);
                        REQUIRE_INDICES(object_changes.modifications);
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
    auto populate_initial_state = [&](SharedRealm realm) {
        test_type.reset_test_state();
        // add a container collection with three valid links
        ObjLink dest1 = create_one_dest_object(realm, dest_pk_1);
        ObjLink dest2 = create_one_dest_object(realm, dest_pk_2);
        ObjLink dest3 = create_one_dest_object(realm, dest_pk_3);
        create_one_dest_object(realm, dest_pk_4);
        create_one_dest_object(realm, dest_pk_5);
        create_one_source_object(realm, source_pk, {dest1, dest2, dest3});
    };

    test_reset->setup([&](SharedRealm realm) {
        populate_initial_state(realm);
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
    SECTION("local adds two links to objects which are both removed by the remote") {
        reset_collection({Add{dest_pk_4}, Add{dest_pk_5}, CreateObject("dest", 6), Add{6}},
                         {RemoveObject("dest", dest_pk_4), RemoveObject("dest", dest_pk_5)},
                         {dest_pk_1, dest_pk_2, dest_pk_3, 6}, 2);
    }
    SECTION("local removes two objects which were linked to by remote") {
        reset_collection(
            {RemoveObject("dest", dest_pk_1), RemoveObject("dest", dest_pk_2), CreateObject("dest", 6), Add{6}}, {},
            {dest_pk_3, 6}, 2);
    }
    SECTION("local has unresolved links") {
        test_reset->setup([&](SharedRealm realm) {
            populate_initial_state(realm);

            auto invalidate_object = [&](SharedRealm realm, std::string_view table_name, Mixed pk) {
                TableRef table = get_table(*realm, table_name);
                Obj obj = table->get_object_with_primary_key(pk);
                REALM_ASSERT(obj.is_valid());
                if (realm->config().path == config.path) {
                    // the local realm does an invalidation
                    table->invalidate_object(obj.get_key());
                }
                else {
                    // the remote realm has deleted it
                    table->remove_object(obj.get_key());
                }
            };

            invalidate_object(realm, "dest", dest_pk_1);
        });

        SECTION("remote adds a link") {
            reset_collection({}, {Add{dest_pk_4}}, {dest_pk_2, dest_pk_3, dest_pk_4}, 1);
        }
        SECTION("remote removes a link") {
            reset_collection({}, {Remove{dest_pk_2}}, {dest_pk_3}, 1);
        }
        SECTION("remote deletes a dest object that local links to") {
            reset_collection({Add{dest_pk_4}}, {RemoveObject{"dest", dest_pk_4}}, {dest_pk_2, dest_pk_3}, 2);
        }
        SECTION("remote deletes a different dest object") {
            reset_collection({Add{dest_pk_4}}, {RemoveObject{"dest", dest_pk_2}}, {dest_pk_3, dest_pk_4}, 2);
        }
        SECTION("local adds two new links and remote deletes a different dest object") {
            reset_collection({Add{dest_pk_4}, Add{dest_pk_5}}, {RemoveObject{"dest", dest_pk_2}},
                             {dest_pk_3, dest_pk_4, dest_pk_5}, 2);
        }
        SECTION("remote deletes an object, then removes and adds to the list") {
            reset_collection({}, {RemoveObject{"dest", dest_pk_2}, Remove{dest_pk_3}, Add{dest_pk_4}}, {dest_pk_4},
                             2);
        }
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
        SECTION("local moves on non-added elements with server dest obj removal") {
            reset_collection(
                {Move{0, 1}, Add{dest_pk_5}}, {Add{dest_pk_4}, RemoveObject("dest", dest_pk_1)},
                {dest_pk_2, dest_pk_3,
                 dest_pk_5}); // copy over local list, but without the dest_pk_1 link because that object was deleted
        }
        SECTION("local moves on non-added elements with all server dest objs removed") {
            reset_collection({Move{0, 1}, Add{dest_pk_5}},
                             {Add{dest_pk_4}, RemoveObject("dest", dest_pk_1), RemoveObject("dest", dest_pk_2),
                              RemoveObject("dest", dest_pk_3), RemoveObject("dest", dest_pk_5)},
                             {}); // copy over local list, but all links have been removed
        }
        SECTION("local moves on non-added elements when server creates a new object and adds it to the list") {
            reset_collection({Move{0, 1}, Add{dest_pk_5}}, {CreateObject("dest", 6), Add{6}},
                             {dest_pk_2, dest_pk_1, dest_pk_3, dest_pk_5});
        }
        SECTION("local moves on locally-added elements when server removes the object that the new links point to") {
            reset_collection({Add{dest_pk_5}, Add{dest_pk_5}, Move{4, 3}},
                             {Add{dest_pk_4}, RemoveObject("dest", dest_pk_5)},
                             {dest_pk_1, dest_pk_2, dest_pk_3}); // local overwrite, but without pk_5
        }
        SECTION("local insert and delete can be recovered even if a local link was deleted by remote") {
            // start  : 1, 2, 3
            // local  : 1, 2, 3, 5, 6, 1
            // remote : 4, 1, 2, 3 {remove obj 5}
            // result : 1, 2, 3, 6, 1
            reset_collection({CreateObject("dest", 6), Add{dest_pk_5}, Add{6}, Insert{4, dest_pk_4},
                              Remove{dest_pk_4}, Add{dest_pk_1}},
                             {Insert{0, dest_pk_4}, RemoveObject("dest", dest_pk_5)},
                             {dest_pk_4, dest_pk_1, dest_pk_2, dest_pk_3, 6, dest_pk_1});
        }
        SECTION("both add link to object which has been deleted by other side") {
            // start  : 1, 2, 3
            // local  : 1, 1, 2, 3, 5, {remove object 4}
            // remote : 1, 2, 3, 3, 4, {remove obj 5}
            // result : 1, 1, 2, 3, 3
            reset_collection({Add{dest_pk_5}, Insert{0, dest_pk_1}, RemoveObject("dest", dest_pk_4)},
                             {Add{dest_pk_4}, Insert{3, dest_pk_3}, RemoveObject("dest", dest_pk_5)},
                             {dest_pk_1, dest_pk_1, dest_pk_2, dest_pk_3, dest_pk_3});
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
    if constexpr (test_type_is_set) {
        SECTION("remote adds two links to objects which are both removed by local") {
            reset_collection({RemoveObject("dest", dest_pk_4), RemoveObject("dest", dest_pk_5),
                              CreateObject("dest", 6), Add{6}, Remove{dest_pk_1}},
                             {Remove{dest_pk_2}, Add{dest_pk_4}, Add{dest_pk_5}, CreateObject("dest", 6), Add{6},
                              CreateObject("dest", 7), Add{7}, RemoveObject("dest", dest_pk_5)},
                             {dest_pk_3, 6, 7});
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

TEST_CASE("client reset with embedded object", "[sync][pbs][client reset][embedded objects]") {
    if (!util::EventLoop::has_implementation())
        return;

    OfflineAppSession oas;
    SyncTestFile config(oas, "default");
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
             {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
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

    SyncTestFile config2(oas.app()->current_user(), "default");
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
        SECTION(
            "local ArrayUpdate to an embedded object through a deep link->linklist element which is removed by the "
            "remote triggers a list copy") {
            local.link_value->array_vals[0] = 12345;
            remote.link_value->array_vals.erase(remote.link_value->array_vals.begin());
            TopLevelContent expected_recovered = local;
            reset_embedded_object({local}, {remote}, expected_recovered);
        }
        SECTION("local ArrayErase to an embedded object through a deep link->linklist element which is removed by "
                "the remote triggers a list copy") {
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
        SECTION("both add the same dictionary key") {
            auto key = GENERATE("new key", "", "\0");
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
        SECTION("deep modifications to inserted and swapped list items are recovered") {
            EmbeddedContent local_added_at_begin, local_added_at_end, local_added_before_end, remote_added;
            size_t list_end = initial.array_values.size();
            test_reset
                ->make_local_changes([&](SharedRealm local) {
                    Obj obj = get_top_object(local);
                    auto list = obj.get_linklist("array_of_objs");
                    auto embedded = list.create_and_insert_linked_object(0);
                    local_added_at_begin.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(list_end - 1);
                    // this item is needed here so that move does not trigger a copy of the list
                    local_added_before_end.assign_to(embedded);
                    embedded = list.create_and_insert_linked_object(list_end);
                    local_added_at_end.assign_to(embedded);
                    local->commit_transaction();
                    local->begin_transaction();
                    // generates two move instructions, move(0, list_end), move(list_end - 1, 0)
                    list.swap(0, list_end);
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
                REQUIRE(embedded);
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
        SyncTestFile config2(oas.app()->current_user(), "default");
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
        SyncTestFile config2(oas.app()->current_user(), "default");
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

TEST_CASE("client reset with nested collection", "[client reset][local][nested collection]") {

    if (!util::EventLoop::has_implementation())
        return;

    OfflineAppSession oas;
    SyncTestFile config(oas, "default");
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

    config.schema =
        Schema{shared_class,
               {"TopLevel",
                {
                    {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                    {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
                    {"list_mixed", PropertyType::Array | PropertyType::Mixed | PropertyType::Nullable},
                    {"dictionary_mixed", PropertyType::Dictionary | PropertyType::Mixed | PropertyType::Nullable},
                }}};

    SECTION("add nested collection locally") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = Schema{shared_class};

        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset->make_local_changes([&](SharedRealm local) {
            advance_and_notify(*local);
            TableRef table = get_table(*local, "TopLevel");
            auto obj = table->create_object_with_primary_key(pk_val);
            auto col = table->get_column_key("any_mixed");
            obj.set_collection(col, CollectionType::List);
            List list{local, obj, col};
            list.insert_collection(0, CollectionType::List);
            auto nlist = list.get_list(0);
            nlist.add(Mixed{10});
            nlist.add(Mixed{"Test"});
            REQUIRE(table->size() == 1);
        });
        if (test_mode == ClientResyncMode::DiscardLocal) {
            REQUIRE_THROWS_WITH(test_reset->run(), "Client reset cannot recover when classes have been removed: "
                                                   "{TopLevel}");
        }
        else {
            test_reset
                ->on_post_reset([&](SharedRealm local) {
                    advance_and_notify(*local);
                    TableRef table = get_table(*local, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 10);
                    REQUIRE(nlist.get_any(1).get_string() == "Test");
                })
                ->run();
        }
    }
    SECTION("server adds nested collection. List of nested collections") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        config.schema = Schema{shared_class};
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);

        test_reset
            ->make_remote_changes([&](SharedRealm remote) {
                advance_and_notify(*remote);
                TableRef table = get_table(*remote, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // List
                obj.set_collection(col, CollectionType::List);
                List list{remote, obj, col};
                // primitive type
                list.add(Mixed{42});
                // List<List<Mixed>>
                list.insert_collection(1, CollectionType::List);
                auto nlist = list.get_list(1);
                nlist.add(Mixed{10});
                nlist.add(Mixed{"Test"});
                // List<Dictionary>
                list.insert_collection(2, CollectionType::Dictionary);
                auto n_dict = list.get_dictionary(2);
                n_dict.insert("Test", Mixed{"10"});
                n_dict.insert("Test1", Mixed{10});
                REQUIRE(list.size() == 3);
                REQUIRE(table->size() == 1);
            })
            ->on_post_reset([&](SharedRealm local) {
                advance_and_notify(*local);
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local, obj, col};
                REQUIRE(list.size() == 3);
                auto mixed = list.get_any(0);
                REQUIRE(mixed.get_int() == 42);
                auto nlist = list.get_list(1);
                REQUIRE(nlist.size() == 2);
                REQUIRE(nlist.get_any(0).get_int() == 10);
                REQUIRE(nlist.get_any(1).get_string() == "Test");
                auto n_dict = list.get_dictionary(2);
                REQUIRE(n_dict.size() == 2);
                REQUIRE(n_dict.get<Mixed>("Test").get_string() == "10");
                REQUIRE(n_dict.get<Mixed>("Test1").get_int() == 10);
            })
            ->run();
    }
    SECTION("server adds nested collection. Dictionary of nested collections") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        config.schema = Schema{shared_class};
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_remote_changes([&](SharedRealm remote) {
                advance_and_notify(*remote);
                TableRef table = get_table(*remote, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // List
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{remote, obj, col};
                // primitive type
                dict.insert("Scalar", Mixed{42});
                // Dictionary<List<Mixed>>
                dict.insert_collection("List", CollectionType::List);
                auto nlist = dict.get_list("List");
                nlist.add(Mixed{10});
                nlist.add(Mixed{"Test"});
                // Dictionary<Dictionary>
                dict.insert_collection("Dict", CollectionType::Dictionary);
                auto n_dict = dict.get_dictionary("Dict");
                n_dict.insert("Test", Mixed{"10"});
                n_dict.insert("Test1", Mixed{10});
                REQUIRE(dict.size() == 3);
                REQUIRE(table->size() == 1);
            })
            ->on_post_reset([&](SharedRealm local) {
                advance_and_notify(*local);
                TableRef table = get_table(*local, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local, obj, col};
                REQUIRE(dict.size() == 3);
                auto mixed = dict.get_any("Scalar");
                REQUIRE(mixed.get_int() == 42);
                auto nlist = dict.get_list("List");
                REQUIRE(nlist.size() == 2);
                REQUIRE(nlist.get_any(0).get_int() == 10);
                REQUIRE(nlist.get_any(1).get_string() == "Test");
                auto n_dict = dict.get_dictionary("Dict");
                REQUIRE(n_dict.size() == 2);
                REQUIRE(n_dict.get<Mixed>("Test").get_string() == "10");
                REQUIRE(n_dict.get<Mixed>("Test1").get_int() == 10);
            })
            ->run();
    }
    SECTION("add nested collection both locally and remotely List vs Dictionary") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                advance_and_notify(*local);
                auto table = get_table(*local, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{local, obj, col};
                list.insert(0, Mixed{30});
                REQUIRE(list.size() == 1);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{remote_realm, obj, col};
                dict.insert("Test", Mixed{40});
                REQUIRE(dict.size() == 1);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 1);
                    REQUIRE(dictionary.get_any("Test").get_int() == 40);
                }
                else {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    REQUIRE(list.get_any(0) == 30);
                }
            })
            ->run();
    }
    SECTION("add nested collection both locally and remotely. Nesting levels mismatch List vs Dictionary") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                advance_and_notify(*local);
                auto table = get_table(*local, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{local, obj, col};
                list.insert_collection(0, CollectionType::Dictionary);
                auto dict = list.get_dictionary(0);
                dict.insert("Test", Mixed{30});
                REQUIRE(list.size() == 1);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{remote_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto nlist = list.get_list(0);
                nlist.insert(0, Mixed{30});
                REQUIRE(nlist.size() == 1);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get<Mixed>(0).get_int() == 30);
                }
                else {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto n_dict = list.get_dictionary(0);
                    REQUIRE(n_dict.size() == 1);
                    REQUIRE(n_dict.get<Mixed>("Test").get_int() == 30);
                    auto n_list = list.get_list(1);
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list.get_any(0) == 30);
                }
            })
            ->run();
    }
    SECTION("add nested collection both locally and remotely. Collections matched. Merge collections if not discard "
            "local") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                advance_and_notify(*local);
                auto table = get_table(*local, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{local, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
                list.insert_collection(1, CollectionType::Dictionary);
                auto dict = list.get_dictionary(1);
                dict.insert("Test", Mixed{10});
                REQUIRE(list.size() == 2);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{remote_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{40});
                list.insert_collection(1, CollectionType::Dictionary);
                auto dict = list.get_dictionary(1);
                dict.insert("Test1", Mixed{11});
                REQUIRE(list.size() == 2);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    REQUIRE(list.size() == 2);
                    auto n_list = list.get_list(0);
                    REQUIRE(n_list.get_any(0).get_int() == 40);
                    auto n_dict = list.get_dictionary(1);
                    REQUIRE(n_dict.size() == 1);
                    REQUIRE(n_dict.get<Mixed>("Test1").get_int() == 11);
                }
                else {
                    REQUIRE(list.size() == 4);
                    auto n_list = list.get_list(0);
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list.get_any(0).get_int() == 30);
                    auto n_dict = list.get_dictionary(1);
                    REQUIRE(n_dict.size() == 1);
                    REQUIRE(n_dict.get<Mixed>("Test").get_int() == 10);
                    auto n_list1 = list.get_list(2);
                    REQUIRE(n_list1.size() == 1);
                    REQUIRE(n_list1.get_any(0).get_int() == 40);
                    auto n_dict1 = list.get_dictionary(3);
                    REQUIRE(n_dict1.size() == 1);
                    REQUIRE(n_dict1.get<Mixed>("Test1").get_int() == 11);
                }
            })
            ->run();
    }
    SECTION("add nested collection both locally and remotely. Collections matched. Mix collections with values") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                advance_and_notify(*local);
                auto table = get_table(*local, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{local, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
                list.insert_collection(1, CollectionType::Dictionary);
                auto dict = list.get_dictionary(1);
                dict.insert("Test", Mixed{10});
                list.insert(0, Mixed{2}); // this shifts all the other collections by 1
                REQUIRE(list.size() == 3);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{remote_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{40});
                list.insert_collection(1, CollectionType::Dictionary);
                auto dict = list.get_dictionary(1);
                dict.insert("Test1", Mixed{11});
                list.insert(0, Mixed{30}); // this shifts all the other collections by 1
                REQUIRE(list.size() == 3);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    REQUIRE(list.size() == 3);
                    REQUIRE(list.get_any(0).get_int() == 30);
                    auto n_list = list.get_list(1);
                    REQUIRE(n_list.get_any(0).get_int() == 40);
                    auto n_dict = list.get_dictionary(2);
                    REQUIRE(n_dict.size() == 1);
                    REQUIRE(n_dict.get<Mixed>("Test1").get_int() == 11);
                }
                else {
                    // local
                    REQUIRE(list.size() == 6);
                    REQUIRE(list.get_any(0).get_int() == 2);
                    auto n_list = list.get_list(1);
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list.get_any(0).get_int() == 30);
                    auto n_dict = list.get_dictionary(2);
                    REQUIRE(n_dict.size() == 1);
                    REQUIRE(n_dict.get<Mixed>("Test").get_int() == 10);
                    // remote
                    REQUIRE(list.get_any(3).get_int() == 30);
                    auto n_list1 = list.get_list(4);
                    REQUIRE(n_list1.size() == 1);
                    REQUIRE(n_list1.get_any(0).get_int() == 40);
                    auto n_dict1 = list.get_dictionary(5);
                    REQUIRE(n_dict1.size() == 1);
                    REQUIRE(n_dict1.get<Mixed>("Test1").get_int() == 11);
                }
            })
            ->run();
    }
    SECTION("add nested collection both locally and remotely. Collections do not match") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->make_local_changes([&](SharedRealm local) {
                advance_and_notify(*local);
                auto table = get_table(*local, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{local, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{remote_realm, obj, col};
                dict.insert_collection("List", CollectionType::List);
                auto n_list = dict.get_list("List");
                n_list.insert(0, Mixed{30});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    object_store::Dictionary dict{local_realm, obj, col};
                    REQUIRE(dict.size() == 1);
                    auto n_list = dict.get_list("List");
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list.get_any(0).get_int() == 30);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto n_list = list.get_list(0);
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list.get_any(0).get_int() == 30);
                }
            })
            ->run();
    }
    SECTION("delete collection remotely and add locally. Collections do not match") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                list.insert_collection(1, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
                n_list = list.get_list(1);
                n_list.insert(0, Mixed{31});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // The changes are recovered (instead of copying the entire list) because
                // the first index in the path is known (it is just inserted)
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{50});
                REQUIRE(list.size() == 3);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.remove(0); // remove list with 30 in it.
                REQUIRE(list.size() == 1);
                auto n_list = list.get_list(0);
                REQUIRE(n_list.get_any(0).get_int() == 31); // new position 0 is the list with entry set to 31
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto n_list = list.get_list(0);
                    REQUIRE(n_list.get_any(0).get_int() == 31);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto n_list1 = list.get_list(0);
                    auto n_list2 = list.get_list(1);
                    REQUIRE(n_list1.size() == 1);
                    REQUIRE(n_list2.size() == 1);
                    REQUIRE(n_list1.get_any(0).get_int() == 50);
                    REQUIRE(n_list2.get_any(0).get_int() == 31);
                }
            })
            ->run();
    }
    SECTION("delete collection remotely and add locally same index.") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{50});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 1);
                list.remove(0);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 0);
                }
                else {
                    TableRef table = get_table(*local_realm, "TopLevel");
                    REQUIRE(table->size() == 1);
                    auto obj = table->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 50);
                }
            })
            ->run();
    }
    SECTION("shift collection remotely and locally") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                // this insert operation triggers the list copy because the index becomes ambiguous
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{50});
                list.insert_collection(0, CollectionType::List); // shift
                auto n_list1 = list.get_list(0);
                n_list1.insert(0, Mixed{150});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                auto n_list = list.get_list(0);
                n_list.insert(1, Mixed{100});
                list.insert_collection(0, CollectionType::List); // shift
                auto n_list1 = list.get_list(0);
                n_list1.insert(0, Mixed{42});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto n_list = list.get_list(0);
                    auto n_list1 = list.get_list(1);
                    REQUIRE(n_list.size() == 1);
                    REQUIRE(n_list1.size() == 2);
                    REQUIRE(n_list1.get_any(0).get_int() == 30);
                    REQUIRE(n_list1.get_any(1).get_int() == 100);
                    REQUIRE(n_list.get_any(0).get_int() == 42);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto n_list1 = list.get_list(0);
                    auto n_list2 = list.get_list(1);
                    REQUIRE(n_list1.size() == 1);
                    REQUIRE(n_list2.size() == 2);
                    REQUIRE(n_list1.get_any(0).get_int() == 150);
                    REQUIRE(n_list2.get_any(0).get_int() == 50);
                    REQUIRE(n_list2.get_any(1).get_int() == 30);
                }
            })
            ->run();
    }
    SECTION("delete collection locally (list). Local should win") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                list.remove(0);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                list.add(Mixed{10});
                REQUIRE(list.size() == 2);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto n_list1 = list.get_list(0);
                    auto mixed = list.get_any(1);
                    REQUIRE(n_list1.size() == 1);
                    REQUIRE(mixed.get_int() == 10);
                    REQUIRE(n_list1.get_any(0).get_int() == 30);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 0);
                }
            })
            ->run();
    }
    SECTION("move collection locally (list). Local should win") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto n_list = list.get_list(0);
                n_list.insert(0, Mixed{30});
                n_list.insert(1, Mixed{10});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                auto nlist = list.get_list(0);
                nlist.move(0, 1); // move value 30 in pos 1.
                REQUIRE(nlist.size() == 2);
                REQUIRE(nlist.get_any(0).get_int() == 10);
                REQUIRE(nlist.get_any(1).get_int() == 30);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 1);
                auto nlist = list.get_list(0);
                REQUIRE(nlist.size() == 2);
                nlist.add(Mixed{2});
                REQUIRE(nlist.size() == 3);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // local state is preserved
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 3);
                    REQUIRE(nlist.get_any(0).get_int() == 30);
                    REQUIRE(nlist.get_any(1).get_int() == 10);
                    REQUIRE(nlist.get_any(2).get_int() == 2);
                }
                else {
                    // local change wins
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 10);
                    REQUIRE(nlist.get_any(1).get_int() == 30);
                }
            })
            ->run();
    }
    SECTION("delete collection locally (dictionary). Local should win") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("Test", CollectionType::Dictionary);
                auto n_dictionary = dictionary.get_dictionary("Test");
                n_dictionary.insert("Val", 30);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                REQUIRE(dictionary.size() == 1);
                dictionary.erase("Test");
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                REQUIRE(dictionary.size() == 1);
                auto n_dictionary = dictionary.get_dictionary("Test");
                n_dictionary.insert("Val1", 31);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 1);
                    auto n_dictionary = dictionary.get_dictionary("Test");
                    REQUIRE(n_dictionary.get_any("Val").get_int() == 30);
                    REQUIRE(n_dictionary.get_any("Val1").get_int() == 31);
                }
                else {
                    // local change wins
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 0);
                }
            })
            ->run();
    }
    // testing copying logic for nested collections
    SECTION("Verify copy logic for collections in mixed.") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                list.insert_collection(1, CollectionType::Dictionary);
                auto nlist = list.get_list(0);
                auto ndict = list.get_dictionary(1);
                nlist.add(Mixed{1});
                nlist.add(Mixed{"Test"});
                ndict.insert("Int", Mixed(3));
                ndict.insert("String", Mixed("Test"));
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);
                auto nlist = list.get_list(0);
                nlist.insert_collection(0, CollectionType::List);
                nlist = nlist.get_list(0);
                nlist.add(Mixed{4});
                auto ndict = list.get_dictionary(1);
                ndict.insert_collection("key", CollectionType::Dictionary);
                ndict = ndict.get_dictionary("key");
                ndict.insert("Int2", 6);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                auto nlist = list.get_list(0);
                nlist.insert_collection(0, CollectionType::List);
                nlist = nlist.get_list(0);
                nlist.add(Mixed{7});
                auto ndict = list.get_dictionary(1);
                ndict.insert_collection("key", CollectionType::Dictionary);
                ndict = ndict.get_dictionary("key");
                ndict.insert("Int3", Mixed{9});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto nlist = list.get_list(0);
                    auto ndict = list.get_dictionary(1);
                    REQUIRE(nlist.size() == 3);
                    REQUIRE(ndict.size() == 3);
                    REQUIRE(nlist.get_any(1).get_int() == 1);
                    REQUIRE(nlist.get_any(2).get_string() == "Test");
                    nlist = nlist.get_list(0);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 7);
                    REQUIRE(ndict.get_any("Int").get_int() == 3);
                    REQUIRE(ndict.get_any("String").get_string() == "Test");
                    ndict = ndict.get_dictionary("key");
                    REQUIRE(ndict.size() == 1);
                    REQUIRE(ndict.get_any("Int3").get_int() == 9);
                }
                else {
                    // db must be equal to local
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto nlist = list.get_list(0);
                    auto ndict = list.get_dictionary(1);
                    REQUIRE(nlist.size() == 3);
                    REQUIRE(ndict.size() == 3);
                    REQUIRE(nlist.get_any(1).get_int() == 1);
                    REQUIRE(nlist.get_any(2).get_string() == "Test");
                    auto nlist2 = nlist.get_list(0);
                    REQUIRE(nlist2.size() == 1);
                    REQUIRE(nlist2.get_any(0).get_int() == 4);
                    REQUIRE(ndict.get_any("Int").get_int() == 3);
                    REQUIRE(ndict.get_any("String").get_string() == "Test");
                    ndict = ndict.get_dictionary("key");
                    REQUIRE(ndict.size() == 1);
                    REQUIRE(ndict.get_any("Int2").get_int() == 6);
                }
            })
            ->run();
    }
    SECTION("Verify prefix/suffix copy logic for list in mixed.") {
        // dictionaries go key by key so they have a different logic.
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto nlist = list.get_list(0);
                nlist.add(Mixed{1});
                nlist.add(Mixed{2});
                nlist.add(Mixed{3});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                auto nlist = list.get_list(0);
                REQUIRE(nlist.size() == 3);
                nlist.add(Mixed{4});
                nlist.add(Mixed{5});
                nlist.add(Mixed{6});
                nlist.add(Mixed{7});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 1);
                auto nlist = list.get_list(0);
                REQUIRE(nlist.size() == 3);
                nlist.add(Mixed{4});
                nlist.add(Mixed{5});
                nlist.add(Mixed{8});
                nlist.add(Mixed{9});
                nlist.add(Mixed{6});
                nlist.add(Mixed{7});
                REQUIRE(nlist.size() == 9);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // list must be equal to remote
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 9);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                    REQUIRE(nlist.get_any(1).get_int() == 2);
                    REQUIRE(nlist.get_any(2).get_int() == 3);
                    REQUIRE(nlist.get_any(3).get_int() == 4);
                    REQUIRE(nlist.get_any(4).get_int() == 5);
                    REQUIRE(nlist.get_any(5).get_int() == 8);
                    REQUIRE(nlist.get_any(6).get_int() == 9);
                    REQUIRE(nlist.get_any(7).get_int() == 6);
                    REQUIRE(nlist.get_any(8).get_int() == 7);
                }
                else {
                    // list must be equal to local
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto nlist = list.get_list(0);
                    REQUIRE(nlist.size() == 7);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                    REQUIRE(nlist.get_any(1).get_int() == 2);
                    REQUIRE(nlist.get_any(2).get_int() == 3);
                    REQUIRE(nlist.get_any(3).get_int() == 4);
                    REQUIRE(nlist.get_any(4).get_int() == 5);
                    REQUIRE(nlist.get_any(5).get_int() == 6);
                    REQUIRE(nlist.get_any(6).get_int() == 7);
                }
            })
            ->run();
    }
    SECTION("Verify copy logic for collections in mixed. Mismatch at index i") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                auto nlist = list.get_list(0);
                nlist.add(Mixed{"Local"});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                list.insert_collection(0, CollectionType::Dictionary);
                auto ndict = list.get_dictionary(0);
                ndict.insert("Test", Mixed{"Remote"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    auto ndict = list.get_dictionary(0);
                    REQUIRE(ndict.size() == 1);
                    REQUIRE(ndict.get_any("Test").get_string() == "Remote");
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 2);
                    auto nlist = list.get_list(0);
                    auto ndict = list.get_dictionary(1);
                    REQUIRE(ndict.get_any("Test").get_string() == "Remote");
                    REQUIRE(nlist.get_any(0).get_string() == "Local");
                }
            })
            ->run();
    }
    SECTION("Verify copy logic for List<Mixed>") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("list_mixed");
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                list.insert_collection(1, CollectionType::Dictionary);
                auto nlist = list.get_list(0);
                auto ndict = list.get_dictionary(1);
                nlist.add(Mixed{1});
                nlist.add(Mixed{"Test"});
                ndict.insert("Int", Mixed(3));
                ndict.insert("String", Mixed("Test"));
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("list_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert(2, Mixed{42});
                auto nlist = list.get_list(0);
                nlist.set_any(0, Mixed{2});
                auto ndict = list.get_dictionary(1);
                ndict.insert("Int", Mixed{6});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("list_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert(2, Mixed{43});
                auto nlist = list.get_list(0);
                nlist.set_any(1, Mixed{3});
                auto ndict = list.get_dictionary(1);
                ndict.insert("Int", Mixed{9});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("list_mixed");
                List list{local_realm, obj, col};
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    REQUIRE(list.size() == 3);
                    auto nlist = list.get_list(0);
                    auto ndict = list.get_dictionary(1);
                    REQUIRE(list.get_any(2).get_int() == 43);
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(ndict.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                    REQUIRE(nlist.get_any(1).get_int() == 3);
                    REQUIRE(ndict.get_any("Int").get_int() == 9);
                    REQUIRE(ndict.get_any("String").get_string() == "Test");
                }
                else {
                    // db must be equal to local
                    REQUIRE(list.size() == 3);
                    auto nlist = list.get_list(0);
                    auto ndict = list.get_dictionary(1);
                    REQUIRE(list.get_any(2).get_int() == 42);
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(ndict.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                    REQUIRE(nlist.get_any(1).get_string() == "Test");
                    REQUIRE(ndict.get_any("Int").get_int() == 6);
                    REQUIRE(ndict.get_any("String").get_string() == "Test");
                }
            })
            ->run();
    }
    SECTION("Verify copy logic for Dictionary<Mixed>") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("dictionary_mixed");
                object_store::Dictionary dict{realm, obj, col};
                dict.insert_collection("key1", CollectionType::List);
                dict.insert_collection("key2", CollectionType::Dictionary);
                auto nlist = dict.get_list("key1");
                auto ndict = dict.get_dictionary("key2");
                nlist.add(Mixed{1});
                nlist.add(Mixed{"Test"});
                ndict.insert("Int", Mixed(3));
                ndict.insert("String", Mixed("Test"));
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("dictionary_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 2);
                auto nlist = dict.get_list("key1");
                nlist.set_any(0, Mixed{2});
                auto ndict = dict.get_dictionary("key2");
                ndict.insert("Int", Mixed{6});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("dictionary_mixed");
                object_store::Dictionary dict{remote_realm, obj, col};
                REQUIRE(dict.size() == 2);
                auto nlist = dict.get_list("key1");
                nlist.set_any(1, Mixed{3});
                auto ndict = dict.get_dictionary("key2");
                ndict.insert("String", Mixed("Test2"));
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("dictionary_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    REQUIRE(dict.size() == 2);
                    auto nlist = dict.get_list("key1");
                    auto ndict = dict.get_dictionary("key2");
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(ndict.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                    REQUIRE(nlist.get_any(1).get_int() == 3);
                    REQUIRE(ndict.get_any("Int").get_int() == 3);
                    REQUIRE(ndict.get_any("String").get_string() == "Test2");
                }
                else {
                    // db must be equal to local
                    REQUIRE(dict.size() == 2);
                    auto nlist = dict.get_list("key1");
                    auto ndict = dict.get_dictionary("key2");
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(ndict.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                    REQUIRE(nlist.get_any(1).get_string() == "Test");
                    REQUIRE(ndict.get_any("Int").get_int() == 6);
                    REQUIRE(ndict.get_any("String").get_string() == "Test2");
                }
            })
            ->run();
    }
    SECTION("Verify copy and notification logic for List<List> and scalar types") {
        Results results;
        Object object;
        List list_listener, nlist_setup_listener, nlist_local_listener;
        CollectionChangeSet list_changes, nlist_setup_changes, nlist_local_changes;
        NotificationToken list_token, nlist_setup_token, nlist_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::List);
                list.add(Mixed{"Setup"});
                auto nlist = list.get_list(0);
                nlist.add(Mixed{"Setup"});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert_collection(0, CollectionType::List);
                list.add(Mixed{"Local"});
                auto nlist = list.get_list(0);
                nlist.add(Mixed{"Local"});
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                TableRef table = get_table(*realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                list_listener = List{realm, obj, col};
                REQUIRE(list_listener.size() == 4);
                list_token = list_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    list_changes = std::move(changes);
                });
                auto nlist_setup = list_listener.get_list(1);
                REQUIRE(nlist_setup.size() == 1);
                REQUIRE(nlist_setup.get_any(0) == Mixed{"Setup"});
                nlist_setup_listener = nlist_setup;
                nlist_setup_token = nlist_setup_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    nlist_setup_changes = std::move(changes);
                });
                auto nlist_local = list_listener.get_list(0);
                REQUIRE(nlist_local.size() == 1);
                REQUIRE(nlist_local.get_any(0) == Mixed{"Local"});
                nlist_local_listener = nlist_local;
                nlist_local_token = nlist_local_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    nlist_local_changes = std::move(changes);
                });
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert_collection(0, CollectionType::List);
                list.add(Mixed{"Remote"});
                auto nlist = list.get_list(0);
                nlist.add(Mixed{"Remote"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 4);
                    auto nlist_remote = list.get_list(0);
                    auto nlist_setup = list.get_list(1);
                    auto mixed_setup = list.get_any(2);
                    auto mixed_remote = list.get_any(3);
                    REQUIRE(nlist_remote.size() == 1);
                    REQUIRE(nlist_setup.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(nlist_remote.get_any(0).get_string() == "Remote");
                    REQUIRE(nlist_setup.get_any(0).get_string() == "Setup");
                    REQUIRE(list_listener.is_valid());
                    REQUIRE_INDICES(list_changes.deletions);  // old nested collection deleted
                    REQUIRE_INDICES(list_changes.insertions); // new nested collection inserted
                    REQUIRE_INDICES(list_changes.modifications, 0,
                                    3); // replace Local with Remote at position 0 and 3
                    REQUIRE(!nlist_local_changes.collection_root_was_deleted); // original local collection deleted
                    REQUIRE(!nlist_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_setup_changes.insertions); // there are no new insertions or deletions
                    REQUIRE_INDICES(nlist_setup_changes.deletions);
                    REQUIRE_INDICES(nlist_setup_changes.modifications);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 6);
                    auto nlist_local = list.get_list(0);
                    auto nlist_remote = list.get_list(1);
                    auto nlist_setup = list.get_list(2);
                    auto mixed_local = list.get_any(3);
                    auto mixed_setup = list.get_any(4);
                    auto mixed_remote = list.get_any(5);
                    // local, remote changes are kept
                    REQUIRE(nlist_remote.size() == 1);
                    REQUIRE(nlist_setup.size() == 1);
                    REQUIRE(nlist_local.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(mixed_local.get_string() == "Local");
                    REQUIRE(nlist_remote.get_any(0).get_string() == "Remote");
                    REQUIRE(nlist_local.get_any(0).get_string() == "Local");
                    REQUIRE(nlist_setup.get_any(0).get_string() == "Setup");
                    // notifications
                    REQUIRE(list_listener.is_valid());
                    // src is [ [Local],[Remote],[Setup], Local, Setup, Remote ]
                    // dst is [ [Local], [Setup], Setup, Local]
                    // no deletions
                    REQUIRE_INDICES(list_changes.deletions);
                    // inserted "Setup" and "Remote" at the end
                    REQUIRE_INDICES(list_changes.insertions, 4, 5);
                    // changed [Setup] ==> [Remote] and Setup ==> [Setup]
                    REQUIRE_INDICES(list_changes.modifications, 1, 2);
                    REQUIRE(!nlist_local_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_local_changes.insertions);
                    REQUIRE_INDICES(nlist_local_changes.deletions);
                    REQUIRE(!nlist_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_setup_changes.insertions);
                    REQUIRE_INDICES(nlist_setup_changes.deletions);
                }
            })
            ->run();
    }
    SECTION("Verify copy and notification logic for Dictionary<List> and scalar types") {
        Results results;
        Object object;
        object_store::Dictionary dictionary_listener;
        List nlist_setup_listener, nlist_local_listener;
        CollectionChangeSet dictionary_changes, nlist_setup_changes, nlist_local_changes;
        NotificationToken dictionary_token, nlist_setup_token, nlist_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("[Setup]", CollectionType::List);
                dictionary.insert("Setup", Mixed{"Setup"});
                auto nlist = dictionary.get_list("[Setup]");
                nlist.add(Mixed{"Setup"});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                REQUIRE(dictionary.size() == 2);
                dictionary.insert_collection("[Local]", CollectionType::List);
                dictionary.insert("Local", Mixed{"Local"});
                auto nlist = dictionary.get_list("[Local]");
                nlist.add(Mixed{"Local"});
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                TableRef table = get_table(*realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                dictionary_listener = object_store::Dictionary{realm, obj, col};
                REQUIRE(dictionary_listener.size() == 4);
                dictionary_token = dictionary_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    dictionary_changes = std::move(changes);
                });
                auto nlist_setup = dictionary_listener.get_list("[Setup]");
                REQUIRE(nlist_setup.size() == 1);
                REQUIRE(nlist_setup.get_any(0) == Mixed{"Setup"});
                nlist_setup_listener = nlist_setup;
                nlist_setup_token = nlist_setup_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    nlist_setup_changes = std::move(changes);
                });
                auto nlist_local = dictionary_listener.get_list("[Local]");
                REQUIRE(nlist_local.size() == 1);
                REQUIRE(nlist_local.get_any(0) == Mixed{"Local"});
                nlist_local_listener = nlist_local;
                nlist_local_token = nlist_local_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    nlist_local_changes = std::move(changes);
                });
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                REQUIRE(dictionary.size() == 2);
                dictionary.insert_collection("[Remote]", CollectionType::List);
                dictionary.insert("Remote", Mixed{"Remote"});
                auto nlist = dictionary.get_list("[Remote]");
                nlist.add(Mixed{"Remote"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 4);
                    auto nlist_remote = dictionary.get_list("[Remote]");
                    auto nlist_setup = dictionary.get_list("[Setup]");
                    auto mixed_setup = dictionary.get_any("Setup");
                    auto mixed_remote = dictionary.get_any("Remote");
                    REQUIRE(nlist_remote.size() == 1);
                    REQUIRE(nlist_setup.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(nlist_remote.get_any(0).get_string() == "Remote");
                    REQUIRE(nlist_setup.get_any(0).get_string() == "Setup");
                    REQUIRE(dictionary_listener.is_valid());
                    REQUIRE_INDICES(dictionary_changes.deletions, 0, 2);  // remove [Local], Local
                    REQUIRE_INDICES(dictionary_changes.insertions, 0, 2); // insert [Remote], Remote
                    REQUIRE_INDICES(
                        dictionary_changes.modifications); // replace Local with Remote at position 0 and 3
                    REQUIRE(nlist_local_changes.collection_root_was_deleted); // local list is deleted
                    REQUIRE(!nlist_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_setup_changes.insertions); // there are no new insertions or deletions
                    REQUIRE_INDICES(nlist_setup_changes.deletions);
                    REQUIRE_INDICES(nlist_setup_changes.modifications);
                }
                else {
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 6);
                    auto nlist_local = dictionary.get_list("[Local]");
                    auto nlist_remote = dictionary.get_list("[Remote]");
                    auto nlist_setup = dictionary.get_list("[Setup]");
                    auto mixed_local = dictionary.get_any("Local");
                    auto mixed_setup = dictionary.get_any("Setup");
                    auto mixed_remote = dictionary.get_any("Remote");
                    // local, remote changes are kept
                    REQUIRE(nlist_remote.size() == 1);
                    REQUIRE(nlist_setup.size() == 1);
                    REQUIRE(nlist_local.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(mixed_local.get_string() == "Local");
                    REQUIRE(nlist_remote.get_any(0).get_string() == "Remote");
                    REQUIRE(nlist_local.get_any(0).get_string() == "Local");
                    REQUIRE(nlist_setup.get_any(0).get_string() == "Setup");
                    // notifications
                    REQUIRE(dictionary_listener.is_valid());
                    // src is [ [Local],[Remote],[Setup], Local, Setup, Remote ]
                    // dst is [ [Local], [Setup], Setup, Local]
                    // no deletions
                    REQUIRE_INDICES(dictionary_changes.deletions);
                    // inserted "[Remote]" and "Remote"
                    REQUIRE_INDICES(dictionary_changes.insertions, 1, 4);
                    REQUIRE_INDICES(dictionary_changes.modifications);
                    REQUIRE(!nlist_local_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_local_changes.insertions);
                    REQUIRE_INDICES(nlist_local_changes.deletions);
                    REQUIRE(!nlist_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(nlist_setup_changes.insertions);
                    REQUIRE_INDICES(nlist_setup_changes.deletions);
                }
            })
            ->run();
    }
    SECTION("Verify copy and notification logic for List<Dictionary> and scalar types") {
        Results results;
        Object object;
        List list_listener;
        object_store::Dictionary ndictionary_setup_listener, ndictionary_local_listener;
        CollectionChangeSet list_changes, ndictionary_setup_changes, ndictionary_local_changes;
        NotificationToken list_token, ndictionary_setup_token, ndictionary_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.insert_collection(0, CollectionType::Dictionary);
                list.add(Mixed{"Setup"});
                auto ndictionary = list.get_dictionary(0);
                ndictionary.insert("Key", Mixed{"Setup"});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert_collection(0, CollectionType::Dictionary);
                list.add(Mixed{"Local"});
                auto ndictionary = list.get_dictionary(0);
                ndictionary.insert("Key", Mixed{"Local"});
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                TableRef table = get_table(*realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                list_listener = List{realm, obj, col};
                REQUIRE(list_listener.size() == 4);
                list_token = list_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    list_changes = std::move(changes);
                });
                auto ndictionary_setup = list_listener.get_dictionary(1);
                REQUIRE(ndictionary_setup.size() == 1);
                REQUIRE(ndictionary_setup.get_any("Key") == Mixed{"Setup"});
                ndictionary_setup_listener = ndictionary_setup;
                ndictionary_setup_token =
                    ndictionary_setup_listener.add_notification_callback([&](CollectionChangeSet changes) {
                        ndictionary_setup_changes = std::move(changes);
                    });
                auto ndictionary_local = list_listener.get_dictionary(0);
                REQUIRE(ndictionary_local.size() == 1);
                REQUIRE(ndictionary_local.get_any("Key") == Mixed{"Local"});
                ndictionary_local_listener = ndictionary_local;
                ndictionary_local_token =
                    ndictionary_local_listener.add_notification_callback([&](CollectionChangeSet changes) {
                        ndictionary_local_changes = std::move(changes);
                    });
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.insert_collection(0, CollectionType::Dictionary);
                list.add(Mixed{"Remote"});
                auto ndictionary = list.get_dictionary(0);
                ndictionary.insert("Key", Mixed{"Remote"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 4);
                    auto ndictionary_remote = list.get_dictionary(0);
                    auto ndictionary_setup = list.get_dictionary(1);
                    auto mixed_setup = list.get_any(2);
                    auto mixed_remote = list.get_any(3);
                    REQUIRE(ndictionary_remote.size() == 1);
                    REQUIRE(ndictionary_setup.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(ndictionary_remote.get_any("Key").get_string() == "Remote");
                    REQUIRE(ndictionary_setup.get_any("Key").get_string() == "Setup");
                    REQUIRE(list_listener.is_valid());
                    REQUIRE_INDICES(list_changes.deletions);  // old nested collection deleted
                    REQUIRE_INDICES(list_changes.insertions); // new nested collection inserted
                    REQUIRE_INDICES(list_changes.modifications, 0,
                                    3); // replace Local with Remote at position 0 and 3
                    REQUIRE(
                        !ndictionary_local_changes.collection_root_was_deleted); // original local collection deleted
                    REQUIRE(!ndictionary_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_setup_changes.insertions); // there are no new insertions or deletions
                    REQUIRE_INDICES(ndictionary_setup_changes.deletions);
                    REQUIRE_INDICES(ndictionary_setup_changes.modifications);
                }
                else {
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 6);
                    auto ndictionary_local = list.get_dictionary(0);
                    auto ndictionary_remote = list.get_dictionary(1);
                    auto ndictionary_setup = list.get_dictionary(2);
                    auto mixed_local = list.get_any(3);
                    auto mixed_setup = list.get_any(4);
                    auto mixed_remote = list.get_any(5);
                    // local, remote changes are kept
                    REQUIRE(ndictionary_remote.size() == 1);
                    REQUIRE(ndictionary_setup.size() == 1);
                    REQUIRE(ndictionary_local.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(mixed_local.get_string() == "Local");
                    REQUIRE(ndictionary_remote.get_any("Key").get_string() == "Remote");
                    REQUIRE(ndictionary_local.get_any("Key").get_string() == "Local");
                    REQUIRE(ndictionary_setup.get_any("Key").get_string() == "Setup");
                    // notifications
                    REQUIRE(list_listener.is_valid());
                    // src is [ [Local],[Remote],[Setup], Local, Setup, Remote ]
                    // dst is [ [Local], [Setup], Setup, Local]
                    // no deletions
                    REQUIRE_INDICES(list_changes.deletions);
                    // inserted "Setup" and "Remote" at the end
                    REQUIRE_INDICES(list_changes.insertions, 4, 5);
                    // changed [Setup] ==> [Remote] and Setup ==> [Setup]
                    REQUIRE_INDICES(list_changes.modifications, 1, 2);
                    REQUIRE(!ndictionary_local_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_local_changes.insertions);
                    REQUIRE_INDICES(ndictionary_local_changes.deletions);
                    REQUIRE(!ndictionary_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_setup_changes.insertions);
                    REQUIRE_INDICES(ndictionary_setup_changes.deletions);
                }
            })
            ->run();
    }
    SECTION("Verify copy and notification logic for Dictionary<Dictionary> and scalar types") {
        Results results;
        Object object;
        object_store::Dictionary dictionary_listener, ndictionary_setup_listener, ndictionary_local_listener;
        CollectionChangeSet dictionary_changes, ndictionary_setup_changes, ndictionary_local_changes;
        NotificationToken dictionary_token, ndictionary_setup_token, ndictionary_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("<Setup>", CollectionType::Dictionary);
                dictionary.insert("Key-Setup", Mixed{"Setup"});
                auto ndictionary = dictionary.get_dictionary("<Setup>");
                ndictionary.insert("Key", Mixed{"Setup"});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                dictionary.insert_collection("<Local>", CollectionType::Dictionary);
                dictionary.insert("Key-Local", Mixed{"Local"});
                auto ndictionary = dictionary.get_dictionary("<Local>");
                ndictionary.insert("Key", Mixed{"Local"});
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                TableRef table = get_table(*realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                dictionary_listener = object_store::Dictionary{realm, obj, col};
                REQUIRE(dictionary_listener.size() == 4);
                dictionary_token = dictionary_listener.add_notification_callback([&](CollectionChangeSet changes) {
                    dictionary_changes = std::move(changes);
                });
                auto ndictionary_setup = dictionary_listener.get_dictionary("<Setup>");
                REQUIRE(ndictionary_setup.size() == 1);
                REQUIRE(ndictionary_setup.get_any("Key") == Mixed{"Setup"});
                ndictionary_setup_listener = ndictionary_setup;
                ndictionary_setup_token =
                    ndictionary_setup_listener.add_notification_callback([&](CollectionChangeSet changes) {
                        ndictionary_setup_changes = std::move(changes);
                    });
                auto ndictionary_local = dictionary_listener.get_dictionary("<Local>");
                REQUIRE(ndictionary_local.size() == 1);
                REQUIRE(ndictionary_local.get_any("Key") == Mixed{"Local"});
                ndictionary_local_listener = ndictionary_local;
                ndictionary_local_token =
                    ndictionary_local_listener.add_notification_callback([&](CollectionChangeSet changes) {
                        ndictionary_local_changes = std::move(changes);
                    });
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                REQUIRE(dictionary.size() == 2);
                dictionary.insert_collection("<Remote>", CollectionType::Dictionary);
                dictionary.insert("Key-Remote", Mixed{"Remote"});
                auto ndictionary = dictionary.get_dictionary("<Remote>");
                ndictionary.insert("Key", Mixed{"Remote"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object(0);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 4);
                    auto ndictionary_remote = dictionary.get_dictionary("<Remote>");
                    auto ndictionary_setup = dictionary.get_dictionary("<Setup>");
                    auto mixed_setup = dictionary.get_any("Key-Setup");
                    auto mixed_remote = dictionary.get_any("Key-Remote");
                    REQUIRE(ndictionary_remote.size() == 1);
                    REQUIRE(ndictionary_setup.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(ndictionary_remote.get_any("Key").get_string() == "Remote");
                    REQUIRE(ndictionary_setup.get_any("Key").get_string() == "Setup");
                    REQUIRE(dictionary_listener.is_valid());
                    REQUIRE_INDICES(dictionary_changes.deletions, 0, 2);
                    REQUIRE_INDICES(dictionary_changes.insertions, 0, 2);
                    REQUIRE_INDICES(dictionary_changes.modifications);
                    REQUIRE(ndictionary_local_changes.collection_root_was_deleted);
                    REQUIRE(!ndictionary_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_setup_changes.insertions);
                    REQUIRE_INDICES(ndictionary_setup_changes.deletions);
                    REQUIRE_INDICES(ndictionary_setup_changes.modifications);
                }
                else {
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    REQUIRE(dictionary.size() == 6);
                    auto ndictionary_local = dictionary.get_dictionary("<Local>");
                    auto ndictionary_remote = dictionary.get_dictionary("<Remote>");
                    auto ndictionary_setup = dictionary.get_dictionary("<Setup>");
                    auto mixed_local = dictionary.get_any("Key-Local");
                    auto mixed_setup = dictionary.get_any("Key-Setup");
                    auto mixed_remote = dictionary.get_any("Key-Remote");
                    // local, remote changes are kept
                    REQUIRE(ndictionary_remote.size() == 1);
                    REQUIRE(ndictionary_setup.size() == 1);
                    REQUIRE(ndictionary_local.size() == 1);
                    REQUIRE(mixed_setup.get_string() == "Setup");
                    REQUIRE(mixed_remote.get_string() == "Remote");
                    REQUIRE(mixed_local.get_string() == "Local");
                    REQUIRE(ndictionary_remote.get_any("Key").get_string() == "Remote");
                    REQUIRE(ndictionary_local.get_any("Key").get_string() == "Local");
                    REQUIRE(ndictionary_setup.get_any("Key").get_string() == "Setup");
                    // notifications
                    REQUIRE(dictionary_listener.is_valid());
                    // src is [ [Local],[Remote],[Setup], Local, Setup, Remote ]
                    // dst is [ [Local], [Setup], Setup, Local]
                    // no deletions
                    REQUIRE_INDICES(dictionary_changes.deletions);
                    REQUIRE_INDICES(dictionary_changes.insertions, 1, 4);
                    REQUIRE_INDICES(dictionary_changes.modifications);
                    REQUIRE(!ndictionary_local_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_local_changes.insertions);
                    REQUIRE_INDICES(ndictionary_local_changes.deletions);
                    REQUIRE(!ndictionary_setup_changes.collection_root_was_deleted);
                    REQUIRE_INDICES(ndictionary_setup_changes.insertions);
                    REQUIRE_INDICES(ndictionary_setup_changes.deletions);
                }
            })
            ->run();
    }
    SECTION("Verify Links Nested Collections") {
        Results results;
        Object object;
        object_store::Dictionary dictionary_listener, ndictionary_setup_listener, ndictionary_local_listener;
        CollectionChangeSet dictionary_changes, ndictionary_setup_changes, ndictionary_local_changes;
        NotificationToken dictionary_token, ndictionary_setup_token, ndictionary_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");

        config.schema = Schema{shared_class,
                               {"TopLevel",
                                {
                                    {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                    {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
                                }},
                               {"Other",
                                {
                                    {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                    {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
                                }}};

        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto other_table = get_table(*realm, "Other");

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                auto other_obj = other_table->create_object_with_primary_key(pk_val);

                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("<Setup>", CollectionType::Dictionary);
                dictionary.insert("Key-Setup", Mixed{"Setup"});
                auto ndictionary = dictionary.get_dictionary("<Setup>");
                ndictionary.insert("Key", other_obj.get_link());

                CHECK(other_obj.get_backlink_count() == 1);
                CHECK(table->query("any_mixed['Key-Setup'].@type == 'string'").count() == 1);
                CHECK(table->query("any_mixed['Key-Setup'] == 'Setup'").count() == 1);
                CHECK(table->query("any_mixed['<Setup>'].@type == 'dictionary'").count() == 1);
                CHECK(table->query("any_mixed['<Setup>'].@size == 1").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].@type == 'link'").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key']._id == $0", std::vector<Mixed>{Mixed{pk_val}})
                          .count() == 1);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                auto table = get_table(*local_realm, "TopLevel");
                auto other_table = get_table(*local_realm, "Other");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);
                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("<Setup>");
                ndictionary.insert("Key", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("Key");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(local_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@size == 2").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[1] == 2").count() == 1);
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                advance_and_notify(*realm);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto other_table = get_table(*remote_realm, "Other");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);

                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});
                list.add({3});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("<Setup>");
                ndictionary.insert("Key", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("Key");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(remote_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(other_obj.get_backlink_count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@size == 3").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[1] == 2").count() == 1);
                CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[2] == 3").count() == 1);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                TableRef other_table = get_table(*local_realm, "Other");
                REQUIRE(table->size() == 1);
                REQUIRE(other_table->size() == 1);
                auto obj = table->get_object(0);
                auto other_obj = other_table->get_object(0);
                auto col = table->get_column_key("any_mixed");
                auto other_col = other_table->get_column_key("any_mixed");
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@type == 'list'").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@size == 3").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[1] == 2").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[2] == 3").count() == 1);
                }
                else {
                    // recover we should try to recover the links
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    CHECK(dictionary.size() == 2);
                    auto ndictionary = dictionary.get_dictionary("<Setup>");
                    auto mixed = ndictionary.get_any("Key");
                    CHECK(mixed.get_type() == type_TypedLink);
                    auto link = mixed.get_link();
                    auto obj = other_table->get_object(link.get_obj_key());
                    CHECK(obj.is_valid());
                    CHECK(other_obj.get_key() == obj.get_key());
                    List list{local_realm, obj, other_col};
                    CHECK(list.size() == 5);
                    std::vector<int> expected{1, 2, 1, 2, 3};
                    for (int i = 0; i < 5; ++i) {
                        CHECK(list.get_any(i).get_int() == expected[i]);
                    }
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@type == 'list'").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed.@size == 5").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[1] == 2").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[2] == 1").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[3] == 2").count() == 1);
                    CHECK(table->query("any_mixed['<Setup>']['Key'].any_mixed[4] == 3").count() == 1);
                }
            })
            ->run();
    }
    SECTION("Verify Links Nested Collections different links same key") {
        Results results;
        Object object;
        object_store::Dictionary dictionary_listener, ndictionary_setup_listener, ndictionary_local_listener;
        CollectionChangeSet dictionary_changes, ndictionary_setup_changes, ndictionary_local_changes;
        NotificationToken dictionary_token, ndictionary_setup_token, ndictionary_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");

        config.schema = Schema{
            shared_class,
            {"TopLevel",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
            {"Other_one",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
            {"Other_two",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
        };

        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("MyDictionary", CollectionType::Dictionary);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                auto table = get_table(*local_realm, "TopLevel");
                auto other_table = get_table(*local_realm, "Other_one");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);
                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("MyDictionary");
                ndictionary.insert("Key", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("Key");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(local_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@size == 2").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[1] == 2").count() == 1);
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                advance_and_notify(*realm);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto other_table = get_table(*remote_realm, "Other_two");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);
                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});
                list.add({3});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("MyDictionary");
                ndictionary.insert("Key", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("Key");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(remote_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@size == 3").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[1] == 2").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[2] == 3").count() == 1);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@type == 'list'").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@size == 3").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[1] == 2").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[2] == 3").count() == 1);
                }
                else {
                    TableRef other_table_one = get_table(*local_realm, "Other_one");
                    TableRef other_table_two = get_table(*local_realm, "Other_two");
                    REQUIRE(other_table_one->size() == 1);
                    REQUIRE(other_table_two->size() == 1);
                    auto obj = table->get_object(0);
                    auto other_obj_one = other_table_one->get_object(0);
                    auto other_obj_two = other_table_two->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    auto other_col_one = other_table_one->get_column_key("any_mixed");
                    auto other_col_two = other_table_two->get_column_key("any_mixed");

                    // check that the link change was recovered, but that the state
                    // of each destination object did not change
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    CHECK(dictionary.size() == 1);
                    auto ndictionary = dictionary.get_dictionary("MyDictionary");
                    auto mixed = ndictionary.get_any("Key");
                    CHECK(mixed.get_type() == type_TypedLink);
                    auto link = mixed.get_link();
                    auto obj_two = other_table_two->get_object(link.get_obj_key());
                    CHECK(obj_two.is_valid());
                    CHECK(other_obj_two.get_key() == obj_two.get_key());
                    {
                        List list{local_realm, obj_two, other_col_two};
                        CHECK(list.size() == 3);
                        std::vector<int> expected{1, 2, 3};
                        for (int i = 0; i < 3; ++i) {
                            CHECK(list.get_any(i).get_int() == expected[i]);
                        }
                    }
                    {
                        List list{local_realm, other_obj_one, other_col_one};
                        CHECK(list.size() == 2);
                        std::vector<int> expected{1, 2};
                        for (int i = 0; i < 2; ++i) {
                            CHECK(list.get_any(i).get_int() == expected[i]);
                        }
                    }
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@type == 'list'").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed.@size == 2").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['Key'].any_mixed[1] == 2").count() == 1);
                    CHECK(other_table_one->query("any_mixed.@size == 2").count() == 1);
                    CHECK(other_table_two->query("any_mixed.@size == 3").count() == 1);
                }
            })
            ->run();
    }
    SECTION("Verify Links Nested Collections different links different keys") {
        Results results;
        Object object;
        object_store::Dictionary dictionary_listener, ndictionary_setup_listener, ndictionary_local_listener;
        CollectionChangeSet dictionary_changes, ndictionary_setup_changes, ndictionary_local_changes;
        NotificationToken dictionary_token, ndictionary_setup_token, ndictionary_local_token;

        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");

        config.schema = Schema{
            shared_class,
            {"TopLevel",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
            {"Other_one",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
            {"Other_two",
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"any_mixed", PropertyType::Mixed | PropertyType::Nullable},
             }},
        };

        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("MyDictionary", CollectionType::Dictionary);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                auto table = get_table(*local_realm, "TopLevel");
                auto other_table = get_table(*local_realm, "Other_one");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);
                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{local_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("MyDictionary");
                ndictionary.insert("KeyLocal", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("KeyLocal");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(local_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed.@size == 2").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed[1] == 2").count() == 1);
            })
            ->on_post_local_changes([&](SharedRealm realm) {
                advance_and_notify(*realm);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                advance_and_notify(*remote_realm);
                auto table = get_table(*remote_realm, "TopLevel");
                auto other_table = get_table(*remote_realm, "Other_two");
                auto other_obj = other_table->create_object_with_primary_key(pk_val);
                auto other_col = other_table->get_column_key("any_mixed");
                other_obj.set_collection(other_col, CollectionType::List);
                auto list = other_obj.get_list<Mixed>(other_col);
                list.add({1});
                list.add({2});
                list.add({3});

                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dictionary{remote_realm, obj, col};
                auto ndictionary = dictionary.get_dictionary("MyDictionary");
                ndictionary.insert("KeyRemote", other_obj.get_link());
                CHECK(other_obj.get_backlink_count() == 1);

                auto link = ndictionary.get_any("KeyRemote");
                CHECK(other_obj.get_key() == link.get_link().get_obj_key());
                CHECK(other_obj.get_table()->get_key() == link.get_link().get_table_key());
                auto linked_obj = other_table->get_object(link.get_link().get_obj_key());
                List list_linked(remote_realm, linked_obj, other_col);
                CHECK(list_linked.size() == list.size());
                for (size_t i = 0; i < list.size(); ++i) {
                    CHECK(list_linked.get_any(i).get_int() == list.get_any(i).get_int());
                }
                CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@type == 'list'").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@size == 3").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[0] == 1").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[1] == 2").count() == 1);
                CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[2] == 3").count() == 1);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // db must be equal to remote
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@type == 'list'").count() ==
                          1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@size == 3").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[1] == 2").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[2] == 3").count() == 1);
                }
                else {
                    TableRef other_table_one = get_table(*local_realm, "Other_one");
                    TableRef other_table_two = get_table(*local_realm, "Other_two");
                    REQUIRE(other_table_one->size() == 1);
                    REQUIRE(other_table_two->size() == 1);
                    auto obj = table->get_object(0);
                    auto other_obj_one = other_table_one->get_object(0);
                    auto other_obj_two = other_table_two->get_object(0);
                    auto col = table->get_column_key("any_mixed");
                    auto other_col_one = other_table_one->get_column_key("any_mixed");
                    auto other_col_two = other_table_two->get_column_key("any_mixed");

                    // recover we should try to recover the links
                    object_store::Dictionary dictionary{local_realm, obj, col};
                    CHECK(dictionary.size() == 1);
                    auto ndictionary = dictionary.get_dictionary("MyDictionary");
                    CHECK(ndictionary.size() == 2);

                    auto mixed_remote = ndictionary.get_any("KeyRemote");
                    CHECK(mixed_remote.get_type() == type_TypedLink);
                    auto link = mixed_remote.get_link();
                    auto obj_two = other_table_two->get_object(link.get_obj_key());
                    CHECK(obj_two.is_valid());
                    CHECK(other_obj_two.get_key() == obj_two.get_key());
                    List list{local_realm, obj_two, other_col_two};
                    CHECK(list.size() == 3);
                    std::vector<int> expected{1, 2, 3};
                    for (int i = 0; i < 3; ++i) {
                        CHECK(list.get_any(i).get_int() == expected[i]);
                    }

                    auto mixed_local = ndictionary.get_any("KeyLocal");
                    CHECK(mixed_local.get_type() == type_TypedLink);
                    link = mixed_local.get_link();
                    auto obj_one = other_table_one->get_object(link.get_obj_key());
                    CHECK(obj_one.is_valid());
                    CHECK(other_obj_one.get_key() == obj_one.get_key());
                    List list1{local_realm, obj_one, other_col_one};
                    CHECK(list1.size() == 2);
                    std::vector<int> expected1{1, 2};
                    for (int i = 0; i < 2; ++i) {
                        CHECK(list1.get_any(i).get_int() == expected1[i]);
                    }
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@type == 'list'").count() ==
                          1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed.@size == 3").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[1] == 2").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyRemote'].any_mixed[2] == 3").count() == 1);

                    CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed.@type == 'list'").count() ==
                          1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed.@size == 2").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed[0] == 1").count() == 1);
                    CHECK(table->query("any_mixed['MyDictionary']['KeyLocal'].any_mixed[1] == 2").count() == 1);
                }
            })
            ->run();
    }
    SECTION("Append to list that was reduced in size remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1, 2, 3]}}}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::Dictionary);
                auto ndictionary = dictionary.get_dictionary("key1");
                ndictionary.insert_collection("key2", CollectionType::List);
                auto nlist = ndictionary.get_list("key2");
                nlist.add(Mixed{1});
                nlist.add(Mixed{2});
                nlist.add(Mixed{3});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1, 2, 3, 4, [5]]}}}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});
                nlist->add(Mixed{4});
                REQUIRE(nlist->size() == 4);
                nlist->insert_collection(4, CollectionType::List);
                nlist = nlist->get_list(4);
                nlist->add(Mixed{5});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [2, 3]}}}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});
                nlist->remove(0);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [2, 3]}}}}}
                    REQUIRE(nlist->size() == 2);
                    REQUIRE(nlist->get_any(0).get_int() == 2);
                    REQUIRE(nlist->get_any(1).get_int() == 3);
                }
                else {
                    // Index of the recovered instruction is updated accordingly.
                    // Result: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [2, 3, 4, [5]]}}}}}
                    REQUIRE(nlist->size() == 4);
                    REQUIRE(nlist->get_any(0).get_int() == 2);
                    REQUIRE(nlist->get_any(1).get_int() == 3);
                    REQUIRE(nlist->get_any(2).get_int() == 4);
                    nlist = nlist->get_list(3);
                    REQUIRE(nlist->size() == 1);
                    REQUIRE(nlist->get_any(0).get_int() == 5);
                }
            })
            ->run();
    }
    SECTION("Operating on local list does not trigger a copy") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": [1, [2]]}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::List);
                auto nlist = dictionary.get_list("key1");
                nlist.add(Mixed{1});
                nlist.insert_collection(1, CollectionType::List);
                nlist = nlist.get_list(1);
                nlist.add(Mixed{2});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": [1, [2], 3, [4]]}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                REQUIRE(nlist->size() == 2);
                // Insert element and then immediatelly after update it.
                nlist->add(Mixed{42});
                nlist->set_any(2, Mixed{3});
                // Insert nested list.
                nlist->insert_collection(3, CollectionType::List);
                nlist = nlist->get_list(3);
                nlist->add(Mixed{4});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key1": [1, [2], 5]}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                nlist->add(Mixed{5});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // list must be equal to remote
                    // Result: {"_id": <id>, "any_mixed": {{"key1": [1, [2], 5]}}}
                    REQUIRE(nlist->size() == 3);
                    REQUIRE(nlist->get_any(0).get_int() == 1);
                    auto nlist2 = nlist->get_list(1);
                    REQUIRE(nlist2->size() == 1);
                    REQUIRE(nlist2->get_any(0).get_int() == 2);
                    REQUIRE(nlist->get_any(2).get_int() == 5);
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": [1, [2], 3, [4], 5]}}}
                    REQUIRE(nlist->size() == 5);
                    REQUIRE(nlist->get_any(0).get_int() == 1);
                    auto nlist2 = nlist->get_list(1);
                    REQUIRE(nlist2->size() == 1);
                    REQUIRE(nlist2->get_any(0).get_int() == 2);
                    REQUIRE(nlist->get_any(2).get_int() == 3);
                    nlist2 = nlist->get_list(3);
                    REQUIRE(nlist2->size() == 1);
                    REQUIRE(nlist2->get_any(0).get_int() == 4);
                    REQUIRE(nlist->get_any(4).get_int() == 5);
                }
            })
            ->run();
    }

    // Test type mismatch in the instruction path.

    SECTION("List changed into Dictionary remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": [1]}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.add(Mixed{1});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": [1, 2, 3]}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                list.add(Mixed{2});
                list.add(Mixed{3});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key": "value"}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // Change type from list to dictionary
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{remote_realm, obj, col};
                dictionary.insert("key", "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                // Result: {"_id": <id>, "any_mixed": {{"key": "value"}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // In the recovery case, the local instructions cannot be recovered
                // because the property type changed.
                object_store::Dictionary dictionary{local_realm, obj, col};
                REQUIRE(dictionary.size() == 1);
                REQUIRE(dictionary.get_any("key").get_string() == "value");
            })
            ->run();
    }
    SECTION("Dictionary changed into List remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key": 42}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{realm, obj, col};
                dict.insert("key", 42);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key": 42}, {"key2": 1}, {"key3": 2}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 1);
                dict.insert("key2", 1);
                dict.insert("key3", 2);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": ["value"]}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // Change type from dictionary to list
                obj.set_collection(col, CollectionType::List);
                List list{remote_realm, obj, col};
                list.add(Mixed{"value"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                // Result: {"_id": <id>, "any_mixed": ["value"]}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // In the recovery case, the local instructions cannot be recovered
                // because the property type changed.
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                REQUIRE(list.get_any(0).get_string() == "value");
            })
            ->run();
    }
    SECTION("List changed into string remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": [1]}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.add(Mixed{1});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": [1, 2, 3]}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                list.add(Mixed{2});
                list.add(Mixed{3});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": "value"}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // Change type from list to string
                obj.set_any(col, "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                // Result: {"_id": <id>, "any_mixed": "value"}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // In the recovery case, the local instructions cannot be recovered
                // because the property type changed.
                REQUIRE(obj.get_any(col) == "value");
            })
            ->run();
    }
    SECTION("Key in intermediate dictionary does not exist") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": {{"key2": []}}}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::Dictionary);
                auto ndictionary = dictionary.get_dictionary("key1");
                ndictionary.insert_collection("key2", CollectionType::List);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1]}}}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});
                nlist->add(Mixed{1});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key3": "value"}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{remote_realm, obj, col};
                // Remove dictionary at 'key1' so the path to local insert does not exist anymore.
                dict.erase("key1");
                dict.insert("key3", "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                // Result: {"_id": <id>, "any_mixed": {{"key3": "value"}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // In the recovery case, the local instructions cannot be recovered
                // because the path does not exist anymore.
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 1);
                REQUIRE(dict.get_any("key3").get_string() == "value");
            })
            ->run();
    }
    SECTION("Intermediate dictionary changed into string remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": {{"key2": []}}}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::Dictionary);
                auto ndictionary = dictionary.get_dictionary("key1");
                ndictionary.insert_collection("key2", CollectionType::List);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1]}}}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});
                nlist->add(Mixed{1});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key1": "value"}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto ndict = obj.get_dictionary_ptr({col});
                // Change type of value at 'key1' so the path to local insert does not exist anymore.
                ndict->insert("key1", "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                // Result: {"_id": <id>, "any_mixed": {{"key1": "value"}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // In the recovery case, the local instructions cannot be recovered
                // because the path does not exist anymore.
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 1);
                REQUIRE(dict.get_any("key1").get_string() == "value");
            })
            ->run();
    }
    SECTION("Accessing ambiguous index triggers list copy") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": [1, [2]]}, {"key2": 42}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::List);
                dictionary.insert("key2", Mixed{42});
                auto nlist = dictionary.get_list("key1");
                nlist.add(Mixed{1});
                nlist.insert_collection(1, CollectionType::List);
                nlist = nlist.get_list(1);
                nlist.add(Mixed{2});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": [1, [2, 3]]}, {"key2": 42}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // this insert operation triggers the list copy because the index becomes ambiguous
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", 1});
                nlist->add(Mixed{3});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key1": ["value", [2]]}, {"key2": 43}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto dict = obj.get_dictionary(col);
                dict.insert("key2", Mixed{43});
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                nlist->set_any(0, Mixed{"value"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 2);
                REQUIRE(dict.get_any("key2").get_int() == 43);
                auto nlist = dict.get_list("key1");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": ["value", [2]]}, {"key2": 43}}}
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_string() == "value");
                    nlist = nlist.get_list(1);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": [1, [2, 3]]}, {"key2": 43}}}
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                    nlist = nlist.get_list(1);
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                    REQUIRE(nlist.get_any(1).get_int() == 3);
                }
            })
            ->run();
    }
    SECTION("List copy for list three levels deep") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1]}}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{realm, obj, col};
                dict.insert_collection("key1", CollectionType::Dictionary);
                auto ndict = dict.get_dictionary("key1");
                ndict.insert_collection("key2", CollectionType::List);
                auto nlist = ndict.get_list("key2");
                nlist.add(Mixed{1});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [42]}}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // this set operation triggers the list copy because the index becomes ambiguous
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", "key2"});
                nlist->set_any(0, Mixed{42});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 1);
                auto ndict = dict.get_dictionary("key1");
                REQUIRE(ndict.size() == 1);
                auto nlist = ndict.get_list("key2");
                REQUIRE(nlist.size() == 1);

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [1]}}}}
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": {{"key1": {{"key2": [42]}}}}
                    REQUIRE(nlist.get_any(0).get_int() == 42);
                }
            })
            ->run();
    }
    SECTION("List marked to be copied but path to it does not exist anymore") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": [1, [2]]}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::List);
                auto nlist = dictionary.get_list("key1");
                nlist.add(Mixed{1});
                nlist.insert_collection(1, CollectionType::List);
                nlist = nlist.get_list(1);
                nlist.add(Mixed{2});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // this insert operation triggers the list copy because the index becomes ambiguous
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", 1});
                nlist->add(Mixed{3});
                // Remove list at 'key1' so path above becomes invalid.
                auto ndict = obj.get_dictionary(col);
                ndict.erase("key1");
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key1": [[2]]}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                // Remove first element in list at 'key1'
                nlist->remove(0);
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // list must be equal to remote
                    // Result: {"_id": <id>, "any_mixed": {{"key1": [[2]]}}}
                    auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                    REQUIRE(nlist->size() == 1);
                    nlist = nlist->get_list(0);
                    REQUIRE(nlist->size() == 1);
                    REQUIRE(nlist->get_any(0).get_int() == 2);
                }
                else {
                    // list must be equal to local
                    // Result: {"_id": <id>, "any_mixed": {}}
                    auto ndict = obj.get_dictionary(col);
                    REQUIRE(ndict.size() == 0);
                }
            })
            ->run();
    }
    SECTION("List marked to be copied but it was changed to string locally") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": [42]}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.add(Mixed{42});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": "value"}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                // this set operation triggers the list copy because the index becomes ambiguous
                list.set_any(0, Mixed{43});
                // change list to string
                obj.set_any(col, Mixed{"value"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": [42]}
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    REQUIRE(list.get_any(0).get_int() == 42);
                }
                else {
                    // list changed into string
                    // Result: {"_id": <id>, "any_mixed": "value"}
                    REQUIRE(obj.get_any(col).get_string() == "value");
                }
            })
            ->run();
    }
    SECTION("Nested list marked to be copied but it was changed to int locally") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key1": [1, [2]]}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dictionary{realm, obj, col};
                dictionary.insert_collection("key1", CollectionType::List);
                auto nlist = dictionary.get_list("key1");
                nlist.add(Mixed{1});
                nlist.insert_collection(1, CollectionType::List);
                nlist = nlist.get_list(1);
                nlist.add(Mixed{2});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key1": 42}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // this insert operation triggers the list copy because the index becomes ambiguous
                auto nlist = obj.get_list_ptr<Mixed>({col, "key1", 1});
                nlist->add(Mixed{3});
                // Change list at 'key1' into integer so path above becomes invalid.
                auto ndict = obj.get_dictionary(col);
                ndict.insert("key1", Mixed{42});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // list must be equal to remote
                    // Result: {"_id": <id>, "any_mixed": {{"key1": [1, [2]]}}}
                    auto nlist = obj.get_list_ptr<Mixed>({col, "key1"});
                    REQUIRE(nlist->size() == 2);
                    REQUIRE(nlist->get_any(0).get_int() == 1);
                    nlist = nlist->get_list(1);
                    REQUIRE(nlist->size() == 1);
                    REQUIRE(nlist->get_any(0).get_int() == 2);
                }
                else {
                    // list changed into integer
                    // Result: {"_id": <id>, "any_mixed": {{"key1": 42}}}
                    auto ndict = obj.get_dictionary(col);
                    REQUIRE(ndict.size() == 1);
                    REQUIRE(ndict.get("key1").get_int() == 42);
                }
            })
            ->run();
    }

    // Test clearing nested collections and collections in mixed.

    SECTION("Clear dictionary changed into primitive remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key": 42}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{realm, obj, col};
                dict.insert("key", 42);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key": "some value"}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                REQUIRE(dict.size() == 1);
                dict.remove_all();
                dict.insert("key", "some value");
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": "value"}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // Change type from dictionary to string
                obj.set_any(col, "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": "value"}
                    REQUIRE(obj.get_any(col).get_string() == "value");
                }
                else {
                    // Clear changes the type back into dictionary.
                    // Result: {"_id": <id>, "any_mixed": {{"key": "some value"}}}
                    object_store::Dictionary dict{local_realm, obj, col};
                    REQUIRE(dict.size() == 1);
                    REQUIRE(dict.get_any("key").get_string() == "some value");
                }
            })
            ->run();
    }
    SECTION("Clear list changed into primitive remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": [1]}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.add(Mixed{1});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": [2]}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 1);
                list.delete_all();
                list.add(Mixed{2});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": "value"}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                // Change type from list to string
                obj.set_any(col, "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": "value"}
                    REQUIRE(obj.get_any(col).get_string() == "value");
                }
                else {
                    // Clear changes the type back into list.
                    // Result: {"_id": <id>, "any_mixed": [2]}
                    List list{local_realm, obj, col};
                    REQUIRE(list.size() == 1);
                    REQUIRE(list.get_any(0).get_int() == 2);
                }
            })
            ->run();
    }
    SECTION("Clear list within dictionary") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key": [42]}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{realm, obj, col};
                dict.insert_collection("key", CollectionType::List);
                auto nlist = dict.get_list("key");
                nlist.add(Mixed{42});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key": ["value"]}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key"});
                REQUIRE(nlist->size() == 1);
                nlist->clear();
                nlist->add(Mixed{"value"});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {{"key": [42]}}}
                    REQUIRE(dict.size() == 1);
                    auto nlist = dict.get_list("key");
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 42);
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": {{"key": ["value"]}}}
                    REQUIRE(dict.size() == 1);
                    auto nlist = dict.get_list("key");
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_string() == "value");
                }
            })
            ->run();
    }
    SECTION("Clear list within dictionary: list removed remotely") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {{"key": [42]}}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
                object_store::Dictionary dict{realm, obj, col};
                dict.insert_collection("key", CollectionType::List);
                auto nlist = dict.get_list("key");
                nlist.add(Mixed{42});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key": [1]}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                auto nlist = obj.get_list_ptr<Mixed>({col, "key"});
                REQUIRE(nlist->size() == 1);
                nlist->clear();
                nlist->add(Mixed{1});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{remote_realm, obj, col};
                REQUIRE(dict.size() == 1);
                // Remove list at 'key'
                dict.erase("key");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {}}
                    REQUIRE(dict.size() == 0);
                }
                else {
                    // List is added back into dictionary.
                    // Result: {"_id": <id>, "any_mixed": {{"key": [1]}}}
                    REQUIRE(dict.size() == 1);
                    auto nlist = dict.get_list("key");
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 1);
                }
            })
            ->run();
    }
    SECTION("Clear list within list triggers list copy") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": [1, [2]]}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::List);
                List list{realm, obj, col};
                list.add(Mixed{1});
                list.insert_collection(1, CollectionType::List);
                auto nlist = list.get_list(1);
                nlist.add(Mixed{2});
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": [1, [3]]}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);
                // this clear operation triggers the list copy because the index becomes ambiguous
                auto nlist = list.get_list(1);
                nlist.delete_all();
                nlist.add(Mixed{3});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": [42, [2]]}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{remote_realm, obj, col};
                REQUIRE(list.size() == 2);
                list.set_any(0, Mixed{42});
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                List list{local_realm, obj, col};
                REQUIRE(list.size() == 2);

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": [42, [2]]}
                    REQUIRE(list.get_any(0).get_int() == 42);
                    auto nlist = list.get_list(1);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": [1, [3]]}
                    REQUIRE(list.get_any(0).get_int() == 1);
                    auto nlist = list.get_list(1);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 3);
                }
            })
            ->run();
    }
    SECTION("Clear nested list added locally") {
        ObjectId pk_val = ObjectId::gen();
        SyncTestFile config2(oas.app()->current_user(), "default");
        config2.schema = config.schema;
        auto test_reset = reset_utils::make_fake_local_client_reset(config, config2);
        test_reset
            ->setup([&](SharedRealm realm) {
                // Baseline: {"_id": <id>, "any_mixed": {}}
                auto table = get_table(*realm, "TopLevel");
                auto obj = table->create_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                obj.set_collection(col, CollectionType::Dictionary);
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                // Local client: {"_id": <id>, "any_mixed": {{"key": [42, [2]]}}}
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};
                dict.insert_collection("key", CollectionType::List);
                auto nlist = dict.get_list("key");
                nlist.add(Mixed{42});
                nlist.insert_collection(1, CollectionType::List);
                nlist = nlist.get_list(1);
                nlist.add(Mixed{1});
                nlist.delete_all();
                nlist.add(Mixed{2});
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                // Remote client: {"_id": <id>, "any_mixed": {{"key2": "value"}}}
                advance_and_notify(*remote_realm);
                TableRef table = get_table(*remote_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{remote_realm, obj, col};
                REQUIRE(dict.size() == 0);
                dict.insert("key2", "value");
            })
            ->on_post_reset([&](SharedRealm local_realm) {
                advance_and_notify(*local_realm);
                TableRef table = get_table(*local_realm, "TopLevel");
                REQUIRE(table->size() == 1);
                auto obj = table->get_object_with_primary_key(pk_val);
                auto col = table->get_column_key("any_mixed");
                object_store::Dictionary dict{local_realm, obj, col};

                if (test_mode == ClientResyncMode::DiscardLocal) {
                    // Result: {"_id": <id>, "any_mixed": {{"key2": "value"}}}
                    REQUIRE(dict.size() == 1);
                    REQUIRE(dict.get_any("key2").get_string() == "value");
                }
                else {
                    // Result: {"_id": <id>, "any_mixed": {{"key": [42, [2]]}, {"key2": "value"}}}
                    REQUIRE(dict.size() == 2);
                    auto nlist = dict.get_list("key");
                    REQUIRE(nlist.size() == 2);
                    REQUIRE(nlist.get_any(0).get_int() == 42);
                    nlist = nlist.get_list(1);
                    REQUIRE(nlist.size() == 1);
                    REQUIRE(nlist.get_any(0).get_int() == 2);
                    REQUIRE(dict.get_any("key2").get_string() == "value");
                }
            })
            ->run();
    }
}
