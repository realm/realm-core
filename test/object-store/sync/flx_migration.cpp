////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#include <util/crypt_key.hpp>
#include <util/sync/baas_admin_api.hpp>
#include <util/sync/flx_sync_harness.hpp>
#include <util/sync/sync_test_utils.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/thread_safe_reference.hpp>
#include <realm/object-store/sync/async_open_task.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/sync/protocol.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset_operation.hpp>

#include <realm/util/future.hpp>

#include <catch2/catch_all.hpp>

#include <chrono>

#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

using namespace realm;

enum MigrationMode { MigrateToFLX, RollbackToPBS };

static void trigger_server_migration(const AppSession& app_session, MigrationMode switch_mode,
                                     const std::shared_ptr<util::Logger>& logger)
{
    auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);

    REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
    app_session.admin_api.migrate_to_flx(app_session.server_app_id, baas_sync_service.id,
                                         switch_mode == MigrateToFLX);

    // While the server migration is in progress, the server cannot be used - wait until the migration
    // is complete. migrated with be populated with the 'isMigrated' value from the complete response
    AdminAPISession::MigrationStatus status;
    std::string last_status;
    std::string op_stg = [switch_mode] {
        if (switch_mode == MigrateToFLX)
            return "PBS->FLX Server migration";
        else
            return "FLX->PBS Server rollback";
    }();
    const int duration = 600; // 10 minutes, for now, since it sometimes takes longer than 300 seconds
    try {
        timed_sleeping_wait_for(
            [&] {
                status = app_session.admin_api.get_migration_status(app_session.server_app_id);
                if (logger && last_status != status.statusMessage) {
                    last_status = status.statusMessage;
                    logger->debug("%1 status: %2", op_stg, last_status);
                }
                return status.complete;
            },
            // Query the migration status every 0.5 seconds for up to 90 seconds
            std::chrono::seconds(duration), std::chrono::milliseconds(500));
    }
    catch (const std::runtime_error&) {
        if (logger)
            logger->debug("%1 timed out after %2 seconds", op_stg, duration);
        REQUIRE(false);
    }
    if (logger) {
        logger->debug("%1 complete", op_stg);
    }
    REQUIRE((switch_mode == MigrateToFLX) == status.isMigrated);
}

// Add a set of count number of Object objects to the realm
static std::vector<ObjectId> fill_test_data(SyncTestFile& config, std::optional<std::string> partition = std::nullopt,
                                            int start = 1, int count = 5)
{
    std::vector<ObjectId> ret;
    auto realm = Realm::get_shared_realm(config);
    realm->begin_transaction();
    CppContext c(realm);
    // Add some objects with the provided partition value
    for (int i = 0; i < count; i++, ++start) {
        auto id = ObjectId::gen();
        auto obj = Object::create(
            c, realm, "Object",
            std::any(AnyDict{{"_id", std::any(id)}, {"string_field", util::format("value-%1", start)}}));

        if (partition) {
            obj.set_column_value("realm_id", *partition);
        }
        ret.push_back(id);
    }
    realm->commit_transaction();
    return ret;
}


TEST_CASE("Test server migration and rollback", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition1 = "migration-test";
    const std::string partition2 = "another-value";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config("server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config1(session.app()->current_user(), partition1, server_app_config.schema);
    SyncTestFile config2(session.app()->current_user(), partition2, server_app_config.schema);

    // Fill some objects
    auto objects1 = fill_test_data(config1, partition1);    // 5 objects starting at 1
    auto objects2 = fill_test_data(config2, partition2, 6); // 5 objects starting at 6

    auto check_data = [&](SharedRealm& realm, bool check_set1, bool check_set2) {
        auto table = realm->read_group().get_table("class_Object");
        auto partition_col = table->get_column_key("realm_id");
        auto string_col = table->get_column_key("string_field");

        size_t table_size = [check_set1, check_set2] {
            if (check_set1 && check_set2)
                return 10;
            if (check_set1 || check_set2)
                return 5;
            return 0;
        }();

        REQUIRE(table->size() == table_size);
        REQUIRE(bool(table->find_first(partition_col, StringData(partition1))) == check_set1);
        REQUIRE(bool(table->find_first(string_col, StringData("value-5"))) == check_set1);
        REQUIRE(bool(table->find_first(partition_col, StringData(partition2))) == check_set2);
        REQUIRE(bool(table->find_first(string_col, StringData("value-6"))) == check_set2);
    };

    // Wait for the two partition sets to upload
    {
        auto realm1 = Realm::get_shared_realm(config1);

        REQUIRE(!wait_for_upload(*realm1));
        REQUIRE(!wait_for_download(*realm1));

        check_data(realm1, true, false);

        auto realm2 = Realm::get_shared_realm(config2);

        REQUIRE(!wait_for_upload(*realm2));
        REQUIRE(!wait_for_download(*realm2));

        check_data(realm2, false, true);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});

        auto flx_realm = Realm::get_shared_realm(flx_config);
        {
            auto subs = flx_realm->get_latest_subscription_set();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            REQUIRE(!wait_for_upload(*flx_realm));
            REQUIRE(!wait_for_download(*flx_realm));

            check_data(flx_realm, false, false);
        }

        {
            auto flx_table = flx_realm->read_group().get_table("class_Object");
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(
                "flx_migrated_Objects_1",
                Query(flx_table).equal(flx_table->get_column_key("realm_id"), StringData{partition1}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            REQUIRE(!wait_for_upload(*flx_realm));
            REQUIRE(!wait_for_download(*flx_realm));
            wait_for_advance(*flx_realm);

            check_data(flx_realm, true, false);
        }

        {
            auto flx_table = flx_realm->read_group().get_table("class_Object");
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(
                "flx_migrated_Objects_2",
                Query(flx_table).equal(flx_table->get_column_key("realm_id"), StringData{partition2}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            REQUIRE(!wait_for_upload(*flx_realm));
            REQUIRE(!wait_for_download(*flx_realm));
            wait_for_advance(*flx_realm);

            check_data(flx_realm, false, true);
        }
    }

    // Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    // Try to connect as FLX
    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});
        auto [err_promise, err_future] = util::make_promise_future<SyncError>();
        util::CopyablePromiseHolder promise(std::move(err_promise));
        flx_config.sync_config->error_handler =
            [&logger_ptr, error_promise = std::move(promise)](std::shared_ptr<SyncSession>, SyncError err) mutable {
                // This situation should return the switch_to_pbs error
                logger_ptr->error("Server rolled back - connect as FLX received error: %1", err.status);
                error_promise.get_promise().emplace_value(std::move(err));
            };
        auto flx_realm = Realm::get_shared_realm(flx_config);
        auto err = wait_for_future(std::move(err_future), std::chrono::seconds(30)).get();
        REQUIRE(err.status == ErrorCodes::WrongSyncType);
        REQUIRE(err.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
    }

    {
        SyncTestFile pbs_config(session.app()->current_user(), partition1, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        REQUIRE(!wait_for_upload(*pbs_realm));
        REQUIRE(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, true, false);
    }
    {
        SyncTestFile pbs_config(session.app()->current_user(), partition2, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        REQUIRE(!wait_for_upload(*pbs_realm));
        REQUIRE(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, false, true);
    }
}

TEST_CASE("Test client migration and rollback", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config("server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.schema_version = 0;

    // Fill some objects
    auto objects = fill_test_data(config, partition); // 5 objects starting at 1

    // Wait to upload the data
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 5);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 5);
    }

    // Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 5);
    }
}

TEST_CASE("Test client migration and rollback with recovery", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();
    enum TestState { idle, wait_for_merge, merge_complete, rollback_complete };
    TestingStateMachine<TestState> test_state(TestState::idle);

    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config("server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;
    config.schema_version = 0;
    config.sync_config->on_sync_client_event_hook = [&](std::weak_ptr<SyncSession>, const SyncClientHookData& data) {
        test_state.transition_with([data](TestState cur_state) -> std::optional<TestState> {
            if (data.event == SyncClientHookEvent::ClientResetMergeComplete &&
                cur_state == TestState::wait_for_merge) {
                return TestState::merge_complete;
            }
            return std::nullopt;
        });
        if (test_state.get() == TestState::merge_complete) {
            // Wait for the FLX->PBS rollback to complete before continuing
            test_state.wait_for(TestState::rollback_complete, std::chrono::seconds(25));
        }
        return SyncClientHookAction::NoAction;
    };

    // Fill some objects
    auto objects = fill_test_data(config); // 5 objects starting at 1 with no partition value set
    // Primary key of the object to recover
    auto obj_id = ObjectId::gen();

    // Keep this realm around for after the revert to PBS
    auto outer_realm = Realm::get_shared_realm(config);
    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

    // Wait to upload the data
    {
        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 5);

        // Pause the sync session and make a change.
        // This will be recovered when it is resumed after the migration.
        outer_realm->sync_session()->pause();
        outer_realm->begin_transaction();
        outer_realm->read_group()
            .get_table("class_Object")
            ->create_object_with_primary_key(obj_id)
            .set("string_field", "partition-set-during-sync-upload");
        outer_realm->commit_transaction();
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    // Resume the session and verify the additional object was uploaded after the migration
    outer_realm->sync_session()->resume();
    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

    {
        auto sync_session = outer_realm->sync_session();
        REQUIRE(sync_session);
        auto sub_store = sync_session->get_flx_subscription_store();
        REQUIRE(sub_store);
        auto active_subs = sub_store->get_active();
        REQUIRE(active_subs.size() == 1);
        REQUIRE(active_subs.find("flx_migrated_Object"));

        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 6);

        auto object_table = outer_realm->read_group().get_table("class_Object");
        auto pending_object = object_table->get_object_with_primary_key(obj_id);
        REQUIRE(pending_object.get<String>("string_field") == "partition-set-during-sync-upload");
    }

    // Pause the sync session so a pending subscription and object can be created
    // before processing the rollback
    outer_realm->sync_session()->pause();
    util::Future<sync::SubscriptionSet::State> new_subs_future = [&] {
        auto sub_store = outer_realm->sync_session()->get_flx_subscription_store();
        auto mut_subs = sub_store->get_active().make_mutable_copy();

        auto object_table = outer_realm->read_group().get_table("class_Object");
        auto string_col_key = object_table->get_column_key("string_field");
        mut_subs.insert_or_assign("dummy_subs", Query(object_table).equal(string_col_key, StringData{"some-value"}));
        auto new_subs = mut_subs.commit();
        return new_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete);
    }();

    // Add a local object while the session is paused. This will be recovered when connecting after the rollback.
    {
        outer_realm->begin_transaction();
        outer_realm->read_group()
            .get_table("class_Object")
            ->create_object_with_primary_key(ObjectId::gen())
            .set("string_field", "partition-set-by-pbs");
        outer_realm->commit_transaction();
    }

    // Wait for the object to be written to Atlas/MongoDB before rollback, otherwise it may be lost
    reset_utils::wait_for_object_to_persist_to_atlas(session.app()->current_user(), session.app_session(), "Object",
                                                     {{"_id", obj_id}});

    //  Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    // Connect after rolling back to PBS
    outer_realm->sync_session()->resume();
    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

    {
        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 7);

        // Verify the internal sync session subscription store has been cleared
        auto sync_session = outer_realm->sync_session();
        REQUIRE(sync_session);
        auto sub_store = SyncSession::OnlyForTesting::get_subscription_store_base(*sync_session);
        REQUIRE(sub_store);
        auto active_subs = sub_store->get_latest();
        REQUIRE(active_subs.size() == 0);
        REQUIRE(active_subs.version() == 0);

        auto result = wait_for_future(std::move(new_subs_future)).get_no_throw();
        REALM_ASSERT(result.is_ok());
        REALM_ASSERT(result.get_value() == sync::SubscriptionSet::State::Superseded);
    }

    test_state.transition_to(TestState::wait_for_merge);

    //  Migrate back to FLX - and keep the realm session open
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    // Cancel any connect waits (since sync session is still active) and try to connect now
    outer_realm->sync_session()->handle_reconnect();

    // wait for the fresh realm to download and merge with the current local realm
    test_state.wait_for(TestState::merge_complete, std::chrono::seconds(180));

    // Verify data has been sync'ed and there is only 1 subscription for the Object table
    {
        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 7);
        auto sync_session = outer_realm->sync_session();
        REQUIRE(sync_session);
        auto sub_store = sync_session->get_flx_subscription_store();
        REQUIRE(sub_store);
        auto active_subs = sub_store->get_active();
        REQUIRE(active_subs.size() == 1);
        REQUIRE(active_subs.find("flx_migrated_Object"));
    }

    // Roll back to PBS once again before the client reset is complete and keep the realm session open
    // NOTE: the realm session is blocked in the hook callback until the rollback is complete
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    // Release the realm session; will reconnect and perform the rollback to PBS client reset
    test_state.transition_to(TestState::rollback_complete);

    // Cancel any connect waits (since sync session is still active) and try to connect now
    outer_realm->sync_session()->handle_reconnect();

    // During the rollback client reset, the previous migrate to flx client reset operation is still
    // tracked, but will be removed since the new rollback server requests action is incompatible.
    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

    {
        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 7);
    }
}

TEST_CASE("An interrupted migration or rollback can recover on the next session",
          "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config("server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.schema_version = 0;

    // Fill some objects
    auto objects = fill_test_data(config, partition);

    // Wait to upload the data
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);
    enum class State { Initial, FirstError, InClientReset, Resumed, SecondError };
    TestingStateMachine<State> state(State::Initial);

    auto error_event_hook = [&config, &state](sync::ProtocolError error) {
        return [&config, &state, error](std::weak_ptr<SyncSession> weak_session,
                                        const SyncClientHookData& data) mutable {
            auto session = weak_session.lock();
            REQUIRE(session);

            if (data.event == SyncClientHookEvent::BindMessageSent &&
                session->path() == _impl::client_reset::get_fresh_path_for(config.path)) {
                bool wait_for_resume = false;
                state.transition_with([&](State cur_state) -> std::optional<State> {
                    if (cur_state == State::FirstError) {
                        wait_for_resume = true;
                        return State::InClientReset;
                    }
                    return std::nullopt;
                });
                if (wait_for_resume) {
                    state.wait_for(State::Resumed);
                }
            }

            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            if (session->path() != config.path) {
                return SyncClientHookAction::NoAction;
            }

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);
            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            REQUIRE(error_code == error);
            state.transition_with([&](State cur_state) -> std::optional<State> {
                switch (cur_state) {
                    case State::Initial:
                        return State::FirstError;
                    case State::Resumed:
                        return State::SecondError;
                    default:
                        FAIL(util::format("Unxpected state %1", static_cast<int>(cur_state)));
                }
                return std::nullopt;
            });
            return SyncClientHookAction::NoAction;
        };
    };

    // Session is interrupted before the migration is completed.
    {
        config.sync_config->on_sync_client_event_hook = error_event_hook(sync::ProtocolError::migrate_to_flx);
        auto realm = Realm::get_shared_realm(config);

        state.wait_for(State::InClientReset);

        // Pause then resume the session. This triggers the server to send a new client reset request.
        realm->sync_session()->pause();
        realm->sync_session()->resume();
        state.transition_with([&](State cur_state) {
            REQUIRE(cur_state == State::InClientReset);
            return State::Resumed;
        });

        state.wait_for(State::SecondError);
        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    state.transition_with([](State) {
        return State::Initial;
    });
    //  Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);


    // Session is interrupted before the rollback is completed.
    {
        config.sync_config->on_sync_client_event_hook = error_event_hook(sync::ProtocolError::revert_to_pbs);
        auto realm = Realm::get_shared_realm(config);

        state.wait_for(State::InClientReset);

        // Pause then resume the session. This triggers the server to send a new client reset request.
        realm->sync_session()->pause();
        realm->sync_session()->resume();
        state.transition_with([&](State cur_state) {
            REQUIRE(cur_state == State::InClientReset);
            return State::Resumed;
        });

        state.wait_for(State::SecondError);
        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }
}

TEST_CASE("Update to native FLX after migration", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config("server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.schema_version = 0;

    // Fill some objects
    auto objects = fill_test_data(config, partition); // 5 objects starting at 1

    // Wait to upload the data
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);

        // Close the sync session and make a change. This will be recovered by the migration.
        realm->sync_session()->force_close();
        realm->begin_transaction();
        realm->read_group()
            .get_table("class_Object")
            ->create_object_with_primary_key(ObjectId::gen())
            .set("string_field", "flx_migration_object");
        realm->commit_transaction();
    }

    // Update to native FLX
    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});
        flx_config.path = config.path;

        auto realm = Realm::get_shared_realm(flx_config);

        realm->begin_transaction();
        realm->read_group()
            .get_table("class_Object")
            ->create_object_with_primary_key(ObjectId::gen())
            .set("realm_id", partition)
            .set("string_field", "flx_native_object");
        ;
        realm->commit_transaction();

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 7);
    }

    // Open a new realm and check all data is sync'ed.
    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});

        auto flx_realm = Realm::get_shared_realm(flx_config);

        auto flx_table = flx_realm->read_group().get_table("class_Object");
        auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
        auto partition_col_key = flx_table->get_column_key("realm_id");
        mut_subs.insert_or_assign("flx_migrated_Object",
                                  Query(flx_table).equal(partition_col_key, StringData{partition}));
        auto subs = std::move(mut_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        flx_realm->refresh();
        auto table = flx_realm->read_group().get_table("class_Object");
        CHECK(table->size() == 7);
    }

    //  Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    // Connect again as native FLX: server replies with SwitchToPBS
    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});
        flx_config.path = config.path;

        auto [err_promise, err_future] = util::make_promise_future<SyncError>();
        util::CopyablePromiseHolder promise(std::move(err_promise));
        flx_config.sync_config->error_handler =
            [&logger_ptr, error_promise = std::move(promise)](std::shared_ptr<SyncSession>, SyncError err) mutable {
                // This situation should return the switch_to_pbs error
                logger_ptr->error("Server rolled back - connect as FLX received error: %1", err.status);
                error_promise.get_promise().emplace_value(std::move(err));
            };
        auto flx_realm = Realm::get_shared_realm(flx_config);
        auto err = wait_for_future(std::move(err_future), std::chrono::seconds(30)).get();
        REQUIRE(err.status == ErrorCodes::WrongSyncType);
        REQUIRE(err.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
    }
}

TEST_CASE("New table is synced after migration", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition = "migration-test";
    const auto obj1_schema = ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                                     {"string_field", PropertyType::String | PropertyType::Nullable},
                                                     {"realm_id", PropertyType::String | PropertyType::Nullable}});
    const auto obj2_schema = ObjectSchema("Object2", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                                      {"realm_id", PropertyType::String | PropertyType::Nullable}});
    const Schema mig_schema{obj1_schema};
    const Schema two_obj_schema{obj1_schema, obj2_schema};
    auto server_app_config = minimal_app_config("server_migrate_rollback", two_obj_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app()->current_user(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
    config.schema_version = 0;

    // Fill some objects
    auto objects = fill_test_data(config, partition); // 5 objects starting at 1

    // Wait to upload the data
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    // Open a new realm with an additional table.
    {
        SyncTestFile flx_config(session.app()->current_user(), two_obj_schema, SyncConfig::FLXSyncEnabled{});

        auto flx_realm = Realm::get_shared_realm(flx_config);

        // Create a subscription for the new table.
        auto table = flx_realm->read_group().get_table("class_Object2");
        auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
        mut_subs.insert_or_assign(Query(table).equal(table->get_column_key("realm_id"), StringData{partition}));

        auto subs = std::move(mut_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        wait_for_upload(*flx_realm);

        // Create one object of the new table type.
        flx_realm->begin_transaction();
        flx_realm->read_group()
            .get_table("class_Object2")
            ->create_object_with_primary_key(ObjectId::gen())
            .set("realm_id", partition);
        flx_realm->commit_transaction();

        wait_for_upload(*flx_realm);
        wait_for_download(*flx_realm);
    }

    // Open the migrated realm and sync the new table and data.
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
        auto table2 = realm->read_group().get_table("class_Object2");
        CHECK(table2->size() == 1);
        auto sync_session = realm->sync_session();
        REQUIRE(sync_session);
        auto sub_store = sync_session->get_flx_subscription_store();
        REQUIRE(sub_store);
        auto active_subs = sub_store->get_active();
        REQUIRE(active_subs.size() == 2);
        REQUIRE(active_subs.find("flx_migrated_Object2"));
    }
}

// There is a sequence of events where we tried to open a frozen Realm with new
// object types in the schema and this fails schema validation causing client reset
// to fail.
//   - Add a new class to the schema, but use async open to initiate
//     sync without any schema
//   - Have the server send a client reset.
//   - The client tries to populate the notify_before callback with a frozen Realm using
//     the schema with the new class, but the class is not stored on disk yet.
// This hits the update_schema() check that makes sure that the frozen Realm's schema is
// a subset of the one found on disk. Since it is not, a schema exception is thrown
// which is eventually forwarded to the sync error handler and client reset fails.
TEST_CASE("Async open + client reset", "[sync][flx][flx migration][baas]") {
    auto logger_ptr = util::Logger::get_default_logger();

    const std::string partition = "async-open-migration-test";
    ObjectSchema shared_object("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                          {"string_field", PropertyType::String | PropertyType::Nullable},
                                          {"realm_id", PropertyType::String | PropertyType::Nullable}});
    const Schema mig_schema{shared_object};
    size_t num_before_reset_notifications = 0;
    size_t num_after_reset_notifications = 0;
    auto server_app_config = minimal_app_config("async_open_during_migration", mig_schema);
    server_app_config.dev_mode_enabled = true;
    std::optional<SyncTestFile> config; // destruct this after the sessions are torn down
    TestAppSession session(create_app(server_app_config));
    config.emplace(session.app()->current_user(), partition, server_app_config.schema);
    config->sync_config->client_resync_mode = ClientResyncMode::Recover;
    config->sync_config->notify_before_client_reset = [&](SharedRealm before) {
        logger_ptr->debug("notify_before_client_reset");
        REQUIRE(before);
        REQUIRE(before->is_frozen());
        auto table = before->read_group().get_table("class_Object");
        CHECK(table);
        ++num_before_reset_notifications;
    };
    config->sync_config->notify_after_client_reset = [&](SharedRealm before, ThreadSafeReference after_ref,
                                                         bool did_recover) {
        logger_ptr->debug("notify_after_client_reset");
        CHECK(did_recover);
        REQUIRE(before);
        auto table_before = before->read_group().get_table("class_Object");
        CHECK(table_before);
        SharedRealm after = Realm::get_shared_realm(std::move(after_ref), util::Scheduler::make_dummy());
        REQUIRE(after);
        auto table_after = after->read_group().get_table("class_Object");
        REQUIRE(table_after);
        ++num_after_reset_notifications;
    };
    config->schema_version = 0;

    ObjectSchema locally_added("LocallyAdded", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                                {"realm_id", PropertyType::String | PropertyType::Nullable}});

    SECTION("no initial state") {
        // Migrate to FLX
        trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);
        shared_object.persisted_properties.push_back({"oid_field", PropertyType::ObjectId | PropertyType::Nullable});
        config->schema = {shared_object, locally_added};

        async_open_realm(*config, [&](ThreadSafeReference&& ref, std::exception_ptr error) {
            REQUIRE(ref);
            REQUIRE_FALSE(error);

            auto realm = Realm::get_shared_realm(std::move(ref));

            auto table = realm->read_group().get_table("class_Object");
            REQUIRE(table->size() == 0);
            REQUIRE(num_before_reset_notifications == 1);
            REQUIRE(num_after_reset_notifications == 1);

            auto locally_added_table = realm->read_group().get_table("class_LocallyAdded");
            REQUIRE(locally_added_table);
            REQUIRE(locally_added_table->size() == 0);
        });
    }

    SECTION("initial state") {
        {
            config->schema = {shared_object};
            auto realm = Realm::get_shared_realm(*config);
            realm->begin_transaction();
            auto table = realm->read_group().get_table("class_Object");
            table->create_object_with_primary_key(ObjectId::gen());
            realm->commit_transaction();
            wait_for_upload(*realm);
        }
        trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);
        {
            shared_object.persisted_properties.push_back(
                {"oid_field", PropertyType::ObjectId | PropertyType::Nullable});
            config->schema = {shared_object, locally_added};

            async_open_realm(*config, [&](ThreadSafeReference&& ref, std::exception_ptr error) {
                REQUIRE(ref);
                REQUIRE_FALSE(error);

                auto realm = Realm::get_shared_realm(std::move(ref));

                auto table = realm->read_group().get_table("class_Object");
                REQUIRE(table->size() == 1);
                REQUIRE(num_before_reset_notifications == 1);
                REQUIRE(num_after_reset_notifications == 1);

                auto locally_added_table = realm->read_group().get_table("class_LocallyAdded");
                REQUIRE(locally_added_table);
                REQUIRE(locally_added_table->size() == 0);
            });
        }
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
