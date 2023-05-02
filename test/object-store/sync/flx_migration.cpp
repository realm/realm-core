#include <sync/flx_sync_harness.hpp>
#include <sync/sync_test_utils.hpp>
#include <util/baas_admin_api.hpp>
#include <util/crypt_key.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
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


TEST_CASE("Test server migration and rollback", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition1 = "migration-test";
    const std::string partition2 = "another-value";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config1(session.app(), partition1, server_app_config.schema);
    SyncTestFile config2(session.app(), partition2, server_app_config.schema);

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
            mut_subs.insert_or_assign(
                "flx_migrated_Objects_2",
                Query(flx_table).equal(flx_table->get_column_key("realm_id"), StringData{partition2}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            REQUIRE(!wait_for_upload(*flx_realm));
            REQUIRE(!wait_for_download(*flx_realm));
            wait_for_advance(*flx_realm);

            check_data(flx_realm, true, true);
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
                logger_ptr->error("Server rolled back - connect as FLX received error: %1", err.reason());
                error_promise.get_promise().emplace_value(std::move(err));
            };
        auto flx_realm = Realm::get_shared_realm(flx_config);
        auto err = wait_for_future(std::move(err_future), std::chrono::seconds(30)).get();
        REQUIRE(err.get_system_error() == make_error_code(sync::ProtocolError::switch_to_pbs));
        REQUIRE(err.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
    }

    {
        SyncTestFile pbs_config(session.app(), partition1, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        REQUIRE(!wait_for_upload(*pbs_realm));
        REQUIRE(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, true, false);
    }
    {
        SyncTestFile pbs_config(session.app(), partition2, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        REQUIRE(!wait_for_upload(*pbs_realm));
        REQUIRE(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, false, true);
    }
}

TEST_CASE("Test client migration and rollback", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

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

TEST_CASE("Test client migration and rollback with recovery", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;

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

    //  Migrate back to FLX - and keep the realm session open
    trigger_server_migration(session.app_session(), MigrateToFLX, logger_ptr);

    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

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

    // Roll back to PBS once again - and keep the realm session open
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    REQUIRE(!wait_for_upload(*outer_realm));
    REQUIRE(!wait_for_download(*outer_realm));

    {
        auto table = outer_realm->read_group().get_table("class_Object");
        REQUIRE(table->size() == 7);
    }
}

TEST_CASE("An interrupted migration or rollback can recover on the next session", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

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

    auto error_event_hook = [&config](sync::ProtocolError error, int& error_count) {
        return [&config, &error_count, error](std::weak_ptr<SyncSession> weak_session,
                                              const SyncClientHookData& data) mutable {
            if (data.event != SyncClientHookEvent::ErrorMessageReceived) {
                return SyncClientHookAction::NoAction;
            }
            auto session = weak_session.lock();
            REQUIRE(session);

            if (session->path() != config.path) {
                return SyncClientHookAction::NoAction;
            }

            auto error_code = sync::ProtocolError(data.error_info->raw_error_code);

            if (error_code == sync::ProtocolError::initial_sync_not_completed) {
                return SyncClientHookAction::NoAction;
            }

            REQUIRE(error_code == error);
            ++error_count;
            return SyncClientHookAction::NoAction;
        };
    };

    // Session is interrupted before the migration is completed.
    {
        auto error_count = 0;
        config.sync_config->on_sync_client_event_hook =
            error_event_hook(sync::ProtocolError::migrate_to_flx, error_count);
        auto realm = Realm::get_shared_realm(config);

        timed_wait_for([&] {
            return util::File::exists(_impl::ClientResetOperation::get_fresh_path_for(config.path));
        });

        // Pause then resume the session. This triggers the server to send a new client reset request.
        realm->sync_session()->pause();
        realm->sync_session()->resume();

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);

        // Client reset is requested twice.
        REQUIRE(error_count == 2);
    }

    //  Roll back to PBS
    trigger_server_migration(session.app_session(), RollbackToPBS, logger_ptr);

    // Session is interrupted before the rollback is completed.
    {
        auto error_count = 0;
        config.sync_config->on_sync_client_event_hook =
            error_event_hook(sync::ProtocolError::revert_to_pbs, error_count);
        auto realm = Realm::get_shared_realm(config);

        timed_wait_for([&] {
            return util::File::exists(_impl::ClientResetOperation::get_fresh_path_for(config.path));
        });

        // Pause then resume the session. This triggers the server to send a new client reset request.
        realm->sync_session()->pause();
        realm->sync_session()->resume();

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);

        // Client reset is requested twice.
        REQUIRE(error_count == 2);
    }
}

TEST_CASE("Update to native FLX after migration", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

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
                logger_ptr->error("Server rolled back - connect as FLX received error: %1", err.reason());
                error_promise.get_promise().emplace_value(std::move(err));
            };
        auto flx_realm = Realm::get_shared_realm(flx_config);
        auto err = wait_for_future(std::move(err_future), std::chrono::seconds(30)).get();
        REQUIRE(err.get_system_error() == make_error_code(sync::ProtocolError::switch_to_pbs));
        REQUIRE(err.server_requests_action == sync::ProtocolErrorInfo::Action::ApplicationBug);
    }
}

TEST_CASE("New table is synced after migration", "[flx][migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition = "migration-test";
    const Schema mig_schema{
        ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                {"string_field", PropertyType::String | PropertyType::Nullable},
                                {"realm_id", PropertyType::String | PropertyType::Nullable}}),
    };
    auto server_app_config = minimal_app_config(base_url, "server_migrate_rollback", mig_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), partition, server_app_config.schema);
    config.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;

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
        const Schema schema{
            ObjectSchema("Object", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                    {"string_field", PropertyType::String | PropertyType::Nullable},
                                    {"realm_id", PropertyType::String | PropertyType::Nullable}}),
            ObjectSchema("Object2", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                     {"realm_id", PropertyType::String | PropertyType::Nullable}}),
        };
        SyncTestFile flx_config(session.app()->current_user(), schema, SyncConfig::FLXSyncEnabled{});

        auto flx_realm = Realm::get_shared_realm(flx_config);

        // Create a subscription for the new table.
        auto table = flx_realm->read_group().get_table("class_Object2");
        auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
        mut_subs.insert_or_assign(Query(table));
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

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
