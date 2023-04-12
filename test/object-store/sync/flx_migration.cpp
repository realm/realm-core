#include <sync/flx_sync_harness.hpp>
#include <sync/sync_test_utils.hpp>
#include <util/baas_admin_api.hpp>
#include <util/crypt_key.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/migration_store.hpp>
#include <realm/util/future.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>


#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

using namespace realm;

static void trigger_server_migration(const AppSession& app_session, bool switch_to_flx,
                                     const std::shared_ptr<util::Logger>& logger)
{
    auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);

    REQUIRE(app_session.admin_api.is_sync_enabled(app_session.server_app_id));
    app_session.admin_api.migrate_to_flx(app_session.server_app_id, baas_sync_service.id, switch_to_flx);

    // While the server migration is in progress, the server cannot be used - wait until the migration
    // is complete. migrated with be populated with the 'isMigrated' value from the complete response
    AdminAPISession::MigrationStatus status;
    std::string last_status;
    std::string op_stg = [switch_to_flx] {
        if (switch_to_flx)
            return "PBS->FLX Server migration";
        else
            return "FLX->PBS Server rollback";
    }();
    const int duration = 300; // 5 minutes, for now, since it sometimes takes longet than 90 seconds
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
    REQUIRE(switch_to_flx == status.isMigrated);
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

const std::string migrated_partition("migrated_partition");
const std::string rql_string("subscription_rql_string");

static void check_not_migrated(const std::shared_ptr<sync::MigrationStore>& migration_store)
{
    auto sync_config = std::make_shared<SyncConfig>(nullptr, migrated_partition);
    REQUIRE_FALSE(sync_config->flx_sync_requested);

    REQUIRE_FALSE(migration_store->is_migrated());
    REQUIRE_FALSE(migration_store->is_migration_in_progress());
    REQUIRE_FALSE(migration_store->get_query_string());
    REQUIRE_FALSE(migration_store->get_migrated_partition());

    auto migrated_config = migration_store->convert_sync_config(sync_config);
    REQUIRE(sync_config == migrated_config);
}

static void check_migration_in_progress(const std::shared_ptr<sync::MigrationStore>& migration_store)
{
    auto sync_config = std::make_shared<SyncConfig>(nullptr, "some_other_partition");
    REQUIRE_FALSE(sync_config->flx_sync_requested);

    REQUIRE_FALSE(migration_store->is_migrated());
    REQUIRE(migration_store->is_migration_in_progress());
    REQUIRE(migration_store->get_query_string());
    REQUIRE(*migration_store->get_query_string() == rql_string);
    REQUIRE(migration_store->get_migrated_partition());
    REQUIRE(*migration_store->get_migrated_partition() == migrated_partition);

    // Verify conversion from PBS to FLX sync config - different partition value will not cause
    // LogicError until migration is complete
    REQUIRE_NOTHROW(migration_store->convert_sync_config(sync_config));
    auto migrated_config = migration_store->convert_sync_config(sync_config);
    REQUIRE(sync_config != migrated_config);
    REQUIRE(migrated_config->flx_sync_requested);
    // Verify no conversion from FLX to FLX sync config
    sync_config->flx_sync_requested = true;
    sync_config->partition_value = {};
    REQUIRE_NOTHROW(migration_store->convert_sync_config(sync_config));
    migrated_config = migration_store->convert_sync_config(sync_config);
    REQUIRE(sync_config == migrated_config);
    REQUIRE(migrated_config->flx_sync_requested);
}

static void check_migration_complete(const std::shared_ptr<sync::MigrationStore>& migration_store)
{
    auto sync_config = std::make_shared<SyncConfig>(nullptr, "some_other_parition");
    REQUIRE_FALSE(sync_config->flx_sync_requested);

    REQUIRE(migration_store->is_migrated());
    REQUIRE_FALSE(migration_store->is_migration_in_progress());
    REQUIRE(migration_store->get_query_string());
    REQUIRE(*migration_store->get_query_string() == rql_string);
    REQUIRE(migration_store->get_migrated_partition());
    REQUIRE(*migration_store->get_migrated_partition() == migrated_partition);

    // Verify logic error is thrown if partition value does not match migrated partition
    CHECK_THROWS_AS(migration_store->convert_sync_config(sync_config), LogicError);
    // Verify conversion from PBS to FLX sync config with matching partition values
    sync_config->partition_value = migrated_partition;
    REQUIRE_NOTHROW(migration_store->convert_sync_config(sync_config));
    auto migrated_config = migration_store->convert_sync_config(sync_config);
    CHECK(sync_config != migrated_config);
    REQUIRE(migrated_config->flx_sync_requested);
    // Verify no conversion from FLX to FLX sync config
    sync_config->flx_sync_requested = true;
    sync_config->partition_value = {};
    REQUIRE_NOTHROW(migration_store->convert_sync_config(sync_config));
    migrated_config = migration_store->convert_sync_config(sync_config);
    REQUIRE(sync_config == migrated_config);
    REQUIRE(migrated_config->flx_sync_requested);
}

static void check_subscription(const sync::SubscriptionSet& sub_set, const std::string& table_name,
                               const std::string& query_string)
{
    auto sub_name = std::string("flx_migrated_" + table_name);
    auto table_sub = sub_set.find(sub_name);
    REQUIRE(table_sub);
    REQUIRE(table_sub->query_string == query_string);
    REQUIRE(table_sub->object_class_name == table_name);
    REQUIRE(table_sub->name == sub_name);
}

TEST_CASE("Migration store", "[flx][migration]") {
    std::string file_path = util::make_temp_dir() + "/migration_store.realm";
    auto mig_db = DB::create(sync::make_client_replication(), file_path);
    auto migration_store = sync::MigrationStore::create(mig_db);

    SECTION("Migration store default", "[flx][migration]") {
        check_not_migrated(migration_store);
    }

    SECTION("Migration store complete and cancel", "[flx][migration]") {
        // Start the migration and check the state
        migration_store->migrate_to_flx(rql_string, migrated_partition);
        check_migration_in_progress(migration_store);

        // Call in progress again and check the state (can be called multiple times)
        migration_store->migrate_to_flx(rql_string, migrated_partition);
        check_migration_in_progress(migration_store);

        // Complete the migration and check the state
        migration_store->complete_migration();
        check_migration_complete(migration_store);

        // Cancel the migration and check the state
        migration_store->cancel_migration();
        check_not_migrated(migration_store);
    }

    SECTION("Migration store complete and cancel", "[flx][migration]") {
        check_not_migrated(migration_store);

        // Complete the migration and check the state - should be not migrated
        migration_store->complete_migration();
        check_not_migrated(migration_store);
    }

    SECTION("Migration store subscriptions", "[flx][migration]") {
        auto sub_store = sync::SubscriptionStore::create(mig_db, [](int64_t) {});
        auto orig_version = sub_store->get_latest().version();

        // Create some dummy tables
        {
            auto tr = mig_db->start_write();
            tr->add_table("class_Table1");
            tr->add_table("class_Table2");
            tr->commit();
        }

        // No subscriptions are created in the NotMigrated state
        migration_store->create_subscriptions(*sub_store);
        {
            auto subs = sub_store->get_latest();
            REQUIRE(subs.size() == 0);
            REQUIRE(subs.version() == orig_version);
        }

        // Start the migration and check the state
        migration_store->migrate_to_flx(rql_string, migrated_partition);
        check_migration_in_progress(migration_store);

        // No subscriptions are created in the InProgress state
        migration_store->create_subscriptions(*sub_store);
        {
            auto subs = sub_store->get_latest();
            REQUIRE(subs.size() == 0);
            REQUIRE(subs.version() == orig_version);
        }

        // Complete the migration and check the state
        migration_store->complete_migration();
        check_migration_complete(migration_store);

        auto query_string = migration_store->get_query_string();
        REQUIRE(query_string);

        // Create subscriptions for known tables once the migration store is in
        // Migrated state
        migration_store->create_subscriptions(*sub_store);
        {
            auto subs = sub_store->get_latest();
            REQUIRE(subs.size() == 2);
            REQUIRE(subs.version() > orig_version);
            check_subscription(subs, "Table1", *query_string);
            check_subscription(subs, "Table2", *query_string);
            orig_version = subs.version();
        }

        // Verify subscription version doesn't change if the tables haven't changed
        migration_store->create_subscriptions(*sub_store);
        {
            auto subs = sub_store->get_latest();
            REQUIRE(subs.size() == 2);
            REQUIRE(subs.version() == orig_version);
        }

        {
            // Create another table
            auto tr = mig_db->start_write();
            tr->add_table("class_Table3");
            tr->commit();
        }

        // Test direct call to create_subscriptions with different query string
        std::string query_string2("subscription_rql_string2");
        migration_store->create_subscriptions(*sub_store, query_string2);
        {
            auto subs = sub_store->get_latest();
            REQUIRE(subs.size() == 3);
            REQUIRE(subs.version() > orig_version);
            check_subscription(subs, "Table1", *query_string);
            check_subscription(subs, "Table2", *query_string);
            check_subscription(subs, "Table3", query_string2);
        }
    }
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
    trigger_server_migration(session.app_session(), true, logger_ptr);

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
    trigger_server_migration(session.app_session(), false, logger_ptr);

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

#ifdef REALM_SYNC_PROTOCOL_V8

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
        CHECK(table->size() == 5);
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), true, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }

    // Roll back to PBS
    trigger_server_migration(session.app_session(), false, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 5);
    }
}

TEST_CASE("Test client migration and rollback with recovery", "[flx][migration]") {
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
    config.sync_config->client_resync_mode = ClientResyncMode::Recover;

    // Fill some objects
    auto objects = fill_test_data(config); // 5 objects starting at 1 with no partition value set
    // Primary key of the object to recover
    auto obj_id = ObjectId::gen();

    // Wait to upload the data
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
            ->create_object_with_primary_key(obj_id)
            .set("string_field", "partition-set-during-sync-upload");
        realm->commit_transaction();
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), true, logger_ptr);

    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 6);
        realm->begin_transaction();
        auto pending_object = realm->read_group().get_table("class_Object")->get_object_with_primary_key(obj_id);
        REQUIRE(pending_object.get<String>("string_field") == "partition-set-during-sync-upload");
    }

    // Wait for the object to be written to Atlas/MongoDB before rollback, otherwise it may be lost
    reset_utils::wait_for_object_to_persist_to_atlas(session.app()->current_user(), session.app_session(), "Object",
                                                     {{"_id", obj_id}});

    //  Roll back to PBS
    trigger_server_migration(session.app_session(), false, logger_ptr);

    // Open up the realm without the sync client attached and make a change. This will be recovered by the rollback.
    {
        DBOptions options;
        options.encryption_key = test_util::crypt_key();
        auto realm = DB::create(sync::make_client_replication(), config.path, options);

        auto tr = realm->start_write();
        tr->get_table("class_Object")
            ->create_object_with_primary_key(ObjectId::gen())
            .set("string_field", "partition-set-by-pbs");
        tr->commit();
    }

    // Connect after rolling back to PBS
    {
        auto realm = Realm::get_shared_realm(config);

        REQUIRE(!wait_for_upload(*realm));
        REQUIRE(!wait_for_download(*realm));

        auto table = realm->read_group().get_table("class_Object");
        CHECK(table->size() == 7);
    }
}

TEST_CASE("Validate protocol v8 features", "[flx][migration]") {
    REQUIRE(sync::get_current_protocol_version() >= 8);
    REQUIRE("com.mongodb.realm-sync#" == sync::get_pbs_websocket_protocol_prefix());
    REQUIRE("com.mongodb.realm-query-sync#" == sync::get_flx_websocket_protocol_prefix());
}

#endif // REALM_SYNC_PROTOCOL_V8

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
