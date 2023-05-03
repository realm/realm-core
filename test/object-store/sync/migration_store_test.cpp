#include <realm/transaction.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/migration_store.hpp>

#include <catch2/catch_all.hpp>

#if REALM_ENABLE_SYNC && REALM_ENABLE_AUTH_TESTS

using namespace realm;

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

static void check_rollback_in_progress(const std::shared_ptr<sync::MigrationStore>& migration_store)
{
    auto sync_config = std::make_shared<SyncConfig>(nullptr, "some_other_partition");
    REQUIRE_FALSE(sync_config->flx_sync_requested);

    REQUIRE_FALSE(migration_store->is_migrated());
    REQUIRE_FALSE(migration_store->is_migration_in_progress());
    REQUIRE(migration_store->is_rollback_in_progress());

    //  Valid until rollback is completed.
    REQUIRE(migration_store->get_query_string());
    REQUIRE(*migration_store->get_query_string() == rql_string);
    REQUIRE(migration_store->get_migrated_partition());
    REQUIRE(*migration_store->get_migrated_partition() == migrated_partition);

    // Verify there is no conversion from PBS to FLX sync config.
    REQUIRE_NOTHROW(migration_store->convert_sync_config(sync_config));
    auto rollback_config = migration_store->convert_sync_config(sync_config);
    REQUIRE(sync_config == rollback_config);
    REQUIRE_FALSE(rollback_config->flx_sync_requested);
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
        migration_store->complete_migration_or_rollback();
        check_migration_complete(migration_store);

        // Cancel the migration and check the state
        migration_store->cancel_migration();
        check_not_migrated(migration_store);
    }

    SECTION("Migration store complete and rollback", "[flx][migration]") {
        // Start the migration and check the state
        migration_store->migrate_to_flx(rql_string, migrated_partition);
        check_migration_in_progress(migration_store);

        // Call in progress again and check the state (can be called multiple times)
        migration_store->migrate_to_flx(rql_string, migrated_partition);
        check_migration_in_progress(migration_store);

        // Complete the migration and check the state
        migration_store->complete_migration_or_rollback();
        check_migration_complete(migration_store);

        // Start the rollback and check the state
        migration_store->rollback_to_pbs();
        check_rollback_in_progress(migration_store);

        // Call in progress again and check the state (can be called multiple times)
        migration_store->rollback_to_pbs();
        check_rollback_in_progress(migration_store);

        // Complete the rollback and check the state
        migration_store->complete_migration_or_rollback();
        check_not_migrated(migration_store);
    }

    SECTION("Migration store complete without in progress", "[flx][migration]") {
        check_not_migrated(migration_store);

        // Complete the migration and check the state - should be not migrated
        migration_store->complete_migration_or_rollback();
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
        migration_store->complete_migration_or_rollback();
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

#endif // REALM_ENABLE_SYNC && REALM_ENABLE_AUTH_TESTS
