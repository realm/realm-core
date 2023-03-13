#include "sync/flx_sync_harness.hpp"
#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"

#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>


#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

using namespace realm;

static void trigger_server_migration(const AppSession& app_session, bool switch_to_flx,
                                     const std::shared_ptr<util::Logger>& logger = nullptr)
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
            return "PBS->FLX Migration";
        else
            return "FLX->PBS Rollback";
    }();
    const int duration = 90;
    try {
        timed_sleeping_wait_for(
            [&] {
                status = app_session.admin_api.get_migration_status(app_session.server_app_id);
                if (logger && last_status != status.statusMessage) {
                    last_status = status.statusMessage;
                    logger->debug("%1 status: %2", op_stg, last_status);
                }
                return status.errorMessage || status.complete;
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
        if (status.errorMessage)
            logger->debug("%1 returned error: %2", op_stg, *status.errorMessage);
        else
            logger->debug("%1 complete", op_stg);
    }
    REQUIRE(!status.errorMessage);
    REQUIRE(switch_to_flx == status.isMigrated);
}

// Add a set of count number of Dog objects to the realm
static std::vector<ObjectId> fill_test_data(SyncTestFile& config, std::string partition, int start = 1, int count = 5)
{
    std::vector<ObjectId> ret;
    auto realm = Realm::get_shared_realm(config);
    realm->begin_transaction();
    CppContext c(realm);
    // Add some objects with the provided partition value
    for (int i = 0; i < count; i++, ++start) {
        auto id = ObjectId::gen();
        auto obj = Object::create(c, realm, "Dog",
                                  std::any(AnyDict{{"_id", std::any(id)},
                                                   {"breed", util::format("breed-%1", start)},
                                                   {"name", util::format("dog-%1", start)},
                                                   {"realm_id", partition}}));
        ret.push_back(id);
    }
    realm->commit_transaction();
    return ret;
}

TEST_CASE("Test server migration and rollback", "[flx],[migration]") {
    std::shared_ptr<util::Logger> logger_ptr =
        std::make_shared<util::StderrLogger>(realm::util::Logger::Level::TEST_LOGGING_LEVEL);

    const std::string base_url = get_base_url();
    const std::string partition1 = "migration-test";
    const std::string partition2 = "another-value";
    auto server_app_config = default_app_config(base_url);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config1(session.app(), partition1, server_app_config.schema);
    SyncTestFile config2(session.app(), partition2, server_app_config.schema);

    // Fill some objects
    auto objects1 = fill_test_data(config1, partition1, 1, 5);
    auto objects2 = fill_test_data(config2, partition2, 6, 5);

    auto check_data = [&](SharedRealm& realm, bool check_set1, bool check_set2) {
        auto table = realm->read_group().get_table("class_Dog");
        auto partition_col = table->get_column_key("realm_id");
        auto breed_col = table->get_column_key("breed");

        size_t table_size = [check_set1, check_set2] {
            if (check_set1 && check_set2)
                return 10;
            if (check_set1 || check_set2)
                return 5;
            return 0;
        }();

        REQUIRE(table->size() == table_size);
        REQUIRE(bool(table->find_first(partition_col, StringData(partition1))) == check_set1);
        REQUIRE(bool(table->find_first(breed_col, StringData("breed-5"))) == check_set1);
        REQUIRE(bool(table->find_first(partition_col, StringData(partition2))) == check_set2);
        REQUIRE(bool(table->find_first(breed_col, StringData("breed-6"))) == check_set2);
    };

    // Wait for the two partition sets to upload
    {
        auto realm1 = Realm::get_shared_realm(config1);

        CHECK(!wait_for_upload(*realm1));
        CHECK(!wait_for_download(*realm1));

        check_data(realm1, true, false);

        auto realm2 = Realm::get_shared_realm(config2);

        CHECK(!wait_for_upload(*realm2));
        CHECK(!wait_for_download(*realm2));

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

            CHECK(!wait_for_upload(*flx_realm));
            CHECK(!wait_for_download(*flx_realm));

            check_data(flx_realm, false, false);
        }

        {
            auto flx_table = flx_realm->read_group().get_table("class_Dog");
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(
                "flx_migrated_Dog_1",
                Query(flx_table).equal(flx_table->get_column_key("realm_id"), StringData{partition1}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            CHECK(!wait_for_upload(*flx_realm));
            CHECK(!wait_for_download(*flx_realm));
            wait_for_advance(*flx_realm);

            check_data(flx_realm, true, false);
        }

        {
            auto flx_table = flx_realm->read_group().get_table("class_Dog");
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.insert_or_assign(
                "flx_migrated_Dog_2",
                Query(flx_table).equal(flx_table->get_column_key("realm_id"), StringData{partition2}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            CHECK(!wait_for_upload(*flx_realm));
            CHECK(!wait_for_download(*flx_realm));
            wait_for_advance(*flx_realm);

            check_data(flx_realm, true, true);
        }
    }

    // Roll back to PBS
    trigger_server_migration(session.app_session(), false, logger_ptr);

    {
        SyncTestFile pbs_config(session.app(), partition1, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        CHECK(!wait_for_upload(*pbs_realm));
        CHECK(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, true, false);
    }
    {
        SyncTestFile pbs_config(session.app(), partition2, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        CHECK(!wait_for_upload(*pbs_realm));
        CHECK(!wait_for_download(*pbs_realm));

        check_data(pbs_realm, false, true);
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
