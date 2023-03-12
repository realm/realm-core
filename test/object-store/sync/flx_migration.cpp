#include <catch2/catch_all.hpp>
#include <chrono>

#include "flx_sync_harness.hpp"
#include "sync/sync_test_utils.hpp"
#include "util/baas_admin_api.hpp"

#include "realm/object-store/impl/object_accessor_impl.hpp"


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
    const int duration = 90;
    try {
        timed_sleeping_wait_for(
            [&] {
                status = app_session.admin_api.get_migration_status(app_session.server_app_id);
                if (logger && last_status != status.statusMessage) {
                    last_status = status.statusMessage;
                    logger->debug("PBS->FLX Migration status: %1", last_status);
                }
                return status.complete;
            },
            std::chrono::seconds(duration), std::chrono::milliseconds(500));
    }
    catch (std::runtime_error) {
        if (logger)
            logger->debug("PBS->FLX Migration timed out after %1 seconds", duration);
        REQUIRE(false);
    }
    REQUIRE(switch_to_flx == status.isMigrated);
    if (logger) {
        if (!status.errorMessage.empty())
            logger->debug("PBS->FLX Migration returned error: %1", status.errorMessage);
        else
            logger->debug("PBS->FLX %1 complete", switch_to_flx ? "Migration" : "Rollback");
    }
    REQUIRE(status.errorMessage.empty());
}

// Populates a FLXSyncTestHarness with the g_large_array_schema with objects that are large enough that
// they are guaranteed to fill multiple bootstrap download messages. Currently this means generating 5
// objects each with 1024 array entries of 1024 bytes each.
//
// Returns a list of the _id values for the objects created.
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

    // Wait for the two partition sets to upload
    {
        auto realm1 = Realm::get_shared_realm(config1);
        CHECK(!wait_for_upload(*realm1));
        CHECK(!wait_for_download(*realm1));
        auto realm2 = Realm::get_shared_realm(config2);
        CHECK(!wait_for_upload(*realm2));
        CHECK(!wait_for_download(*realm2));
    }

    // Migrate to FLX
    trigger_server_migration(session.app_session(), true, logger_ptr);

    {
        SyncTestFile flx_config(session.app()->current_user(), server_app_config.schema,
                                SyncConfig::FLXSyncEnabled{});

        auto flx_realm = Realm::get_shared_realm(flx_config);

        wait_for_upload(*flx_realm);
        wait_for_download(*flx_realm);
        wait_for_advance(*flx_realm); // wait for initial bootstrap
        {
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            auto flx_table = flx_realm->read_group().get_table("class_Dog");
            auto partition_col = flx_table->get_column_key("realm_id");
            auto breed_col = flx_table->get_column_key("breed");

            REQUIRE(flx_table->size() == 0);
            REQUIRE_FALSE(flx_table->find_first(partition_col, StringData(partition1)));
            REQUIRE_FALSE(flx_table->find_first(breed_col, StringData("breed-5")));

            mut_subs.insert_or_assign("flx_migrated_Dog_1",
                                      Query(flx_table).equal(partition_col, StringData{partition1}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            wait_for_upload(*flx_realm);
            wait_for_download(*flx_realm);
            wait_for_advance(*flx_realm); // wait for subscription bootstrap

            REQUIRE(flx_table->size() == 5);
            REQUIRE(flx_table->find_first(partition_col, StringData(partition1)));
            REQUIRE(flx_table->find_first(breed_col, StringData("breed-5")));
            REQUIRE_FALSE(flx_table->find_first(partition_col, StringData(partition2)));
            REQUIRE_FALSE(flx_table->find_first(breed_col, StringData("breed-6")));
        }

        {
            auto mut_subs = flx_realm->get_latest_subscription_set().make_mutable_copy();
            auto flx_table = flx_realm->read_group().get_table("class_Dog");
            auto partition_col = flx_table->get_column_key("realm_id");
            auto breed_col = flx_table->get_column_key("breed");

            mut_subs.insert_or_assign("flx_migrated_Dog_2",
                                      Query(flx_table).equal(partition_col, StringData{partition2}));
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

            wait_for_upload(*flx_realm);
            wait_for_download(*flx_realm);
            wait_for_advance(*flx_realm); // wait for subscription bootstrap

            REQUIRE(flx_table->size() == 10);
            REQUIRE(flx_table->find_first(partition_col, StringData(partition1)));
            REQUIRE(flx_table->find_first(breed_col, StringData("breed-5")));
            REQUIRE(flx_table->find_first(partition_col, StringData(partition2)));
            REQUIRE(flx_table->find_first(breed_col, StringData("breed-6")));
        }
    }

    // Roll back to PBS
    trigger_server_migration(session.app_session(), false, logger_ptr);

    {
        SyncTestFile pbs_config(session.app(), partition1, server_app_config.schema);
        auto pbs_realm = Realm::get_shared_realm(pbs_config);

        CHECK(!wait_for_download(*pbs_realm));
        CHECK(!wait_for_upload(*pbs_realm));

        auto table = pbs_realm->read_group().get_table("class_Dog");
        auto partition_col = table->get_column_key("realm_id");
        auto breed_col = table->get_column_key("breed");
        REQUIRE(table->size() == 5);
        REQUIRE(table->find_first(partition_col, StringData(partition1)));
        REQUIRE(table->find_first(breed_col, StringData("breed-5")));
        REQUIRE_FALSE(table->find_first(partition_col, StringData(partition2)));
        REQUIRE_FALSE(table->find_first(breed_col, StringData("breed-6")));
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
