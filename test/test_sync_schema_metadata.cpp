#include <realm/transaction.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/sync_metadata_schema.hpp>

#include "test.hpp"
#include "util/test_path.hpp"

#include <filesystem>

constexpr static std::string_view c_legacy_table_name("flx_metadata");
constexpr static std::string_view c_current_table_name("sync_internal_schemas");

using namespace realm;
using namespace realm::sync;

TEST(SyncSchemaVersions_LegacyMigration)
{
    SHARED_GROUP_TEST_PATH(path)

    auto bundled_path =
        std::filesystem::path(test_util::get_test_resource_path()) / "test_flx_metadata_tables_v1.realm";
    CHECK(util::File::exists(bundled_path.string()));
    util::File::copy(bundled_path.string(), path);
    auto db = DB::create(make_client_replication(), path);

    auto tr = db->start_read();

    // Verify that the pre-migration file is in the expected shape
    auto legacy_table = tr->get_table(c_legacy_table_name);
    CHECK(legacy_table);
    CHECK_EQUAL(legacy_table->size(), 1);
    CHECK_EQUAL(legacy_table->get_column_count(), 1);
    CHECK_EQUAL(legacy_table->get_object(0).get<int64_t>("schema_version"), 2);
    CHECK_NOT(tr->has_table(c_current_table_name));

    std::string_view group_name = "a schema group name";
    {
        SyncMetadataSchemaVersions versions(*tr);

        // should not have migrated anything
        CHECK(tr->has_table(c_legacy_table_name));
        CHECK_NOT(tr->has_table(c_current_table_name));

        // should report the correct version even though the table wasn't migrated
        CHECK_EQUAL(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store), 2);
        CHECK_NOT(versions.get_version_for(*tr, group_name));

        // writing a schema version should migrate the tables
        tr->promote_to_write();
        versions.set_version_for(*tr, group_name, 5);
        tr->commit_and_continue_as_read();

        CHECK_NOT(tr->has_table(c_legacy_table_name));
        CHECK(tr->has_table(c_current_table_name));
        CHECK_EQUAL(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store), 2);
        CHECK_EQUAL(versions.get_version_for(*tr, group_name), 5);
        CHECK_NOT(versions.get_version_for(*tr, "invalid"));
    }

    // Reopen the migrated version store
    {
        SyncMetadataSchemaVersions versions(*tr);
        CHECK_EQUAL(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store), 2);
        CHECK_EQUAL(versions.get_version_for(*tr, group_name), 5);
        CHECK_NOT(versions.get_version_for(*tr, "invalid"));
    }
}

TEST(SyncSchemaVersions_ReportsNoneBeforeFirstSet)
{
    SHARED_GROUP_TEST_PATH(path)
    auto db = DB::create(make_client_replication(), path);
    auto tr = db->start_read();
    SyncMetadataSchemaVersions versions(*tr);
    CHECK_NOT(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store));
    CHECK_NOT(versions.get_version_for(*tr, internal_schema_groups::c_pending_bootstraps));
}

TEST(SyncSchemaVersions_PersistsSetValues)
{
    SHARED_GROUP_TEST_PATH(path)

    {
        auto db = DB::create(make_client_replication(), path);
        auto tr = db->start_write();
        SyncMetadataSchemaVersions versions(*tr);
        versions.set_version_for(*tr, internal_schema_groups::c_flx_subscription_store, 123);
        versions.set_version_for(*tr, internal_schema_groups::c_pending_bootstraps, 456);
        tr->commit();
    }
    {
        auto db = DB::create(make_client_replication(), path);
        auto tr = db->start_read();
        SyncMetadataSchemaVersions versions(*tr);
        CHECK_EQUAL(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store), 123);
        CHECK_EQUAL(versions.get_version_for(*tr, internal_schema_groups::c_pending_bootstraps), 456);
    }
}

TEST(SyncSchemaVersions_CreatesTableWhenFirstNeeded)
{
    SHARED_GROUP_TEST_PATH(path)
    auto db = DB::create(make_client_replication(), path);
    auto tr = db->start_read();
    SyncMetadataSchemaVersions versions(*tr);
    CHECK_EQUAL(tr->size(), 0);
    CHECK_NOT(versions.get_version_for(*tr, internal_schema_groups::c_flx_subscription_store));
    tr->promote_to_write();
    versions.set_version_for(*tr, internal_schema_groups::c_flx_subscription_store, 123);
    CHECK_EQUAL(tr->size(), 1);
}
