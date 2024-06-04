#include <realm/sync/noinst/migration_store.hpp>

#include <realm/transaction.hpp>
#include <realm/sync/noinst/sync_metadata_schema.hpp>

namespace realm::sync {
namespace {
constexpr static int c_schema_version = 1;
constexpr static std::string_view c_flx_migration_table("flx_migration");
constexpr static std::string_view c_flx_migration_started_at("started_at");
constexpr static std::string_view c_flx_migration_completed_at("completed_at");
constexpr static std::string_view c_flx_migration_state("state");
constexpr static std::string_view c_flx_migration_query_string("query_string");
constexpr static std::string_view c_flx_migration_original_partition("original_partition");
constexpr static std::string_view
    c_flx_migration_sentinel_subscription_set_version("sentinel_subscription_set_version");
constexpr static std::string_view c_flx_subscription_name_prefix("flx_migrated_");

} // namespace

MigrationStoreRef MigrationStore::create(DBRef db)
{
    return std::make_shared<MigrationStore>(Private(), std::move(db));
}

MigrationStore::MigrationStore(Private, DBRef db)
    : m_db(std::move(db))
    , m_state(MigrationState::NotMigrated)
{
    load_data(true); // read_only, default to NotMigrated if table is not initialized
}

bool MigrationStore::load_data(bool read_only)
{
    std::unique_lock lock{m_mutex};

    if (m_migration_table) {
        return true; // already initialized
    }

    std::vector<SyncMetadataTable> internal_tables{
        {&m_migration_table,
         c_flx_migration_table,
         {
             {&m_migration_started_at, c_flx_migration_started_at, type_Timestamp},
             {&m_migration_completed_at, c_flx_migration_completed_at, type_Timestamp, true},
             {&m_migration_state, c_flx_migration_state, type_Int},
             {&m_migration_query_str, c_flx_migration_query_string, type_String},
             {&m_migration_partition, c_flx_migration_original_partition, type_String},
             {&m_sentinel_query_version, c_flx_migration_sentinel_subscription_set_version, type_Int, true},
         }},
    };

    auto tr = m_db->start_read();
    // Start with a reader so it doesn't try to write until we are ready
    SyncMetadataSchemaVersionsReader schema_versions_reader(tr);
    if (auto schema_version =
            schema_versions_reader.get_version_for(tr, internal_schema_groups::c_flx_migration_store)) {
        if (*schema_version != c_schema_version) {
            throw RuntimeError(ErrorCodes::UnsupportedFileFormatVersion,
                               "Invalid schema version for flexible sync migration store metadata");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }
    else {
        if (read_only) {
            // Writing is disabled
            return false; // Either table is not initialized or version does not exist
        }
        tr->promote_to_write();
        // Ensure the schema versions table is initialized (may add its own commit)
        SyncMetadataSchemaVersions schema_versions(tr);
        // Create the metadata schema and set the version (in the same commit)
        schema_versions.set_version_for(tr, internal_schema_groups::c_flx_migration_store, c_schema_version);
        create_sync_metadata_schema(tr, &internal_tables);
        tr->commit_and_continue_as_read();
    }
    REALM_ASSERT(m_migration_table);

    // Read the migration object if exists, or default to not migrated
    if (auto migration_table = tr->get_table(m_migration_table); !migration_table->is_empty()) {
        auto migration_store_obj = migration_table->get_object(0);
        m_state = static_cast<MigrationState>(migration_store_obj.get<int64_t>(m_migration_state));
        m_query_string = migration_store_obj.get<String>(m_migration_query_str);
        m_migrated_partition = migration_store_obj.get<String>(m_migration_partition);
        m_sentinel_subscription_set_version =
            migration_store_obj.get<util::Optional<int64_t>>(m_sentinel_query_version);
    }
    else {
        m_state = MigrationState::NotMigrated;
        m_query_string.reset();
        m_migrated_partition.reset();
        m_sentinel_subscription_set_version.reset();
    }
    return true;
}

bool MigrationStore::is_migration_in_progress()
{
    std::lock_guard lock{m_mutex};
    return m_state == MigrationState::InProgress;
}

bool MigrationStore::is_migrated()
{
    std::lock_guard lock{m_mutex};
    return m_state == MigrationState::Migrated;
}

bool MigrationStore::is_rollback_in_progress()
{
    std::lock_guard lock{m_mutex};
    return m_state == MigrationState::RollbackInProgress;
}

void MigrationStore::complete_migration_or_rollback()
{
    // Ensure the migration table has been initialized
    bool loaded = load_data();
    REALM_ASSERT(loaded);

    std::unique_lock lock{m_mutex};
    if (m_state != MigrationState::InProgress && m_state != MigrationState::RollbackInProgress) {
        return;
    }

    // Complete rollback.
    if (m_state == MigrationState::RollbackInProgress) {
        clear(std::move(lock)); // releases the lock
        return;
    }

    // Complete migration.
    m_state = MigrationState::Migrated;

    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    REALM_ASSERT(!migration_table->is_empty());
    auto migration_store_obj = migration_table->get_object(0);
    migration_store_obj.set(m_migration_state, int64_t(m_state));
    migration_store_obj.set(m_migration_completed_at, Timestamp{std::chrono::system_clock::now()});
    tr->commit();
}

std::optional<std::string> MigrationStore::get_migrated_partition()
{
    std::lock_guard lock{m_mutex};
    // This will be valid if migration in progress or complete
    return m_migrated_partition;
}

std::optional<std::string> MigrationStore::get_query_string()
{
    std::lock_guard lock{m_mutex};
    // This will be valid if migration in progress or complete
    return m_query_string;
}

std::shared_ptr<realm::SyncConfig> MigrationStore::convert_sync_config(std::shared_ptr<realm::SyncConfig> config)
{
    REALM_ASSERT(config);
    // If load data failed in the constructor, m_state defaults to NotMigrated

    std::unique_lock lock{m_mutex};
    if (config->flx_sync_requested || m_state == MigrationState::NotMigrated ||
        m_state == MigrationState::RollbackInProgress) {
        return config;
    }

    // Once in the migrated state, the partition value cannot change for the same realm file
    if (m_state == MigrationState::Migrated && m_migrated_partition &&
        m_migrated_partition != config->partition_value) {
        throw LogicError(
            ErrorCodes::IllegalOperation,
            util::format("Partition value cannot be changed for migrated realms\n - original: %1\n -   config: %2",
                         m_migrated_partition, config->partition_value));
    }

    return convert_sync_config_to_flx(std::move(config));
}

std::shared_ptr<realm::SyncConfig>
MigrationStore::convert_sync_config_to_flx(std::shared_ptr<realm::SyncConfig> config)
{
    if (config->flx_sync_requested) {
        return config;
    }

    auto flx_config = std::make_shared<realm::SyncConfig>(*config); // deep copy
    flx_config->partition_value = "";
    flx_config->flx_sync_requested = true;

    return flx_config;
}

void MigrationStore::migrate_to_flx(std::string_view rql_query_string, std::string_view partition_value)
{
    REALM_ASSERT(!rql_query_string.empty());

    // Ensure the migration table has been initialized
    bool loaded = load_data();
    REALM_ASSERT(loaded);

    std::unique_lock lock{m_mutex};
    // Can call migrate_to_flx multiple times if migration has not completed.
    REALM_ASSERT(m_state != MigrationState::Migrated);
    m_state = MigrationState::InProgress;
    m_query_string.emplace(rql_query_string);
    m_migrated_partition.emplace(partition_value);

    auto tr = m_db->start_read();
    auto migration_table = tr->get_table(m_migration_table);
    // A migration object may exist if the migration was started in a previous session.
    if (migration_table->is_empty()) {
        tr->promote_to_write();
        auto migration_store_obj = migration_table->create_object();
        migration_store_obj.set(m_migration_query_str, *m_query_string);
        migration_store_obj.set(m_migration_state, int64_t(m_state));
        migration_store_obj.set(m_migration_partition, *m_migrated_partition);
        migration_store_obj.set(m_migration_started_at, Timestamp{std::chrono::system_clock::now()});
        tr->commit();
    }
    else {
        auto migration_store_obj = migration_table->get_object(0);
        auto state = static_cast<MigrationState>(migration_store_obj.get<int64_t>(m_migration_state));
        auto query_string = migration_store_obj.get<String>(m_migration_query_str);
        auto migrated_partition = migration_store_obj.get<String>(m_migration_partition);
        REALM_ASSERT(m_state == state);
        REALM_ASSERT(m_query_string == query_string);
        REALM_ASSERT(m_migrated_partition == migrated_partition);
    }
}

void MigrationStore::rollback_to_pbs()
{
    // Ensure the migration table has been initialized
    bool loaded = load_data();
    REALM_ASSERT(loaded);

    std::unique_lock lock{m_mutex};
    // Can call rollback_to_pbs multiple times if rollback has not completed.
    REALM_ASSERT(m_state != MigrationState::NotMigrated);
    m_state = MigrationState::RollbackInProgress;

    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    REALM_ASSERT(!migration_table->is_empty());
    auto migration_store_obj = migration_table->get_object(0);
    migration_store_obj.set(m_migration_state, int64_t(m_state));
    tr->commit();
}

void MigrationStore::cancel_migration()
{
    // Ensure the migration table has been initialized
    bool loaded = load_data();
    REALM_ASSERT(loaded);

    // Clear the migration state
    std::unique_lock lock{m_mutex};
    REALM_ASSERT(m_state == MigrationState::Migrated);
    clear(std::move(lock)); // releases the lock
}

void MigrationStore::clear(std::unique_lock<std::mutex>)
{
    // Make sure the migration table has been initialized before calling clear()
    REALM_ASSERT(m_migration_table);

    auto tr = m_db->start_read();
    auto migration_table = tr->get_table(m_migration_table);
    if (migration_table->is_empty()) {
        return; // already cleared
    }

    m_state = MigrationState::NotMigrated;
    m_query_string.reset();
    m_migrated_partition.reset();
    m_sentinel_subscription_set_version.reset();
    tr->promote_to_write();
    migration_table->clear();
    tr->commit();
}

Subscription MigrationStore::make_subscription(const std::string& object_class_name,
                                               const std::string& rql_query_string)
{
    REALM_ASSERT(!object_class_name.empty());

    std::string subscription_name = c_flx_subscription_name_prefix.data() + object_class_name;
    return Subscription{subscription_name, object_class_name, rql_query_string};
}

void MigrationStore::create_subscriptions(SubscriptionStore& subs_store)
{
    std::unique_lock lock{m_mutex};
    if (m_state != MigrationState::Migrated) {
        return;
    }

    REALM_ASSERT(m_query_string);
    create_subscriptions(subs_store, *m_query_string);
}

void MigrationStore::create_subscriptions(SubscriptionStore& subs_store, const std::string& rql_query_string)
{
    if (rql_query_string.empty()) {
        return;
    }

    auto mut_sub = subs_store.get_latest().make_mutable_copy();
    auto sub_count = mut_sub.size();

    auto tr = m_db->start_read();
    // List of tables covered by the latest subscription set.
    auto tables = subs_store.get_tables_for_latest(*tr);

    // List of tables in the realm.
    auto table_keys = tr->get_table_keys();
    for (auto key : table_keys) {
        if (!tr->table_is_public(key)) {
            continue;
        }
        auto table = tr->get_table(key);
        if (table->get_table_type() != Table::Type::TopLevel) {
            continue;
        }
        auto object_class_name = table->get_class_name();
        if (tables.find(object_class_name) == tables.end()) {
            auto sub = make_subscription(object_class_name, rql_query_string);
            mut_sub.insert_sub(sub);
        }
    }

    // No new subscription was added.
    if (mut_sub.size() == sub_count) {
        return;
    }

    // Commit new subscription set.
    mut_sub.commit();
}

void MigrationStore::create_sentinel_subscription_set(SubscriptionStore& subs_store)
{
    std::lock_guard lock{m_mutex};
    if (m_state != MigrationState::Migrated) {
        return;
    }
    if (m_sentinel_subscription_set_version) {
        return;
    }
    auto mut_sub = subs_store.get_latest().make_mutable_copy();
    auto subscription_set_version = mut_sub.commit().version();
    m_sentinel_subscription_set_version.emplace(subscription_set_version);

    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    REALM_ASSERT(!migration_table->is_empty());
    auto migration_store_obj = migration_table->get_object(0);
    migration_store_obj.set(m_sentinel_query_version, *m_sentinel_subscription_set_version);
    tr->commit();
}

std::optional<int64_t> MigrationStore::get_sentinel_subscription_set_version()
{
    std::lock_guard lock{m_mutex};
    return m_sentinel_subscription_set_version;
}

} // namespace realm::sync
