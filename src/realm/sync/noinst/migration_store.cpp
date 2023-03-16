#include <realm/sync/noinst/migration_store.hpp>

#include <realm/transaction.hpp>
#include <realm/sync/noinst/sync_metadata_schema.hpp>

namespace realm::sync {
namespace {
constexpr static int c_schema_version = 1;
constexpr static std::string_view c_flx_migration_table("flx_migration");
constexpr static std::string_view c_flx_migration_completed_at("flx_migration_completed_at");
constexpr static std::string_view c_flx_migration_state("flx_migration_state");
constexpr static std::string_view c_flx_migration_query_string("flx_migration_query_string");

constexpr static std::string_view c_flx_subscription_name_prefix("flx_migrated_");

class MigrationStoreInit : public MigrationStore {
public:
    explicit MigrationStoreInit(DBRef db,
                                std::function<void(MigrationStore::MigrationState)>&& on_migration_state_changed)
        : MigrationStore(std::move(db), std::move(on_migration_state_changed))
    {
    }
};

} // namespace

MigrationStoreRef
MigrationStore::create(DBRef db, std::function<void(MigrationStore::MigrationState)>&& on_migration_state_changed)
{
    return std::make_shared<MigrationStoreInit>(std::move(db), std::move(on_migration_state_changed));
}

MigrationStore::MigrationStore(DBRef db,
                               std::function<void(MigrationStore::MigrationState)>&& on_migration_state_changed)
    : m_db(std::move(db))
    , m_on_migration_state_changed(std::move(on_migration_state_changed))
    , m_state(MigrationState::NotMigrated)
    , m_query_string{}
{
    load_data(true); // read_only
}

bool MigrationStore::load_data(bool read_only)
{
    std::vector<SyncMetadataTable> internal_tables{
        {&m_migration_table,
         c_flx_migration_table,
         {
             {&m_migration_completed_at, c_flx_migration_completed_at, type_Timestamp},
             {&m_migration_state, c_flx_migration_state, type_Int},
             {&m_migration_query_str, c_flx_migration_query_string, type_String},
         }},
    };

    auto tr = m_db->start_read();
    if (!m_migration_table) {
        SyncMetadataSchemaVersions schema_versions(tr, read_only);
        if (!schema_versions.is_initialized()) {
            return false; // no data to read and writing is disabled
        }

        if (auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_flx_migration_store);
            !schema_version) {
            if (read_only) {
                return false; // no data to read and writing is disabled
            }

            tr->promote_to_write();
            schema_versions.set_version_for(tr, internal_schema_groups::c_flx_migration_store, c_schema_version);
            create_sync_metadata_schema(tr, &internal_tables);
            tr->commit_and_continue_as_read();
        }
        else {
            if (*schema_version != c_schema_version) {
                throw std::runtime_error("Invalid schema version for flexible sync migration store metadata");
            }
            load_sync_metadata_schema(tr, &internal_tables);
        }
    }

    // Read the migration object if exists, or default to not migrated
    std::lock_guard lock(m_mutex);
    if (auto migration_table = tr->get_table(m_migration_table); !migration_table->is_empty()) {
        auto migration_store_obj = migration_table->get_object(0);
        m_state = static_cast<MigrationState>(migration_store_obj.get<int64_t>(m_migration_state));
        m_query_string = migration_store_obj.get<String>(m_migration_query_str);
    }
    else {
        m_state = MigrationState::NotMigrated;
        m_query_string = {};
    }
    return true;
}

bool MigrationStore::is_migrated()
{
    std::lock_guard lock{m_mutex};
    return m_state == MigrationState::Migrated;
}

std::string_view MigrationStore::get_query_string()
{
    std::lock_guard lock{m_mutex};
    return m_query_string;
}

std::shared_ptr<realm::SyncConfig> MigrationStore::convert_sync_config(std::shared_ptr<realm::SyncConfig> config)
{
    REALM_ASSERT(config);

    {
        std::unique_lock lock{m_mutex};
        if (config->flx_sync_requested) {
            // Will need to clear the config if the user SyncConfig if FLX and state is
            // migrated, but not during download fresh realm as part of a client reset
            if (m_state == MigrationState::Migrated) {
                // No need to fire the notification callback here, just proceed as normal
                clear(std::move(lock));
            }
            return config;
        }
        if (m_state == MigrationState::NotMigrated) {
            return config;
        }
    }

    auto flx_config = std::make_shared<realm::SyncConfig>(*config); // deep copy
    flx_config->partition_value = "";
    flx_config->flx_sync_requested = true;

    return flx_config;
}

void MigrationStore::migrate_to_flx(std::string_view rql_query_string)
{
    REALM_ASSERT(m_state == MigrationState::NotMigrated);
    REALM_ASSERT(!rql_query_string.empty());

    {
        // Make sure the migration table has been initialized
        if (!m_migration_table) {
            REALM_ASSERT(load_data());
        }

        std::unique_lock lock{m_mutex};
        m_query_string = rql_query_string;
        m_state = MigrationState::Migrated;

        auto tr = m_db->start_write();
        auto migration_table = tr->get_table(m_migration_table);
        // This should be called in the non-migrated state, so the migration table should not exist
        REALM_ASSERT(migration_table->is_empty());
        auto migration_store_obj = migration_table->create_object();
        migration_store_obj.set(m_migration_query_str, m_query_string);
        migration_store_obj.set(m_migration_state, int64_t(m_state));
        migration_store_obj.set(m_migration_completed_at, Timestamp{std::chrono::system_clock::now()});
        tr->commit();
    }

    m_on_migration_state_changed(MigrationState::Migrated);
}

void MigrationStore::cancel_migration()
{
    // Clear the migration state
    std::unique_lock lock{m_mutex};
    clear(std::move(lock));

    m_on_migration_state_changed(MigrationState::NotMigrated);
}

void MigrationStore::clear(std::unique_lock<std::mutex> lock)
{
    // Make sure the migration table has been initialized
    lock.unlock();
    if (!m_migration_table) {
        REALM_ASSERT(load_data());
    }

    auto tr = m_db->start_read();
    auto migration_table = tr->get_table(m_migration_table);
    if (migration_table->is_empty()) {
        return; // already cleared
    }

    lock.lock();
    m_state = MigrationState::NotMigrated;
    m_query_string = {};
    tr->promote_to_write();
    migration_table->clear();
    tr->commit();
}

std::optional<Subscription> MigrationStore::make_subscription(const std::string& object_class_name)
{
    std::lock_guard lock{m_mutex};
    if (m_state == MigrationState::NotMigrated) {
        return std::nullopt;
    }
    if (object_class_name.empty()) {
        return std::nullopt;
    }

    std::string subscription_name = c_flx_subscription_name_prefix.data() + object_class_name;
    return Subscription{subscription_name, object_class_name, m_query_string};
}

} // namespace realm::sync