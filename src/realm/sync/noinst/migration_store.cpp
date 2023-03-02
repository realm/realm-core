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
    SyncMetadataSchemaVersions schema_versions(tr);

    if (auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_flx_migration_store);
        !schema_version) {
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

    // create migration object if it doesn't exist
    if (auto migration_table = tr->get_table(m_migration_table); migration_table->is_empty()) {
        tr->promote_to_write();
        auto migration_store_obj = migration_table->create_object();
        migration_store_obj.set(m_migration_state, int64_t(MigrationState::NotStarted));
        m_state = MigrationState::NotStarted;
        tr->commit();
    }
    else {
        auto migration_store_obj = migration_table->get_object(0);
        m_state = static_cast<MigrationState>(migration_store_obj.get<int64_t>(m_migration_state));
        m_query_string = migration_store_obj.get<String>(m_migration_query_str);
    }
}

std::shared_ptr<realm::SyncConfig> MigrationStore::convert_sync_config(std::shared_ptr<realm::SyncConfig> config)
{
    REALM_ASSERT(config);

    std::lock_guard lock{m_mutex};
    if (m_state == MigrationState::Completed && config->flx_sync_requested) {
        // Migration state is no longer needed - natively using flexible sync
        reset_migration_info();
        return config; // return original sync config
    }
    if (m_state == MigrationState::NotStarted) {
        // Using PBS and not migrated, return original sync config
        return config;
    }

    // Using PBS and migrated, update copy of original sync config to FLX and return
    auto flx_config = std::make_shared<realm::SyncConfig>(*config); // deep copy
    flx_config->partition_value = {};
    flx_config->flx_sync_requested = true;

    return flx_config;
}

void MigrationStore::migrate_to_flx(std::string_view rql_query_string)
{
    REALM_ASSERT(!rql_query_string.empty());

    {
        std::unique_lock lock{m_mutex};
        m_query_string = rql_query_string;
        m_state = MigrationState::Completed;

        auto tr = m_db->start_write();
        auto migration_store_obj = tr->get_table(m_migration_table)->get_object(0);
        migration_store_obj.set(m_migration_query_str, m_query_string);
        migration_store_obj.set(m_migration_state, int64_t(m_state));
        migration_store_obj.set(m_migration_completed_at, Timestamp{std::chrono::system_clock::now()});
        tr->commit();
    }

    m_on_migration_state_changed(MigrationState::Completed);
}

void MigrationStore::cancel_migration()
{
    // Clear the migration state
    reset_migration_info();

    m_on_migration_state_changed(MigrationState::NotStarted);
}

void MigrationStore::reset_migration_info()
{
    std::lock_guard lock{m_mutex};
    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    migration_table->clear();
    // create migration object
    auto migration_store_obj = migration_table->create_object();
    migration_store_obj.set(m_migration_state, int64_t(MigrationState::NotStarted));
    tr->commit_and_continue_as_read();
    tr->commit();
    m_state = MigrationState::NotStarted;
}

std::optional<Subscription> MigrationStore::make_subscription(const std::string& object_class_name)
{
    std::lock_guard lock{m_mutex};
    if (m_state == MigrationState::NotStarted) {
        return std::nullopt;
    }
    if (object_class_name.empty()) {
        return std::nullopt;
    }

    std::string subscription_name = c_flx_subscription_name_prefix.data() + object_class_name;
    return Subscription{subscription_name, object_class_name, m_query_string};
}

} // namespace realm::sync