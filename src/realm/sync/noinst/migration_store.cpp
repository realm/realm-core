#include <realm/sync/noinst/migration_store.hpp>

#include <realm/transaction.hpp>
#include <realm/sync/noinst/sync_metadata_schema.hpp>

#include <external/json/json.hpp>

namespace realm::sync {
namespace {
constexpr static int c_schema_version = 1;
constexpr static std::string_view c_flx_migration_table("flx_migration");
constexpr static std::string_view c_flx_migration_started_at("flx_migration_started_at");
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
             {&m_migration_started_at, c_flx_migration_started_at, type_Timestamp},
             {&m_migration_started_at, c_flx_migration_completed_at, type_Timestamp},
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
        // create migration object
        auto migration_store_obj = tr->get_table(m_migration_table)->create_object();
        migration_store_obj.set(m_migration_started_at, Timestamp{std::chrono::system_clock::now()});
        migration_store_obj.set(m_migration_state, MigrationState::InProgress);
        tr->commit_and_continue_as_read();
        m_state = MigrationState::InProgress;
    }
    else {
        if (*schema_version != c_schema_version) {
            throw std::runtime_error("Invalid schema version for flexible sync migration store metadata");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }

    m_on_migration_state_changed(m_state);
}

std::shared_ptr<realm::SyncConfig> MigrationStore::convert_sync_config(std::shared_ptr<realm::SyncConfig> config)
{
    if (m_state == MigrationState::Completed && config->flx_sync_requested) {
        cancel_migration();
        return config;
    }

    auto flx_config = std::make_shared<realm::SyncConfig>(*config); // deep copy
    flx_config->partition_value = "";
    flx_config->flx_sync_requested = true;

    return flx_config;
}

void MigrationStore::migrate_to_flx(std::string rql_query_string)
{
    std::unique_lock lock{m_mutex};
    m_query_string = rql_query_string;

    auto queries = nlohmann::json::parse(m_query_string);
    m_table_to_query.clear();

    for (auto&& [k, v] : queries.items()) {
        m_table_to_query[k] = v;
    }

    m_state = MigrationState::Completed;
    lock.unlock();

    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    auto migration_table_obj = migration_table->get_object(0);
    migration_table_obj.set(c_flx_migration_query_string, rql_query_string);
    migration_table_obj.set(c_flx_migration_state, m_state);
    migration_table_obj.set(c_flx_migration_completed_at, Timestamp{std::chrono::system_clock::now()});
    tr->commit();

    m_on_migration_state_changed(m_state);
}

void MigrationStore::cancel_migration()
{
    auto tr = m_db->start_write();
    auto migration_table = tr->get_table(m_migration_table);
    migration_table->clear();
    tr->commit();
}

std::optional<Subscription> MigrationStore::make_subscription(const std::string& object_class_name)
{
    std::lock_guard lock{m_mutex};
    if (m_state != MigrationState::Completed) {
        return std::nullopt;
    }

    auto it = m_table_to_query.find(object_class_name);
    if (it == m_table_to_query.end()) {
        return std::nullopt;
    }

    std::string subscription_name = c_flx_subscription_name_prefix.data() + object_class_name;
    return Subscription{subscription_name, object_class_name, it->second};
}

} // namespace realm::sync