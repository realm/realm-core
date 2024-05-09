/*************************************************************************
 *
 * Copyright 2023 Realm, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/sync/noinst/sync_schema_migration.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset_recovery.hpp>

#include <string_view>

using namespace realm;
using namespace _impl;

namespace realm::_impl::sync_schema_migration {

// A table without a "class_" prefix will not generate sync instructions.
constexpr static std::string_view s_meta_schema_migration_table_name("schema_migration_metadata");
constexpr static std::string_view s_pk_col_name("id");
constexpr static std::string_view s_version_column_name("version");
constexpr static std::string_view s_timestamp_col_name("event_time");
constexpr static std::string_view s_previous_schema_version_col_name("previous_schema_version");
constexpr int64_t metadata_version = 1;

std::optional<uint64_t> has_pending_migration(const Transaction& rt)
{
    ConstTableRef table = rt.get_table(s_meta_schema_migration_table_name);
    if (!table || table->size() == 0) {
        return none;
    }
    ColKey timestamp_col = table->get_column_key(s_timestamp_col_name);
    ColKey version_col = table->get_column_key(s_version_column_name);
    ColKey previous_schema_version_col = table->get_column_key(s_previous_schema_version_col_name);
    REALM_ASSERT(timestamp_col);
    REALM_ASSERT(version_col);
    REALM_ASSERT(previous_schema_version_col);

    Obj first = *table->begin();
    REALM_ASSERT(first);
    auto version = first.get<int64_t>(version_col);
    auto time = first.get<Timestamp>(timestamp_col);
    if (version != metadata_version) {
        throw SyncSchemaMigrationFailed(
            util::format("Unsupported sync schema migration metadata version: %1 vs %2, from %3", version,
                         metadata_version, time));
    }
    return first.get<int64_t>(previous_schema_version_col);
}

void track_sync_schema_migration(Transaction& wt, uint64_t previous_schema_version)
{
    TableRef table = wt.get_table(s_meta_schema_migration_table_name);
    ColKey version_col, timestamp_col, previous_schema_version_col;
    if (!table) {
        table = wt.add_table_with_primary_key(s_meta_schema_migration_table_name, type_ObjectId, s_pk_col_name);
        REALM_ASSERT(table);
        version_col = table->add_column(type_Int, s_version_column_name);
        timestamp_col = table->add_column(type_Timestamp, s_timestamp_col_name);
        previous_schema_version_col = table->add_column(type_Int, s_previous_schema_version_col_name);
    }
    else {
        version_col = table->get_column_key(s_version_column_name);
        timestamp_col = table->get_column_key(s_timestamp_col_name);
        previous_schema_version_col = table->get_column_key(s_previous_schema_version_col_name);
    }
    REALM_ASSERT(version_col);
    REALM_ASSERT(timestamp_col);
    REALM_ASSERT(previous_schema_version_col);

    // A migration object may exist if the migration was started in a previous session.
    if (table->is_empty()) {
        table->create_object_with_primary_key(ObjectId::gen(),
                                              {{version_col, metadata_version},
                                               {timestamp_col, Timestamp(std::chrono::system_clock::now())},
                                               {previous_schema_version_col, int64_t(previous_schema_version)}});
    }
    else {
        auto first = *table->begin();
        auto version = first.get<int64_t>(version_col);
        auto time = first.get<Timestamp>(timestamp_col);
        if (version != metadata_version) {
            throw SyncSchemaMigrationFailed(
                util::format("Unsupported sync schema migration metadata version: %1 vs %2, from %3", version,
                             metadata_version, time));
        }
        uint64_t schema_version = first.get<int64_t>(previous_schema_version_col);
        if (schema_version != previous_schema_version) {
            throw SyncSchemaMigrationFailed(
                util::format("Cannot continue sync schema migration with different previous schema version (existing "
                             "previous_schema_version=%1, new previous_schema_version=%2)",
                             schema_version, previous_schema_version));
        }
    }
}

void perform_schema_migration(DB& db)
{
    // Everything is performed in one single write transaction.
    auto tr = db.start_write();

    // Disable sync replication.
    auto& repl = dynamic_cast<sync::ClientReplication&>(*db.get_replication());
    sync::TempShortCircuitReplication sync_history_guard(repl);
    repl.set_write_validator_factory(nullptr);

    // Delete all columns before deleting tables to avoid complications with links
    for (auto tk : tr->get_table_keys()) {
        tr->get_table(tk)->remove_columns();
    }
    for (auto tk : tr->get_table_keys()) {
        tr->remove_table(tk);
    }

    // Clear sync history, reset the file ident and the server version in the download and upload progress.

    auto& history = repl.get_history();
    sync::SaltedFileIdent reset_file_ident{0, 0};
    sync::SaltedVersion reset_server_version{0, 0};
    std::vector<_impl::client_reset::RecoveredChange> changes{};
    history.set_history_adjustments(*db.get_logger(), tr->get_version(), reset_file_ident, reset_server_version,
                                    changes);

    tr->commit();
}

} // namespace realm::_impl::sync_schema_migration
