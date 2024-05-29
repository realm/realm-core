///////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/sync/noinst/pending_reset_store.hpp>

#include <realm/sync/noinst/client_reset.hpp>
#include <realm/sync/noinst/client_reset_recovery.hpp>

#include <chrono>
#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace realm::sync {

std::ostream& operator<<(std::ostream& os, const sync::PendingReset& pr)
{
    if (pr.action == sync::ProtocolErrorInfo::Action::NoAction || pr.time.is_null()) {
        os << "empty pending client reset";
    }
    else if (pr.action != sync::ProtocolErrorInfo::Action::ClientReset) {
        os << "pending '" << pr.action << "' client reset of type: '" << pr.mode << "' at: " << pr.time;
    }
    else {
        os << "pending client reset of type: '" << pr.mode << "' at: " << pr.time;
    }
    if (pr.error) {
        os << " for error: " << *pr.error;
    }
    return os;
}

bool operator==(const sync::PendingReset& lhs, const sync::PendingReset& rhs)
{
    return (lhs.mode == rhs.mode && lhs.action == rhs.action && lhs.time == rhs.time);
}

bool operator==(const sync::PendingReset& lhs, const PendingReset::Action& action)
{
    return lhs.action == action;
}

// A table without a "class_" prefix will not generate sync instructions.
constexpr static std::string_view s_meta_reset_table_name("client_reset_metadata");
constexpr static std::string_view s_pk_col_name("id");
constexpr static std::string_view s_timestamp_col_name("reset_time");
constexpr static std::string_view s_reset_recovery_mode_col_name("reset_mode");
constexpr static std::string_view s_reset_action_col_name("reset_action");
constexpr static std::string_view s_reset_error_code_col_name("reset_error_code");
constexpr static std::string_view s_reset_error_msg_col_name("reset_error_msg");
constexpr int64_t s_pending_reset_version = 2;

void PendingResetStore::clear_pending_reset(const TransactionRef& wr_tr)
{
    // Write transaction required
    REALM_ASSERT(wr_tr->get_transact_stage() == DB::TransactStage::transact_Writing);
    auto reset_store = PendingResetStore::load_or_create_schema(wr_tr);
    REALM_ASSERT(reset_store.m_pending_reset_table);
    // Ensure the pending reset table is empty
    if (auto table = wr_tr->get_table(reset_store.m_pending_reset_table); table && !table->is_empty()) {
        table->clear();
    }
    // Don't commit at the end - allow the caller to do it when they are ready
}

std::optional<PendingReset> PendingResetStore::has_pending_reset(const TransactionRef& rd_tr)
{
    // Make sure the schema has been loaded and try to read legacy data if it's not found
    auto reset_store = PendingResetStore::load_schema(rd_tr);
    if (!reset_store) {
        return PendingResetStore::read_legacy_pending_reset(rd_tr);
    }
    // Otherwise, read the pending reset entry using the schema metadata
    REALM_ASSERT(reset_store->m_pending_reset_table);
    auto table = rd_tr->get_table(reset_store->m_pending_reset_table);

    if (!table || table->size() == 0) {
        return std::nullopt;
    }
    if (table->size() > 1) {
        // this may happen if a future version of this code changes the format and expectations around reset metadata.
        throw ClientResetFailed(
            util::format("Previous client resets detected (%1) but only one is expected.", table->size()));
    }
    auto reset_entry = *table->begin();
    PendingReset pending;
    pending.time = reset_entry.get<Timestamp>(reset_store->m_timestamp);
    pending.mode = to_resync_mode(reset_entry.get<int64_t>(reset_store->m_recovery_mode));
    pending.action = to_reset_action(reset_entry.get<int64_t>(reset_store->m_action));
    auto error_code = reset_entry.get<int64_t>(reset_store->m_error_code);
    if (error_code > 0) {
        pending.error = Status(static_cast<ErrorCodes::Error>(error_code),
                               reset_entry.get<StringData>(reset_store->m_error_message));
    }
    return pending;
}

void PendingResetStore::track_reset(const TransactionRef& wr_tr, ClientResyncMode mode, PendingReset::Action action,
                                    const std::optional<Status>& error)
{
    REALM_ASSERT(mode != ClientResyncMode::Manual);
    // Write transaction required
    REALM_ASSERT(wr_tr->get_transact_stage() == DB::TransactStage::transact_Writing);
    if (auto table = wr_tr->get_table(s_meta_reset_table_name); table && table->size() > 0) {
        // this may happen if a future version of this code changes the format and expectations around reset
        // metadata.
        throw ClientResetFailed(
            util::format("Previous client resets detected (%1) but only one is expected.", table->size()));
    }
    auto reset_store = PendingResetStore::load_or_create_schema(wr_tr);

    REALM_ASSERT(reset_store.m_pending_reset_table);
    auto table = wr_tr->get_table(reset_store.m_pending_reset_table);
    REALM_ASSERT(table);
    // Create the new object
    auto obj = table->create_object_with_primary_key(
        ObjectId::gen(), {
                             {reset_store.m_timestamp, Timestamp(std::chrono::system_clock::now())},
                             {reset_store.m_recovery_mode, from_resync_mode(mode)},
                             {reset_store.m_action, from_reset_action(action)},
                         });
    // Add the error, if provided
    if (error) {
        obj.set(reset_store.m_error_code, static_cast<int64_t>(error->code()));
        obj.set(reset_store.m_error_message, error->reason());
    }
    // Don't commit at the end - allow the caller to do it when they are ready
}

PendingResetStore::PendingResetStore(const TransactionRef& rd_tr)
    : m_internal_tables{
          {&m_pending_reset_table,
           s_meta_reset_table_name,
           {&m_id, s_pk_col_name, type_ObjectId},
           {
               {&m_timestamp, s_timestamp_col_name, type_Timestamp},
               {&m_recovery_mode, s_reset_recovery_mode_col_name, type_Int},
               {&m_action, s_reset_action_col_name, type_Int},
               {&m_error_code, s_reset_error_code_col_name, type_Int, true},
               {&m_error_message, s_reset_error_msg_col_name, type_String, true},
           }},
      }
{
    // Works with read, write, and frozen transactions
    SyncMetadataSchemaVersionsReader schema_versions(rd_tr);
    auto schema_version = schema_versions.get_version_for(rd_tr, internal_schema_groups::c_pending_reset_store);

    // Load the metadata schema info if a schema version was found
    if (schema_version) {
        if (*schema_version != s_pending_reset_version) {
            // Unsupported schema version
            throw RuntimeError(ErrorCodes::UnsupportedFileFormatVersion,
                               "Found invalid schema version for existing client reset cycle tracking metadata");
        }
        load_sync_metadata_schema(rd_tr, &m_internal_tables);
        if (m_pending_reset_table) {
            // If the schema info was read, then store the schema version
            m_schema_version = schema_version;
        }
    }
}

std::optional<PendingResetStore> PendingResetStore::load_schema(const TransactionRef& rd_tr)
{
    PendingResetStore reset_store(rd_tr);
    if (reset_store.m_schema_version) {
        return reset_store;
    }
    return std::nullopt;
}

PendingResetStore PendingResetStore::load_or_create_schema(const TransactionRef& wr_tr)
{
    PendingResetStore reset_store(wr_tr);
    if (reset_store.m_schema_version) {
        // If the schema metadata was found, return the initialized class
        return reset_store;
    }
    // Otherwise, set it up from scratch - Make sure the transaction is set for writing
    if (wr_tr->get_transact_stage() == DB::TransactStage::transact_Reading) {
        wr_tr->promote_to_write();
    }
    // Ensure writing - all other transaction stages are not allowed
    REALM_ASSERT_EX(wr_tr->get_transact_stage() == DB::TransactStage::transact_Writing, wr_tr->get_transact_stage());

    // Drop the old table and any stale pending resets
    if (wr_tr->has_table(s_meta_reset_table_name)) {
        wr_tr->remove_table(s_meta_reset_table_name);
    }

    // Ensure the schema versions table is initialized (may add its own commit)
    SyncMetadataSchemaVersions schema_versions(wr_tr);
    // Create the metadata schema and set the version (in the same commit)
    schema_versions.set_version_for(wr_tr, internal_schema_groups::c_pending_reset_store, s_pending_reset_version);
    create_sync_metadata_schema(wr_tr, &reset_store.m_internal_tables);
    REALM_ASSERT(reset_store.m_pending_reset_table);
    reset_store.m_schema_version = s_pending_reset_version;

    // Don't commit yet
    return reset_store;
}

std::optional<PendingReset> PendingResetStore::read_legacy_pending_reset(const TransactionRef& rd_tr)
{
    // Try to read the pending reset info from v1 of the schema
    constexpr static std::string_view s_v1_version_column_name("version");
    constexpr static std::string_view s_v1_timestamp_col_name("event_time");
    constexpr static std::string_view s_v1_reset_mode_col_name("type_of_reset");

    // Check for pending reset v1 - does not use schema version
    TableRef table = rd_tr->get_table(s_meta_reset_table_name);
    if (table && table->size() > 0) {
        ColKey version_col = table->get_column_key(s_v1_version_column_name);
        ColKey timestamp_col = table->get_column_key(s_v1_timestamp_col_name);
        ColKey mode_col = table->get_column_key(s_v1_reset_mode_col_name);
        Obj reset_entry = *table->begin();

        if (version_col && reset_entry.get<int64_t>(version_col) == 1LL) {
            REALM_ASSERT(timestamp_col);
            REALM_ASSERT(mode_col);
            PendingReset pending;
            pending.time = reset_entry.get<Timestamp>(timestamp_col);
            pending.mode = to_resync_mode(reset_entry.get<int64_t>(mode_col));
            // Create a fake action depending on the resync mode
            pending.action = pending.mode == ClientResyncMode::DiscardLocal
                                 ? sync::ProtocolErrorInfo::Action::ClientResetNoRecovery
                                 : sync::ProtocolErrorInfo::Action::ClientReset;
            return pending;
        }
    }
    // Add checking for future schema versions here
    return std::nullopt;
}

int64_t PendingResetStore::from_reset_action(PendingReset::Action action)
{
    switch (action) {
        case PendingReset::Action::ClientReset:
            return 1;
        case PendingReset::Action::ClientResetNoRecovery:
            return 2;
        case PendingReset::Action::MigrateToFLX:
            return 3;
        case PendingReset::Action::RevertToPBS:
            return 4;
        default:
            throw ClientResetFailed(util::format("Unsupported client reset action: %1 for pending reset", action));
    }
}

PendingReset::Action PendingResetStore::to_reset_action(int64_t action)
{
    switch (action) {
        case 1:
            return PendingReset::Action::ClientReset;
        case 2:
            return PendingReset::Action::ClientResetNoRecovery;
        case 3:
            return PendingReset::Action::MigrateToFLX;
        case 4:
            return PendingReset::Action::RevertToPBS;
        default:
            return PendingReset::Action::NoAction;
    }
}

ClientResyncMode PendingResetStore::to_resync_mode(int64_t mode)
{
    // Retains compatibility with v1
    // RecoverOrDiscard is treated as Recover and is not stored
    switch (mode) {
        case 0: // DiscardLocal
            return ClientResyncMode::DiscardLocal;
        case 1: // Recover
            return ClientResyncMode::Recover;
        default:
            throw ClientResetFailed(util::format("Unsupported client reset resync mode: %1 for pending reset", mode));
    }
}

int64_t PendingResetStore::from_resync_mode(ClientResyncMode mode)
{
    // Retains compatibility with v1
    switch (mode) {
        case ClientResyncMode::DiscardLocal:
            return 0; // DiscardLocal
        case ClientResyncMode::RecoverOrDiscard:
            [[fallthrough]]; // RecoverOrDiscard is treated as Recover
        case ClientResyncMode::Recover:
            return 1; // Recover
        default:
            throw ClientResetFailed(util::format("Unsupported client reset resync mode: %1 for pending reset", mode));
    }
}

} // namespace realm::sync
