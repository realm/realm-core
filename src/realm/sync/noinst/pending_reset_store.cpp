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
    os << " for error: " << pr.error;
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
constexpr static std::string_view s_version_col_name("core_version");
constexpr static std::string_view s_timestamp_col_name("time");
constexpr static std::string_view s_reset_recovery_mode_col_name("mode");
constexpr static std::string_view s_reset_action_col_name("action");
constexpr static std::string_view s_reset_error_code_col_name("error_code");
constexpr static std::string_view s_reset_error_msg_col_name("error_msg");

void PendingResetStore::clear_pending_reset(Group& group)
{
    if (auto table = group.get_table(s_meta_reset_table_name); table && !table->is_empty()) {
        table->clear();
    }
}

std::optional<PendingReset> PendingResetStore::has_pending_reset(const Group& group)
{
    auto reset_store = PendingResetStore::load_schema(group);
    if (!reset_store) {
        // Table hasn't been created yet (or has the wrong schema)
        return std::nullopt;
    }
    REALM_ASSERT(reset_store->m_pending_reset_table);
    auto table = group.get_table(reset_store->m_pending_reset_table);

    if (!table || table->size() != 1) {
        return std::nullopt;
    }
    auto reset_entry = *table->begin();
    if (reset_entry.get<String>(reset_store->m_version) != REALM_VERSION_STRING) {
        // Previous pending reset was written by a different version, so ignore it
        return std::nullopt;
    }

    PendingReset pending;
    pending.time = reset_entry.get<Timestamp>(reset_store->m_timestamp);
    pending.mode = to_resync_mode(reset_entry.get<int64_t>(reset_store->m_recovery_mode));
    pending.action = to_reset_action(reset_entry.get<int64_t>(reset_store->m_action));
    auto error_code = reset_entry.get<int64_t>(reset_store->m_error_code);
    if (error_code != 0) {
        pending.error = Status(static_cast<ErrorCodes::Error>(error_code),
                               reset_entry.get<StringData>(reset_store->m_error_message));
    }
    return pending;
}

void PendingResetStore::track_reset(Group& group, ClientResyncMode mode, PendingReset::Action action, Status error)
{
    REALM_ASSERT(mode != ClientResyncMode::Manual);
    auto reset_store = PendingResetStore::load_or_create_schema(group);

    REALM_ASSERT(reset_store.m_pending_reset_table);
    auto table = group.get_table(reset_store.m_pending_reset_table);
    REALM_ASSERT(table);
    table->clear();
    table->create_object(null_key, {
                                       {reset_store.m_version, Mixed(REALM_VERSION_STRING)},
                                       {reset_store.m_timestamp, Timestamp(std::chrono::system_clock::now())},
                                       {reset_store.m_recovery_mode, from_resync_mode(mode)},
                                       {reset_store.m_action, from_reset_action(action)},
                                       {reset_store.m_error_code, static_cast<int64_t>(error.code())},
                                       {reset_store.m_error_message, error.reason()},
                                   });
}

PendingResetStore::PendingResetStore(const Group& g)
    : m_internal_tables{
          {&m_pending_reset_table,
           s_meta_reset_table_name,
           {
               {&m_version, s_version_col_name, type_String},
               {&m_timestamp, s_timestamp_col_name, type_Timestamp},
               {&m_recovery_mode, s_reset_recovery_mode_col_name, type_Int},
               {&m_action, s_reset_action_col_name, type_Int},
               {&m_error_code, s_reset_error_code_col_name, type_Int},
               {&m_error_message, s_reset_error_msg_col_name, type_String},
           }},
      }
{
    if (!try_load_sync_metadata_schema(g, &m_internal_tables).is_ok()) {
        m_pending_reset_table = {};
    }
}

std::optional<PendingResetStore> PendingResetStore::load_schema(const Group& group)
{
    if (PendingResetStore reset_store(group); reset_store.m_pending_reset_table) {
        return reset_store;
    }
    return std::nullopt;
}

PendingResetStore PendingResetStore::load_or_create_schema(Group& group)
{
    PendingResetStore reset_store(group);
    if (!reset_store.m_pending_reset_table) {
        // If the table exists but has the wrong schema just drop it
        if (group.has_table(s_meta_reset_table_name)) {
            group.remove_table(s_meta_reset_table_name);
        }

        // Create the table with the correct schema
        create_sync_metadata_schema(group, &reset_store.m_internal_tables);
    }
    return reset_store;
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
