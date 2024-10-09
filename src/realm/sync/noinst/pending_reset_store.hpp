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

#ifndef REALM_NOINST_PENDING_RESET_STORE_HPP
#define REALM_NOINST_PENDING_RESET_STORE_HPP

#include <realm/status.hpp>
#include <realm/timestamp.hpp>
#include <realm/sync/client_base.hpp>
#include <realm/sync/config.hpp>
#include <realm/sync/protocol.hpp>

#include <realm/sync/noinst/sync_metadata_schema.hpp>

#include <ostream>
#include <optional>

namespace realm::sync {

struct PendingReset {
    using Action = sync::ProtocolErrorInfo::Action;
    Timestamp time;
    ClientResyncMode mode;
    Action action = Action::NoAction;
    Status error = Status::OK();
};

std::ostream& operator<<(std::ostream& os, const sync::PendingReset& pr);
bool operator==(const sync::PendingReset& lhs, const sync::PendingReset& rhs);
bool operator==(const sync::PendingReset& lhs, const PendingReset::Action& action);

class PendingResetStore {
public:
    // Store the pending reset tracking information. Any pre-existing tracking
    // will be deleted and replaced with this.
    // Requires a writable transaction and changes must be committed manually
    static void track_reset(Group& group, ClientResyncMode mode, PendingReset::Action action, Status error);
    // Record the version of the final recovered changeset that must be uploaded
    // for a client reset to be complete. Not called for DiscardLocal or if there
    // was nothing to recover.
    static void set_recovered_version(Group&, version_type);
    // Clear the pending reset tracking information, if it exists
    // Requires a writable transaction and changes must be committed manually
    static void clear_pending_reset(Group& group);
    // Remove the pending reset tracking information if it exists and the version
    // set with set_recovered_version() is less than or equal to version.
    static void remove_if_complete(Group& group, version_type version, util::Logger&);
    static std::optional<PendingReset> has_pending_reset(const Group& group);

private:
    // The instantiated class is only used internally
    PendingResetStore(const Group& group);

    std::vector<SyncMetadataTable> m_internal_tables;
    TableKey m_pending_reset_table;
    ColKey m_core_version;
    ColKey m_recovered_version;
    ColKey m_timestamp;
    ColKey m_recovery_mode;
    ColKey m_action;
    ColKey m_error_code;
    ColKey m_error_message;

    // Returns true if the schema was loaded
    static std::optional<PendingResetStore> load_schema(const Group& group);
    // Loads the schema or creates it if it doesn't exist
    // Requires a writable transaction and changes must be committed manually
    static PendingResetStore load_or_create_schema(Group& group);
};

} // namespace realm::sync

#endif // REALM_NOINST_PENDING_RESET_STORE_HPP
