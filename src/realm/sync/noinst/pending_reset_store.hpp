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

class PendingResetStore;

using PendingResetStoreRef = std::shared_ptr<PendingResetStore>;

struct PendingReset {
    using Action = sync::ProtocolErrorInfo::Action;
    Timestamp time;
    ClientResyncMode mode;
    Action action = Action::NoAction;
    std::optional<Status> error;
};

std::ostream& operator<<(std::ostream& os, const sync::PendingReset& pr);
bool operator==(const sync::PendingReset& lhs, const sync::PendingReset& rhs);

class PendingResetStore {
    struct Private {};

public:
    static PendingResetStoreRef create(DBRef db);
    PendingResetStore(Private, DBRef db);
    ~PendingResetStore() = default;

    void track_reset(ClientResyncMode mode, PendingReset::Action action,
                     const std::optional<Status>& error = std::nullopt);
    void clear_pending_reset();
    util::Optional<PendingReset> has_pending_reset();

    static int64_t from_reset_action(PendingReset::Action action);
    static PendingReset::Action to_reset_action(int64_t action);
    static ClientResyncMode to_resync_mode(int64_t mode);
    static int64_t from_resync_mode(ClientResyncMode mode);

private:
    // Returns true if the schema was loaded
    bool load_schema(const TransactionRef& rd_tr);
    void create_schema(const TransactionRef& rd_tr);

    std::optional<PendingReset> read_legacy_pending_reset(const TransactionRef& rd_tr);
    std::optional<PendingReset> has_v1_pending_reset(const TableRef& table);

    DBRef m_db;
    std::vector<SyncMetadataTable> m_internal_tables;

    std::optional<int64_t> m_schema_version = std::nullopt;
    TableKey m_pending_reset_table;
    ColKey m_id;
    ColKey m_version;
    ColKey m_timestamp;
    ColKey m_recovery_mode;
    ColKey m_action;
    ColKey m_error_code;
    ColKey m_error_message;
};

} // namespace realm::sync

#endif // REALM_NOINST_PENDING_RESET_STORE_HPP
