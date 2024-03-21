/*************************************************************************
 *
 * Copyright 2022 Realm, Inc.
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

#pragma once

#include "realm/db.hpp"
#include "realm/list.hpp"
#include "realm/obj.hpp"
#include "realm/object-store/c_api/util.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/noinst/protocol_codec.hpp"
#include "realm/sync/transform.hpp"
#include "realm/util/buffer.hpp"
#include "realm/util/logger.hpp"
#include "realm/util/optional.hpp"
#include "realm/util/span.hpp"
#include <stdexcept>

namespace realm::sync {

class PendingBootstrapException : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The PendingBootstrapStore is used internally by the sync client to store changesets from FLX sync bootstraps
// that are sent across multiple download messages.
class PendingBootstrapStore {
public:
    // Constructs from a DBRef. Constructing is destructive - since pending bootstraps are only valid for the
    // session they occurred in, this will drop/clear all data when the bootstrap store is constructed.
    //
    // Underneath this creates a table which stores each download message's changesets.
    explicit PendingBootstrapStore(DBRef db, util::Logger& logger);

    PendingBootstrapStore(const PendingBootstrapStore&) = delete;
    PendingBootstrapStore& operator=(const PendingBootstrapStore&) = delete;

    // True if the current bootstrap entry has received at least one bootstrap message
    bool has_pending();

    // True if a complete bootstrap entry (i.e. progress has been set) for the current bootstrap entry
    bool bootstrap_complete();

    struct PendingBatch {
        int64_t query_version = 0;
        int64_t remote_version = 0;
        std::vector<RemoteChangeset> changesets;
        std::vector<util::AppendBuffer<char>> changeset_data;
        util::Optional<SyncProgress> progress;
        size_t remaining_changesets = 0;
    };

    // Returns the next batch (download message) of changesets if it exists. The transaction must be in the reading
    // state.
    PendingBatch peek_pending(size_t limit_in_bytes);

    std::optional<int64_t> remote_version();
    std::optional<int64_t> query_version();

    struct PendingBatchStats {
        int64_t query_version = 0;
        int64_t remote_version = 0;
        bool complete = false;
        size_t pending_changesets = 0;
        size_t pending_changeset_bytes = 0;
    };
    PendingBatchStats pending_stats();

    // Removes the first set of changesets from the current pending bootstrap batch. The transaction must be in the
    // writing state.
    void pop_front_pending(const TransactionRef& tr, size_t count);

    // Adds a set of changesets to the store - returns false if the changesets were not added
    bool add_batch(int64_t query_version, int64_t remote_version, util::Optional<SyncProgress> progress,
                   const std::vector<RemoteChangeset>& changesets, bool* created_new_batch);

    void clear();

private:
    void reset_state();

    DBRef m_db;
    // The pending_bootstrap_store is tied to the lifetime of a session, so a shared_ptr is not needed
    util::Logger& m_logger;
    _impl::ClientProtocol m_client_protocol;

    TableKey m_cursor_table;

    TableKey m_table;
    ColKey m_changesets_col;
    ColKey m_query_version_col;
    ColKey m_progress_col;

    TableKey m_progress_table;
    ColKey m_progress_download_server_version;
    ColKey m_progress_download_client_version;
    ColKey m_progress_upload_server_version;
    ColKey m_progress_upload_client_version;
    ColKey m_progress_latest_server_version;
    ColKey m_progress_latest_server_version_salt;

    TableKey m_changeset_table;
    ColKey m_changeset_remote_version;
    ColKey m_changeset_last_integrated_client_version;
    ColKey m_changeset_origin_file_ident;
    ColKey m_changeset_origin_timestamp;
    ColKey m_changeset_original_changeset_size;
    ColKey m_changeset_data;

    // Cached values for the current stored bootstrap
    bool m_has_pending = false;   // Stored bootstrap entry is in progress
    bool m_is_complete = false;   // Stored bootstrap entry received all data
    int64_t m_query_version = 0;  // Query version of bootstrap in progress
    int64_t m_remote_version = 0; // Remote version of bootstrap in progress
};

} // namespace realm::sync
