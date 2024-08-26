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
#include "realm/sync/transform.hpp"
#include "realm/util/buffer.hpp"
#include "realm/util/logger.hpp"
#include "realm/util/optional.hpp"
#include "realm/util/span.hpp"
#include <stdexcept>

namespace realm::sync {

class SubscriptionStore;

class PendingBootstrapException : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The PendingBootstrapStore is used internally by the sync client to store changesets from FLX sync bootstraps
// that are sent across multiple download messages.
class PendingBootstrapStore {
public:
    // Constructs from a DBRef.
    //
    // Underneath this creates a table which stores each download message's changesets.
    PendingBootstrapStore(DBRef db, util::Logger& logger, std::shared_ptr<SubscriptionStore> subscription_store);

    PendingBootstrapStore(const PendingBootstrapStore&) = delete;
    PendingBootstrapStore& operator=(const PendingBootstrapStore&) = delete;

    // True if there are pending changesets to process.
    bool has_pending() const noexcept;

    struct PendingBatch {
        int64_t query_version = 0;
        std::vector<RemoteChangeset> changesets;
        std::vector<util::AppendBuffer<char>> changeset_data;
        util::Optional<SyncProgress> progress;
        size_t remaining_changesets = 0;
    };

    // Returns the next batch (download message) of changesets if it exists. The transaction must be in the reading
    // state.
    PendingBatch peek_pending(Transaction& tr, size_t limit_in_bytes);

    struct PendingBatchStats {
        int64_t query_version = 0;
        size_t pending_changesets = 0;
        size_t pending_changeset_bytes = 0;
    };
    PendingBatchStats pending_stats();

    // Removes the first set of changesets from the current pending bootstrap batch. The transaction must be in the
    // writing state.
    void pop_front_pending(const Transaction& tr, size_t count);

    // Adds a set of changesets to the store.
    void add_batch(int64_t query_version, util::Optional<SyncProgress> progress,
                   DownloadableProgress download_progress, const std::vector<RemoteChangeset>& changesets);

    void clear(Transaction& wt, int64_t query_version);

private:
    DBRef m_db;
    // The pending_bootstrap_store is tied to the lifetime of a session, so a shared_ptr is not needed
    util::Logger& m_logger;
    std::shared_ptr<SubscriptionStore> m_subscription_store;

    TableKey m_cursor_table;

    TableKey m_table;
    ColKey m_changesets;
    ColKey m_query_version;
    ColKey m_progress;

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

    bool m_has_pending = false;
};

} // namespace realm::sync
