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

#include "realm/sync/noinst/pending_bootstrap_store.hpp"

#include "realm/binary_data.hpp"
#include "realm/chunked_binary.hpp"
#include "realm/data_type.hpp"
#include "realm/db.hpp"
#include "realm/list.hpp"
#include "realm/query.hpp"
#include "realm/sync/changeset_parser.hpp"
#include "realm/sync/noinst/protocol_codec.hpp"
#include "realm/sync/noinst/sync_metadata_schema.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/transform.hpp"
#include "realm/util/assert.hpp"
#include "realm/util/buffer.hpp"
#include "realm/util/compression.hpp"
#include "realm/util/logger.hpp"
#include <stdexcept>

namespace realm::sync {
namespace {
constexpr static int c_schema_version = 1;
constexpr static std::string_view c_progress_table("flx_pending_bootstrap_progress");
constexpr static std::string_view c_pending_bootstrap_table("flx_pending_bootstrap");
constexpr static std::string_view c_pending_changesets_table("flx_pending_bootstrap_changesets");
constexpr static std::string_view c_pending_bootstrap_query_version("query_version");
constexpr static std::string_view c_pending_bootstrap_changesets("changesets");
constexpr static std::string_view c_pending_bootstrap_progress("progress");
constexpr static std::string_view c_pending_changesets_remote_version("remote_version");
constexpr static std::string_view
    c_pending_changesets_last_integrated_client_version("last_integrated_client_version");
constexpr static std::string_view c_pending_changesets_origin_file_ident("origin_file_ident");
constexpr static std::string_view c_pending_changesets_origin_timestamp("origin_timestamp");
constexpr static std::string_view c_pending_changesets_original_size("original_size");
constexpr static std::string_view c_pending_changesets_data("data");
constexpr static std::string_view c_progress_download_server_version("download_server_version");
constexpr static std::string_view c_progress_download_client_version("download_client_version");
constexpr static std::string_view c_progress_upload_server_version("upload_server_version");
constexpr static std::string_view c_progress_upload_client_version("upload_client_version");
constexpr static std::string_view c_progress_latest_server_version("latest_server_version");
constexpr static std::string_view c_progress_latest_server_version_salt("latest_server_version_salt");

} // namespace

PendingBootstrapStore::PendingBootstrapStore(DBRef db, util::Logger& logger)
    : m_db(std::move(db))
    , m_logger(logger)
{
    std::vector<SyncMetadataTable> internal_tables{
        {&m_table,
         c_pending_bootstrap_table,
         {&m_query_version_col, c_pending_bootstrap_query_version, type_Int},
         {
             {&m_changesets_col, c_pending_bootstrap_changesets, c_pending_changesets_table, true},
             {&m_progress_col, c_pending_bootstrap_progress, c_progress_table, false},
         }},
        {&m_progress_table,
         c_progress_table,
         SyncMetadataTable::IsEmbeddedTag{},
         {
             {&m_progress_upload_server_version, c_progress_upload_server_version, type_Int},
             {&m_progress_upload_client_version, c_progress_upload_client_version, type_Int},
             {&m_progress_download_server_version, c_progress_download_server_version, type_Int},
             {&m_progress_download_client_version, c_progress_download_client_version, type_Int},
             {&m_progress_latest_server_version, c_progress_latest_server_version, type_Int},
             {&m_progress_latest_server_version_salt, c_progress_latest_server_version_salt, type_Int},
         }},
        {&m_changeset_table,
         c_pending_changesets_table,
         SyncMetadataTable::IsEmbeddedTag{},
         {
             {&m_changeset_remote_version, c_pending_changesets_remote_version, type_Int},
             {&m_changeset_last_integrated_client_version, c_pending_changesets_last_integrated_client_version,
              type_Int},
             {&m_changeset_origin_file_ident, c_pending_changesets_origin_file_ident, type_Int},
             {&m_changeset_origin_timestamp, c_pending_changesets_origin_timestamp, type_Int},
             {&m_changeset_original_changeset_size, c_pending_changesets_original_size, type_Int},
             {&m_changeset_data, c_pending_changesets_data, type_Binary, true},
         }}};

    auto tr = m_db->start_read();
    SyncMetadataSchemaVersions schema_versions(tr);
    if (auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_pending_bootstraps)) {
        if (*schema_version != c_schema_version) {
            throw RuntimeError(ErrorCodes::SchemaVersionMismatch,
                               "Invalid schema version for FLX sync pending bootstrap table group");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }
    else {
        tr->promote_to_write();
        create_sync_metadata_schema(tr, &internal_tables);
        schema_versions.set_version_for(tr, internal_schema_groups::c_pending_bootstraps, c_schema_version);
        tr->commit_and_continue_as_read();
    }

    if (auto bootstrap_table = tr->get_table(m_table); !bootstrap_table->is_empty()) {
        auto bootstrap_obj = bootstrap_table->get_object(0);
        auto changeset_list = bootstrap_obj.get_linklist(m_changesets_col);

        if (!bootstrap_obj.is_null(m_progress_col) && !changeset_list.is_empty()) {
            m_has_pending = true;
            m_is_complete = true;
            m_query_version = bootstrap_obj.get<int64_t>(m_query_version_col);
            // All changesets should have the same remote_version value
            auto cur_changeset = changeset_list.get_object(0);
            m_remote_version = cur_changeset.get<int64_t>(m_changeset_remote_version);
        }
        else {
            // If the object is not complete, then clear the bootstrap store to remove stale bootstraps
            clear();
        }
    }
}

bool PendingBootstrapStore::add_batch(int64_t query_version, int64_t remote_version,
                                      util::Optional<SyncProgress> progress,
                                      const _impl::ClientProtocol::ReceivedChangesets& changesets,
                                      bool* created_new_batch_out)
{
    std::vector<util::AppendBuffer<char>> compressed_changesets;
    if (!changesets.empty()) {
        compressed_changesets.reserve(changesets.size());

        // Compress the changeset data being stored in the boostrap store
        util::compression::CompressMemoryArena arena;
        for (auto& changeset : changesets) {
            if (static_cast<int64_t>(changeset.remote_version) != remote_version) {
                m_logger.info(util::LogCategory::changeset,
                              "Not a bootstrap message: not all changesets have the same remote version");
                return false;
            }
            compressed_changesets.emplace_back();
            util::compression::allocate_and_compress_nonportable(arena, {changeset.data.get_first_chunk()},
                                                                 compressed_changesets.back());
        }
    }

    auto tr = m_db->start_write();
    auto bootstrap_table = tr->get_table(m_table);
    // Delete any stale or incomplete bootstrap entries
    auto incomplete_bootstraps = Query(bootstrap_table).not_equal(m_query_version_col, query_version).find_all();
    incomplete_bootstraps.for_each([&](Obj obj) {
        bool incomplete = obj.is_null(m_progress_col);
        m_logger.debug(util::LogCategory::changeset, "Clearing old %1 bootstrap for query version %2",
                       incomplete ? "incomplete" : "complete", obj.get<int64_t>(m_query_version_col));
        return IteratorControl::AdvanceToNext;
    });
    incomplete_bootstraps.clear();

    bool did_create = false;
    // Create or get the table entry for the specified query_version
    auto bootstrap_obj = bootstrap_table->create_object_with_primary_key(Mixed{query_version}, &did_create);
    auto changeset_list = bootstrap_obj.get_linklist(m_changesets_col);
    size_t total_changesets = 0;

    if (did_create) {
        m_remote_version = remote_version;
        m_query_version = query_version;
    }
    // If the bootstrap entry exists for this query_version, but is empty,
    // then just update the remote_version; no need to create a new entry
    else if (bootstrap_obj.is_null(m_progress_col) && changeset_list.is_empty()) {
        m_remote_version = remote_version;
    }
    else {
        // If the progress object has already been populated, then the previous bootstrap entry
        // was completely downloaded and future adds are not allowed, start over with a new entry
        // Also, if a table entry already exists for this query_version, but the remote_version
        // does not match, start over with a new entry.
        if (bool incomplete = bootstrap_obj.is_null(m_progress_col);
            !incomplete || remote_version != m_remote_version) {
            auto log_level = incomplete ? util::Logger::Level::debug : util::Logger::Level::error;
            m_logger.log(util::LogCategory::changeset, log_level,
                         "Clearing old %1 bootstrap entry for version: query %2 / remote %3",
                         incomplete ? "incomplete" : "complete", m_query_version, m_remote_version);
            bootstrap_obj.remove();
            reset_state();
            bootstrap_obj = bootstrap_table->create_object_with_primary_key(Mixed{query_version}, &did_create);
            REALM_ASSERT_EX(did_create, "Pending Bootstrap entry creation failed");
            changeset_list = bootstrap_obj.get_linklist(m_changesets_col);
            m_remote_version = remote_version;
            m_query_version = query_version;
        }
    }

    // At this point the provided versions should match the cached versions
    REALM_ASSERT_3(remote_version, ==, m_remote_version);
    REALM_ASSERT_3(query_version, ==, m_query_version);
    m_has_pending = true; // Bootstrap entry is in progress

    // If a progress object is provided (i.e. this is the last bootstrap message), then save it
    if (progress) {
        REALM_ASSERT_3(remote_version, ==, static_cast<int64_t>(progress->download.server_version));
        auto progress_obj = bootstrap_obj.create_and_set_linked_object(m_progress_col);
        progress_obj.set(m_progress_latest_server_version, int64_t(progress->latest_server_version.version));
        progress_obj.set(m_progress_latest_server_version_salt, int64_t(progress->latest_server_version.salt));
        progress_obj.set(m_progress_download_server_version, int64_t(progress->download.server_version));
        progress_obj.set(m_progress_download_client_version,
                         int64_t(progress->download.last_integrated_client_version));
        progress_obj.set(m_progress_upload_server_version, int64_t(progress->upload.last_integrated_server_version));
        progress_obj.set(m_progress_upload_client_version, int64_t(progress->upload.client_version));
        // bootstrap is finalized
        m_is_complete = true;
    }

    // Add the compressed changeset data to the bootstrap entry.
    for (size_t idx = 0; idx < changesets.size(); ++idx) {
        auto cur_changeset = changeset_list.create_and_insert_linked_object(changeset_list.size());
        cur_changeset.set(m_changeset_remote_version, int64_t(changesets[idx].remote_version));
        cur_changeset.set(m_changeset_last_integrated_client_version,
                          int64_t(changesets[idx].last_integrated_local_version));
        cur_changeset.set(m_changeset_origin_file_ident, int64_t(changesets[idx].origin_file_ident));
        cur_changeset.set(m_changeset_origin_timestamp, int64_t(changesets[idx].origin_timestamp));
        cur_changeset.set(m_changeset_original_changeset_size, int64_t(changesets[idx].original_changeset_size));
        BinaryData compressed_data(compressed_changesets[idx].data(), compressed_changesets[idx].size());
        cur_changeset.set(m_changeset_data, compressed_data);
    }
    total_changesets = changeset_list.size();

    tr->commit();

    if (created_new_batch_out) {
        *created_new_batch_out = did_create;
    }

    if (did_create) {
        m_logger.debug(util::LogCategory::changeset,
                       "Created new pending bootstrap object with %1 changesets for version: query %2 / remote %3",
                       total_changesets, query_version, remote_version);
    }
    else {
        m_logger.debug(
            util::LogCategory::changeset,
            "Added batch of %1 changesets (%2 total) to pending bootstrap object for version: query %3 / remote %4",
            changesets.size(), total_changesets, query_version, remote_version);
    }
    if (progress) {
        m_logger.debug(util::LogCategory::changeset,
                       "Finalized pending bootstrap object with %1 changesets for version: query %2 / remote %3",
                       total_changesets, query_version, remote_version);
    }

    return true;
}

bool PendingBootstrapStore::has_pending()
{
    return m_has_pending;
}

bool PendingBootstrapStore::bootstrap_complete()
{
    return m_is_complete;
}

std::optional<int64_t> PendingBootstrapStore::remote_version()
{
    if (m_remote_version == 0) {
        return std::nullopt;
    }
    else {
        return m_remote_version;
    }
}

std::optional<int64_t> PendingBootstrapStore::query_version()
{
    if (m_query_version == 0) {
        return std::nullopt;
    }
    else {
        return m_query_version;
    }
}

void PendingBootstrapStore::clear()
{
    auto tr = m_db->start_read();
    auto bootstrap_table = tr->get_table(m_table);
    // Just make sure the state is reset if the bootstrap table is empty
    if (bootstrap_table->is_empty()) {
        reset_state();
        return;
    }
    tr->promote_to_write();
    bootstrap_table->clear();
    reset_state();
    tr->commit();
}

PendingBootstrapStore::PendingBatch PendingBootstrapStore::peek_pending(size_t limit_in_bytes)
{
    auto tr = m_db->start_read();
    auto bootstrap_table = tr->get_table(m_table);
    if (bootstrap_table->is_empty()) {
        REALM_ASSERT(!m_has_pending);
        REALM_ASSERT(!m_is_complete);
        return {};
    }

    // We should only have one pending bootstrap at a time.
    REALM_ASSERT(bootstrap_table->size() == 1);

    auto bootstrap_obj = bootstrap_table->get_object(0);
    auto query_version = bootstrap_obj.get<int64_t>(m_query_version_col);
    REALM_ASSERT_3(query_version, ==, m_query_version);
    PendingBatch ret;
    ret.query_version = query_version;
    ret.remote_version = m_remote_version;

    if (!bootstrap_obj.is_null(m_progress_col)) {
        REALM_ASSERT(m_is_complete);
        auto progress_obj = bootstrap_obj.get_linked_object(m_progress_col);
        SyncProgress progress;
        progress.latest_server_version.version = progress_obj.get<int64_t>(m_progress_latest_server_version);
        progress.latest_server_version.salt = progress_obj.get<int64_t>(m_progress_latest_server_version_salt);
        progress.download.server_version = progress_obj.get<int64_t>(m_progress_download_server_version);
        progress.download.last_integrated_client_version =
            progress_obj.get<int64_t>(m_progress_download_client_version);
        progress.upload.last_integrated_server_version = progress_obj.get<int64_t>(m_progress_upload_server_version);
        progress.upload.client_version = progress_obj.get<int64_t>(m_progress_upload_client_version);
        ret.progress = std::move(progress);
    }

    auto changeset_list = bootstrap_obj.get_linklist(m_changesets_col);
    size_t bytes_so_far = 0;
    for (size_t idx = 0; idx < changeset_list.size() && bytes_so_far < limit_in_bytes; ++idx) {
        auto cur_changeset = changeset_list.get_object(idx);
        // Verify the remote versions match
        auto remote_version = cur_changeset.get<int64_t>(m_changeset_remote_version);
        REALM_ASSERT_3(remote_version, ==, m_remote_version);

        ret.changeset_data.push_back(util::AppendBuffer<char>());
        auto& uncompressed_buffer = ret.changeset_data.back();

        auto compressed_changeset_data = cur_changeset.get<BinaryData>(m_changeset_data);
        ChunkedBinaryInputStream changeset_is(compressed_changeset_data);
        auto ec = util::compression::decompress_nonportable(changeset_is, uncompressed_buffer);
        if (ec == util::compression::error::decompress_unsupported) {
            REALM_TERMINATE(
                "Synchronized Realm files with unprocessed pending bootstraps cannot be copied between platforms.");
        }
        REALM_ASSERT_3(ec, ==, std::error_code{});

        RemoteChangeset parsed_changeset;
        parsed_changeset.original_changeset_size =
            static_cast<size_t>(cur_changeset.get<int64_t>(m_changeset_original_changeset_size));
        parsed_changeset.origin_timestamp = cur_changeset.get<int64_t>(m_changeset_origin_timestamp);
        parsed_changeset.origin_file_ident = cur_changeset.get<int64_t>(m_changeset_origin_file_ident);
        parsed_changeset.remote_version = remote_version;
        parsed_changeset.last_integrated_local_version =
            cur_changeset.get<int64_t>(m_changeset_last_integrated_client_version);
        parsed_changeset.data = BinaryData(uncompressed_buffer.data(), uncompressed_buffer.size());
        bytes_so_far += parsed_changeset.data.size();
        ret.changesets.push_back(std::move(parsed_changeset));
    }
    ret.remaining_changesets = changeset_list.size() - ret.changesets.size();

    return ret;
}

PendingBootstrapStore::PendingBatchStats PendingBootstrapStore::pending_stats()
{
    auto tr = m_db->start_read();
    auto bootstrap_table = tr->get_table(m_table);
    if (bootstrap_table->is_empty()) {
        return {};
    }

    REALM_ASSERT(bootstrap_table->size() == 1);

    auto bootstrap_obj = bootstrap_table->get_object(0);
    auto query_version = bootstrap_obj.get<int64_t>(m_query_version_col);
    REALM_ASSERT_3(query_version, ==, m_query_version);

    auto changeset_list = bootstrap_obj.get_linklist(m_changesets_col);
    PendingBatchStats stats;
    stats.query_version = query_version;
    stats.pending_changesets = changeset_list.size();

    if (!bootstrap_obj.is_null(m_progress_col)) {
        REALM_ASSERT(m_is_complete);
        stats.complete = true;
    }

    if (changeset_list.is_empty()) {
        stats.remote_version = m_remote_version;
    }
    else {
        changeset_list.for_each([&](Obj& cur_changeset) {
            auto remote_version = cur_changeset.get<int64_t>(m_changeset_remote_version);
            REALM_ASSERT_3(remote_version, ==, m_remote_version);
            stats.remote_version = remote_version;
            stats.pending_changeset_bytes +=
                static_cast<size_t>(cur_changeset.get<int64_t>(m_changeset_original_changeset_size));
            return IteratorControl::AdvanceToNext;
        });
    }

    return stats;
}

void PendingBootstrapStore::pop_front_pending(const TransactionRef& tr, size_t count)
{
    REALM_ASSERT_3(tr->get_transact_stage(), ==, DB::transact_Writing);
    auto bootstrap_table = tr->get_table(m_table);
    if (bootstrap_table->is_empty()) {
        reset_state();
        return;
    }

    // We should only have one pending bootstrap at a time.
    REALM_ASSERT(bootstrap_table->size() == 1);

    auto bootstrap_obj = bootstrap_table->get_object(0);
    auto changeset_list = bootstrap_obj.get_linklist(m_changesets_col);
    REALM_ASSERT_3(changeset_list.size(), >=, count);
    if (count == changeset_list.size()) {
        changeset_list.clear();
    }
    else {
        for (size_t idx = 0; idx < count; ++idx) {
            changeset_list.remove(0);
        }
    }

    if (changeset_list.is_empty()) {
        m_logger.debug(util::LogCategory::changeset,
                       "Removing pending bootstrap obj for version: query %1 / remote %2", m_query_version,
                       m_remote_version);
        bootstrap_obj.remove();
        reset_state();
    }
    else {
        m_logger.debug(util::LogCategory::changeset,
                       "Removed %1 changesets from pending bootstrap for version: query %2 / remote %3. %4 "
                       "changeset(s) remaining",
                       count, m_query_version, m_remote_version, changeset_list.size());
    }
}

void PendingBootstrapStore::reset_state()
{
    m_has_pending = false;
    m_is_complete = false;
    m_query_version = 0;
    m_remote_version = 0;
}

} // namespace realm::sync
