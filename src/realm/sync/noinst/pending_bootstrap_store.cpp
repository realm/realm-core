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

PendingBootstrapStore::PendingBootstrapStore(DBRef db, util::Logger* logger)
    : m_db(std::move(db))
    , m_logger(logger)
{
    std::vector<SyncMetadataTable> internal_tables{
        {&m_table,
         c_pending_bootstrap_table,
         {&m_query_version, c_pending_bootstrap_query_version, type_Int},
         {
             {&m_changesets, c_pending_bootstrap_changesets, c_pending_changesets_table, true},
             {&m_progress, c_pending_bootstrap_progress, c_progress_table, false},
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
            throw std::runtime_error("Invalid schema version for FLX sync pending bootstrap table group");
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
        m_has_pending = true;
    }
    else {
        m_has_pending = false;
    }
}

void PendingBootstrapStore::add_batch(int64_t query_version, util::Optional<SyncProgress> progress,
                                      const _impl::ClientProtocol::ReceivedChangesets& changesets)
{
    std::vector<util::AppendBuffer<char>> compressed_changesets;
    compressed_changesets.reserve(changesets.size());

    util::compression::CompressMemoryArena arena;
    for (auto& changeset : changesets) {
        compressed_changesets.emplace_back();
        util::compression::allocate_and_compress_nonportable(arena, {changeset.data.get_first_chunk()},
                                                             compressed_changesets.back());
    }

    auto tr = m_db->start_write();
    auto bootstrap_table = tr->get_table(m_table);
    auto incomplete_bootstraps = Query(bootstrap_table).not_equal(m_query_version, query_version).find_all();
    incomplete_bootstraps.for_each([&](Obj obj) {
        m_logger->debug("Clearing incomplete bootstrap for query version %1", obj.get<int64_t>(m_query_version));
        return false;
    });
    incomplete_bootstraps.clear();

    bool did_create = false;
    auto bootstrap_obj = bootstrap_table->create_object_with_primary_key(Mixed{query_version}, &did_create);
    if (progress) {
        auto progress_obj = bootstrap_obj.create_and_set_linked_object(m_progress);
        progress_obj.set(m_progress_latest_server_version, int64_t(progress->latest_server_version.version));
        progress_obj.set(m_progress_latest_server_version_salt, int64_t(progress->latest_server_version.salt));
        progress_obj.set(m_progress_download_server_version, int64_t(progress->download.server_version));
        progress_obj.set(m_progress_download_client_version,
                         int64_t(progress->download.last_integrated_client_version));
        progress_obj.set(m_progress_upload_server_version, int64_t(progress->upload.last_integrated_server_version));
        progress_obj.set(m_progress_upload_client_version, int64_t(progress->upload.client_version));
    }

    auto changesets_list = bootstrap_obj.get_linklist(m_changesets);
    for (size_t idx = 0; idx < changesets.size(); ++idx) {
        auto cur_changeset = changesets_list.create_and_insert_linked_object(changesets_list.size());
        cur_changeset.set(m_changeset_remote_version, int64_t(changesets[idx].remote_version));
        cur_changeset.set(m_changeset_last_integrated_client_version,
                          int64_t(changesets[idx].last_integrated_local_version));
        cur_changeset.set(m_changeset_origin_file_ident, int64_t(changesets[idx].origin_file_ident));
        cur_changeset.set(m_changeset_origin_timestamp, int64_t(changesets[idx].origin_timestamp));
        cur_changeset.set(m_changeset_original_changeset_size, int64_t(changesets[idx].original_changeset_size));
        BinaryData compressed_data(compressed_changesets[idx].data(), compressed_changesets[idx].size());
        cur_changeset.set(m_changeset_data, compressed_data);
    }

    tr->commit();

    if (did_create) {
        m_logger->trace("Created new pending bootstrap object for query version %1", query_version);
    }
    else {
        m_logger->trace("Added batch to pending bootstrap object for query version %1", query_version);
    }
    if (progress) {
        m_logger->trace("Finalized pending bootstrap object for query version %1", query_version);
    }
    m_has_pending = true;
}

bool PendingBootstrapStore::has_pending()
{
    return m_has_pending;
}

void PendingBootstrapStore::clear()
{
    auto tr = m_db->start_write();
    auto bootstrap_table = tr->get_table(m_table);
    bootstrap_table->clear();
    tr->commit();
}

PendingBootstrapStore::PendingBatch PendingBootstrapStore::peek_pending(size_t limit_in_bytes)
{
    auto tr = m_db->start_read();
    auto bootstrap_table = tr->get_table(m_table);
    if (bootstrap_table->is_empty()) {
        return {};
    }

    // We should only have one pending bootstrap at a time.
    REALM_ASSERT(bootstrap_table->size() == 1);

    auto bootstrap_obj = bootstrap_table->get_object(0);
    PendingBatch ret;
    ret.query_version = bootstrap_obj.get<int64_t>(m_query_version);

    if (!bootstrap_obj.is_null(m_progress)) {
        auto progress_obj = bootstrap_obj.get_linked_object(m_progress);
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

    auto changeset_list = bootstrap_obj.get_linklist(m_changesets);
    size_t bytes_so_far = 0;
    for (size_t idx = 0; idx < changeset_list.size() && bytes_so_far < limit_in_bytes; ++idx) {
        auto cur_changeset = changeset_list.get_object(idx);
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

        Transformer::RemoteChangeset parsed_changeset;
        parsed_changeset.original_changeset_size =
            static_cast<size_t>(cur_changeset.get<int64_t>(m_changeset_original_changeset_size));
        parsed_changeset.origin_timestamp = cur_changeset.get<int64_t>(m_changeset_origin_timestamp);
        parsed_changeset.origin_file_ident = cur_changeset.get<int64_t>(m_changeset_origin_file_ident);
        parsed_changeset.remote_version = cur_changeset.get<int64_t>(m_changeset_remote_version);
        parsed_changeset.last_integrated_local_version =
            cur_changeset.get<int64_t>(m_changeset_last_integrated_client_version);
        parsed_changeset.data = BinaryData(uncompressed_buffer.data(), uncompressed_buffer.size());
        ret.changesets.push_back(std::move(parsed_changeset));
    }
    ret.remaining = changeset_list.size() - ret.changesets.size();

    return ret;
}

void PendingBootstrapStore::pop_front_pending(const TransactionRef& tr, size_t count)
{
    REALM_ASSERT_3(tr->get_transact_stage(), ==, DB::transact_Writing);
    auto bootstrap_table = tr->get_table(m_table);
    if (bootstrap_table->is_empty()) {
        return;
    }

    // We should only have one pending bootstrap at a time.
    REALM_ASSERT(bootstrap_table->size() == 1);

    auto bootstrap_obj = bootstrap_table->get_object(0);
    auto changeset_list = bootstrap_obj.get_linklist(m_changesets);
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
        m_logger->trace("Removing pending bootstrap obj for query version %1",
                        bootstrap_obj.get<int64_t>(m_query_version));
        bootstrap_obj.remove();
    }
    else {
        m_logger->trace("Removing pending bootstrap batch for query version %1. %2 changeset remaining",
                        bootstrap_obj.get<int64_t>(m_query_version), changeset_list.size());
    }

    m_has_pending = (bootstrap_table->is_empty() == false);
}

} // namespace realm::sync
