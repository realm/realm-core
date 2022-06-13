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

#include "realm/sync/noinst/pending_error_store.hpp"

#include "realm/list.hpp"
#include "realm/sort_descriptor.hpp"
#include "realm/sync/noinst/sync_metadata_schema.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/table_view.hpp"
#include "realm/transaction.hpp"
#include <cstdint>

namespace realm::sync {
namespace {
constexpr static int c_schema_version = 1;
constexpr static std::string_view c_errors_table_name("flx_pending_errors");
constexpr static std::string_view c_rejected_updates_table_name("flx_pending_errors_rejected_updates");
constexpr static std::string_view c_pending_util_server_version("pending_until_server_version");
constexpr static std::string_view c_error_code_col("code");
constexpr static std::string_view c_error_message_col("message");
constexpr static std::string_view c_log_url_col("log_url");
constexpr static std::string_view c_recovery_mode_disabled_col("recovery_mode_disabled");
constexpr static std::string_view c_try_again_col("try_again");
constexpr static std::string_view c_should_client_reset_col("should_client_reset");
constexpr static std::string_view c_rejected_updates_col("rejected_updates");
constexpr static std::string_view c_max_resumption_delay_col("max_resumption_delay_secs");
constexpr static std::string_view c_resumption_delay_interval_col("resumption_delay_interval_secs");
constexpr static std::string_view c_resumption_delay_multiplier_col("resumption_delay_multiplier");
constexpr static std::string_view c_rejected_update_reason_col("reason");
constexpr static std::string_view c_rejected_update_primary_key_col("primary_key");
constexpr static std::string_view c_rejected_update_table_col("table");

} // namespace

PendingErrorStore::PendingErrorStore(DBRef db, util::Logger* logger)
    : m_db(std::move(db))
    , m_logger(logger)
{
    std::vector<SyncMetadataTable> internal_tables{
        {&m_errors_table,
         c_errors_table_name,
         {&m_pending_until_server_version, c_pending_util_server_version, type_Int},
         {
             {&m_error_code, c_error_code_col, type_Int},
             {&m_error_message, c_error_message_col, type_String},
             {&m_log_url, c_log_url_col, type_String, true},
             {&m_recovery_mode_disabled, c_recovery_mode_disabled_col, type_Bool},
             {&m_try_again, c_try_again_col, type_Bool},
             {&m_should_client_reset, c_should_client_reset_col, type_Bool, true},
             {&m_rejected_updates, c_rejected_updates_col, c_rejected_updates_table_name, true},
             {&m_resumption_delay_interval, c_resumption_delay_interval_col, type_Int, true},
             {&m_max_resumption_delay_interval, c_max_resumption_delay_col, type_Int},
             {&m_resumption_delay_backoff_multiplier, c_resumption_delay_multiplier_col, type_Int},
         }},
        {&m_rejected_updates_table,
         c_rejected_updates_table_name,
         SyncMetadataTable::IsEmbeddedTag{},
         {
             {&m_rejected_update_reason, c_rejected_update_reason_col, type_String},
             {&m_rejected_update_pk, c_rejected_update_primary_key_col, type_Mixed},
             {&m_rejected_update_table, c_rejected_update_table_col, type_String},
         }}};

    auto tr = m_db->start_read();
    SyncMetadataSchemaVersions schema_versions(tr);
    if (auto schema_version = schema_versions.get_version_for(tr, internal_schema_groups::c_pending_errors)) {
        if (*schema_version != c_schema_version) {
            throw std::runtime_error("Invalid schema version for FLX sync pending error table group");
        }
        load_sync_metadata_schema(tr, &internal_tables);
    }
    else {
        tr->promote_to_write();
        create_sync_metadata_schema(tr, &internal_tables);
        schema_versions.set_version_for(tr, internal_schema_groups::c_pending_errors, c_schema_version);
        tr->commit_and_continue_as_read();
    }
}

std::vector<ProtocolErrorInfo> PendingErrorStore::peek_pending_errors(const TransactionRef& tr,
                                                                      sync::version_type before_server_version)
{
    REALM_ASSERT(tr->is_attached());
    auto table = tr->get_table(m_errors_table);
    if (table->is_empty()) {
        return {};
    }

    auto query =
        table->where().less_equal(m_pending_until_server_version, static_cast<int64_t>(before_server_version));
    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{m_pending_until_server_version}}, {false}});

    auto pending_errors = query.find_all(std::move(descriptor_ordering));
    std::vector<ProtocolErrorInfo> ret;

    for (size_t idx = 0; idx < pending_errors.size(); ++idx) {
        auto obj = pending_errors.get_object(idx);
        ProtocolErrorInfo error_info(static_cast<int>(obj.get<int64_t>(m_error_code)),
                                     obj.get<StringData>(m_error_message), obj.get<bool>(m_try_again));
        error_info.pending_until_server_version = obj.get<int64_t>(m_pending_until_server_version);
        error_info.client_reset_recovery_is_disabled = obj.get<bool>(m_recovery_mode_disabled);
        if (!obj.is_null(m_should_client_reset)) {
            error_info.should_client_reset = obj.get<bool>(m_should_client_reset);
        }
        if (!obj.is_null(m_log_url)) {
            error_info.log_url = std::string{obj.get<StringData>(m_log_url)};
        }
        if (!obj.is_null(m_resumption_delay_interval)) {
            error_info.resumption_delay_info = ResumptionDelayInfo{
                std::chrono::seconds{obj.get<int64_t>(m_max_resumption_delay_interval)},
                std::chrono::seconds{obj.get<int64_t>(m_resumption_delay_interval)},
                static_cast<int>(obj.get<int64_t>(m_resumption_delay_backoff_multiplier)),
            };
        }

        auto rejected_updates = obj.get_linklist(m_rejected_updates);
        for (size_t idx = 0; idx < rejected_updates.size(); ++idx) {
            auto rejected_update = rejected_updates.get_object(idx);
            error_info.compensating_writes.push_back({rejected_update.get<StringData>(m_rejected_update_table),
                                                      rejected_update.get<Mixed>(m_rejected_update_pk),
                                                      rejected_update.get<StringData>(m_rejected_update_reason)});
        }

        m_logger->trace("Found error message that was pending until server version %1",
                        obj.get<int64_t>(m_pending_until_server_version));
        ret.push_back(std::move(error_info));
    }

    return ret;
}

void PendingErrorStore::remove_pending_errors(sync::version_type before_server_version)
{
    auto tr = m_db->start_write();
    auto table = tr->get_table(m_errors_table);
    if (table->is_empty()) {
        return;
    }

    auto query =
        table->where().less_equal(m_pending_until_server_version, static_cast<int64_t>(before_server_version));
    DescriptorOrdering descriptor_ordering;
    descriptor_ordering.append_sort(SortDescriptor{{{m_pending_until_server_version}}, {false}});

    auto pending_errors = query.find_all(std::move(descriptor_ordering));
    auto removed_count = pending_errors.size();
    pending_errors.clear();
    tr->commit();
    m_logger->trace("Removed %1 pending error message records", removed_count);
}

void PendingErrorStore::add_pending_error(const ProtocolErrorInfo& error_info)
{
    REALM_ASSERT(error_info.pending_until_server_version);
    auto tr = m_db->start_write();

    auto table = tr->get_table(m_errors_table);
    auto new_obj =
        table->create_object_with_primary_key(Mixed{static_cast<int64_t>(*error_info.pending_until_server_version)});
    new_obj.set(m_error_code, error_info.raw_error_code);
    new_obj.set(m_error_message, error_info.message);
    if (error_info.log_url) {
        new_obj.set(m_log_url, *error_info.log_url);
    }
    new_obj.set(m_try_again, error_info.try_again);
    if (error_info.should_client_reset) {
        new_obj.set(m_should_client_reset, *error_info.should_client_reset);
    }
    if (error_info.resumption_delay_info) {
        new_obj.set(m_resumption_delay_interval, error_info.resumption_delay_info->resumption_delay_interval.count());
        new_obj.set(m_resumption_delay_backoff_multiplier,
                    error_info.resumption_delay_info->resumption_delay_backoff_multiplier);
        new_obj.set(m_max_resumption_delay_interval,
                    error_info.resumption_delay_info->max_resumption_delay_interval.count());
    }

    auto rejected_updates = new_obj.get_linklist(m_rejected_updates);
    for (const auto& rejected_update : error_info.compensating_writes) {
        auto new_rejected_update_obj = rejected_updates.create_and_insert_linked_object(
            rejected_updates.is_empty() ? 0 : rejected_updates.size() - 1);
        new_rejected_update_obj.set(m_rejected_update_table, rejected_update.object_name);
        new_rejected_update_obj.set(m_rejected_update_reason, rejected_update.reason);
        new_rejected_update_obj.set(m_rejected_update_pk, rejected_update.primary_key);
    }

    auto version = tr->commit();
    m_logger->trace(
        "Added pending error in version %1 that will be pending until server version %2 (code: %3, message: %4)",
        version, *error_info.pending_until_server_version, error_info.raw_error_code, error_info.message);
}

} // namespace realm::sync
