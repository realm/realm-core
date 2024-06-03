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

#include <realm/sync/noinst/sync_metadata_schema.hpp>

#include <realm/data_type.hpp>
#include <realm/transaction.hpp>
#include <realm/util/flat_map.hpp>
#include <stdexcept>

namespace realm::sync {
namespace {

constexpr static std::string_view c_flx_metadata_table("flx_metadata");
constexpr static std::string_view c_sync_internal_schemas_table("sync_internal_schemas");
constexpr static std::string_view c_meta_schema_version_field("schema_version");
constexpr static std::string_view c_meta_schema_schema_group_field("schema_group_name");

} // namespace

void create_sync_metadata_schema(const TransactionRef& tr, std::vector<SyncMetadataTable>* tables)
{
    util::FlatMap<std::string_view, TableRef> found_tables;
    for (auto& table : *tables) {
        if (tr->has_table(table.name)) {
            throw RuntimeError(
                ErrorCodes::RuntimeError,
                util::format("table %1 already existed when creating internal tables for sync", table.name));
        }
        TableRef table_ref;
        if (table.is_embedded) {
            table_ref = tr->add_table(table.name, Table::Type::Embedded);
        }
        else if (table.pk_info) {
            table_ref = tr->add_table_with_primary_key(table.name, table.pk_info->data_type, table.pk_info->name,
                                                       table.pk_info->is_optional);
            *table.pk_info->key_out = table_ref->get_primary_key_column();
        }
        else {
            table_ref = tr->add_table(table.name);
        }

        found_tables.insert({table.name, table_ref});
        *table.key_out = table_ref->get_key();
    }

    for (auto& table : *tables) {
        auto& table_ref = found_tables.at(table.name);
        for (auto& column : table.columns) {
            if (column.data_type == type_Link) {
                auto target_table_it = found_tables.find(column.target_table);
                if (target_table_it == found_tables.end()) {
                    throw LogicError(ErrorCodes::InvalidArgument,
                                     util::format("cannot link to non-existant table %1 from internal sync table %2",
                                                  column.target_table, table.name));
                }
                if (column.is_list) {
                    *column.key_out = table_ref->add_column_list(*target_table_it->second, column.name);
                }
                else {
                    *column.key_out = table_ref->add_column(*target_table_it->second, column.name);
                }
            }
            else {
                *column.key_out = table_ref->add_column(column.data_type, column.name, column.is_optional);
            }
        }
    }
}

void load_sync_metadata_schema(const TransactionRef& tr, std::vector<SyncMetadataTable>* tables)
{
    for (auto& table : *tables) {
        auto table_ref = tr->get_table(table.name);
        if (!table_ref) {
            throw RuntimeError(ErrorCodes::RuntimeError,
                               util::format("could not find internal sync table %1", table.name));
        }

        *table.key_out = table_ref->get_key();
        if (table.pk_info) {
            auto pk_col = table_ref->get_primary_key_column();
            if (auto pk_name = table_ref->get_column_name(pk_col); pk_name != table.pk_info->name) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format(
                        "primary key name of sync internal table %1 does not match (stored: %2, defined: %3)",
                        table.name, pk_name, table.pk_info->name));
            }
            if (auto pk_type = table_ref->get_column_type(pk_col); pk_type != table.pk_info->data_type) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format(
                        "primary key type of sync internal table %1 does not match (stored: %2, defined: %3)",
                        table.name, pk_type, table.pk_info->data_type));
            }
            if (auto is_nullable = table_ref->is_nullable(pk_col); is_nullable != table.pk_info->is_optional) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format(
                        "primary key nullabilty of sync internal table %1 does not match (stored: %2, defined: %3)",
                        table.name, is_nullable, table.pk_info->is_optional));
            }
            *table.pk_info->key_out = pk_col;
        }
        else if (table.is_embedded && !table_ref->is_embedded()) {
            throw RuntimeError(ErrorCodes::RuntimeError,
                               util::format("internal sync table %1 should be embedded, but is not", table.name));
        }

        if (table.columns.size() + size_t(table.pk_info ? 1 : 0) != table_ref->get_column_count()) {
            throw RuntimeError(
                ErrorCodes::RuntimeError,
                util::format("sync internal table %1 has a different number of columns than its schema", table.name));
        }

        for (auto& col : table.columns) {
            auto col_key = table_ref->get_column_key(col.name);
            if (!col_key) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format("column %1 is missing in sync internal table %2", col.name, table.name));
            }

            auto found_col_type = table_ref->get_column_type(col_key);
            if (found_col_type != col.data_type) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format("column %1 in sync internal table %2 is the wrong type", col.name, table.name));
            }

            if (col.is_optional != table_ref->is_nullable(col_key)) {
                throw RuntimeError(
                    ErrorCodes::RuntimeError,
                    util::format("column %1 in sync internal table %2 has different nullabilty than in its schema",
                                 col.name, table.name));
            }

            if (col.data_type == type_Link) {
                if (table_ref->get_link_target(col_key)->get_name() != col.target_table) {
                    RuntimeError(ErrorCodes::RuntimeError,
                                 util::format("column %1 in sync internal table %2 links to the wrong table %3",
                                              col.name, table.name, table_ref->get_link_target(col_key)->get_name()));
                }
            }
            *col.key_out = col_key;
        }
    }
}

SyncMetadataSchemaVersionsReader::SyncMetadataSchemaVersionsReader(const TransactionRef& tr)
{
    std::vector<SyncMetadataTable> unified_schema_version_table_def{
        {&m_table,
         c_sync_internal_schemas_table,
         {&m_schema_group_field, c_meta_schema_schema_group_field, type_String},
         {{&m_version_field, c_meta_schema_version_field, type_Int}}}};

    // Any type of transaction is allowed, including frozen and write, as long as it supports reading
    REALM_ASSERT_EX(tr->get_transact_stage() != DB::transact_Ready, tr->get_transact_stage());
    // If the legacy_meta_table exists, then this table hasn't been converted and
    // the metadata schema versions information has not been upgraded/not accurate
    if (tr->has_table(c_flx_metadata_table)) {
        return;
    }

    if (tr->has_table(c_sync_internal_schemas_table)) {
        // Load m_table with the table/schema information
        load_sync_metadata_schema(tr, &unified_schema_version_table_def);
    }
}

std::optional<int64_t> SyncMetadataSchemaVersionsReader::get_legacy_version(const TransactionRef& tr)
{
    if (!tr->has_table(c_flx_metadata_table)) {
        return std::nullopt;
    }

    TableKey legacy_table_key;
    ColKey legacy_version_key;
    std::vector<SyncMetadataTable> legacy_table_def{
        {&legacy_table_key, c_flx_metadata_table, {{&legacy_version_key, c_meta_schema_version_field, type_Int}}}};

    // Convert the legacy table to the regular schema versions table if it exists
    load_sync_metadata_schema(tr, &legacy_table_def);

    if (auto legacy_meta_table = tr->get_table(legacy_table_key);
        legacy_meta_table && legacy_meta_table->size() > 0) {
        auto legacy_obj = legacy_meta_table->get_object(0);
        return legacy_obj.get<int64_t>(legacy_version_key);
    }

    return std::nullopt;
}

std::optional<int64_t> SyncMetadataSchemaVersionsReader::get_version_for(const TransactionRef& tr,
                                                                         std::string_view schema_group_name)
{
    if (!m_table) {
        // The legacy version only applies to the subscription store, don't query otherwise
        if (schema_group_name == internal_schema_groups::c_flx_subscription_store) {
            if (auto legacy_version = get_legacy_version(tr)) {
                return legacy_version;
            }
        }
        return util::none;
    }

    auto schema_versions = tr->get_table(m_table);
    auto obj_key = schema_versions->find_primary_key(Mixed{StringData(schema_group_name)});
    if (!obj_key) {
        return util::none;
    }
    auto metadata_obj = schema_versions->get_object(obj_key);
    if (!metadata_obj) {
        return util::none;
    }

    return metadata_obj.get<int64_t>(m_version_field);
}

SyncMetadataSchemaVersions::SyncMetadataSchemaVersions(const TransactionRef& tr)
    : SyncMetadataSchemaVersionsReader(tr)
{
    std::vector<SyncMetadataTable> unified_schema_version_table_def{
        {&m_table,
         c_sync_internal_schemas_table,
         {&m_schema_group_field, c_meta_schema_schema_group_field, type_String},
         {{&m_version_field, c_meta_schema_version_field, type_Int}}}};

    DB::TransactStage orig = tr->get_transact_stage();
    bool modified = false;

    // Read and write transactions are allowed, but not frozen
    REALM_ASSERT_EX((orig == DB::transact_Reading || orig == DB::transact_Writing), orig);
    // If the versions table exists, then m_table would have been initialized by the reader constructor
    // If the versions table doesn't exist, then initialize it now
    if (!m_table) {
        // table should have already been initialized or needs to be created,
        // but re-initialize in case it isn't (e.g. both unified and legacy tables exist in DB)
        if (REALM_UNLIKELY(tr->has_table(c_sync_internal_schemas_table))) {
            load_sync_metadata_schema(tr, &unified_schema_version_table_def);
        }
        else {
            // Only write the versions table if it doesn't exist
            if (tr->get_transact_stage() != DB::transact_Writing) {
                tr->promote_to_write();
            }
            create_sync_metadata_schema(tr, &unified_schema_version_table_def);
            modified = true;
        }
    }

    if (auto legacy_version = get_legacy_version(tr)) {
        // Migrate from just having a subscription store metadata table to having multiple table groups with multiple
        // versions.
        if (tr->get_transact_stage() != DB::transact_Writing) {
            tr->promote_to_write();
        }
        // Only the flx subscription store can potentially have the legacy metadata table
        set_version_for(tr, internal_schema_groups::c_flx_subscription_store, *legacy_version);
        tr->remove_table(c_flx_metadata_table);
        modified = true;
    }
    if (!modified)
        return; // nothing to commit

    // Commit and revert to the original transact stage
    if (orig == DB::transact_Reading)
        tr->commit_and_continue_as_read();
    else
        tr->commit_and_continue_writing();
}

void SyncMetadataSchemaVersions::set_version_for(const TransactionRef& tr, std::string_view schema_group_name,
                                                 int64_t version)
{
    REALM_ASSERT(m_table);

    auto schema_versions = tr->get_table(m_table);
    auto metadata_obj = schema_versions->create_object_with_primary_key(Mixed{StringData(schema_group_name)});
    metadata_obj.set(m_version_field, version);
}

} // namespace realm::sync
