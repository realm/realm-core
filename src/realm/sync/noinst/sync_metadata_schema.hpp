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

#include "realm/data_type.hpp"
#include "realm/keys.hpp"
#include "realm/util/optional.hpp"
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace realm {
class Transaction;
using TransactionRef = std::shared_ptr<Transaction>;
} // namespace realm

namespace realm::sync {
namespace internal_schema_groups {
constexpr static std::string_view c_flx_subscription_store("flx_subscription_store");
constexpr static std::string_view c_pending_bootstraps("pending_bootstraps");
}

/*
 * The types in this file represent the schema for tables used internally by the sync client. They are similar
 * to the Schema/ObjectSchema/Property types in object store, but are lighter weight and have no dependencies
 * outside of core.
 *
 * The two functions for interacting with them are:
 * create_sync_metadata_schema - which takes a write transaction and a group of tables, and creates them.
 * load_sync_metadata_schema - validates and loads a group of tables.
 *
 * The SyncMetadataTable/SyncMetadataColumn classes each have an out parameter `key_out` that is the core key
 * type for the table/column that will get updated when it's created/loaded/validated.
 *
 * If validation fails, load_sync_metadata_schema will throw a std::runtime_exception.
 */

struct SyncMetadataColumn {
    SyncMetadataColumn(ColKey* out, std::string_view name, DataType data_type, bool is_optional = false)
        : key_out(out)
        , name(name)
        , data_type(data_type)
        , is_optional(is_optional)
    {
    }

    SyncMetadataColumn(ColKey* out, std::string_view name, std::string_view target_table, bool is_list)
        : key_out(out)
        , name(name)
        , data_type(is_list ? type_LinkList : type_Link)
        , is_optional(is_list ? false : true)
        , target_table(target_table)
    {
    }

    ColKey* key_out;
    std::string_view name;
    DataType data_type;
    bool is_optional;
    std::string_view target_table;
};

struct SyncMetadataTable {
    TableKey* key_out;
    std::string_view name;
    bool is_embedded;
    util::Optional<SyncMetadataColumn> pk_info;
    std::vector<SyncMetadataColumn> columns;

    struct IsEmbeddedTag {
    };
    SyncMetadataTable(TableKey* out, std::string_view name, IsEmbeddedTag,
                      std::initializer_list<SyncMetadataColumn> columns)
        : key_out(out)
        , name(name)
        , is_embedded(true)
        , pk_info(util::none)
        , columns(std::move(columns))
    {
    }

    SyncMetadataTable(TableKey* out, std::string_view name, SyncMetadataColumn pk_info,
                      std::initializer_list<SyncMetadataColumn> columns)
        : key_out(out)
        , name(name)
        , is_embedded(false)
        , pk_info(std::move(pk_info))
        , columns(std::move(columns))
    {
    }

    SyncMetadataTable(TableKey* out, std::string_view name, std::initializer_list<SyncMetadataColumn> columns)
        : key_out(out)
        , name(name)
        , is_embedded(false)
        , pk_info(util::none)
        , columns(std::move(columns))
    {
    }
};

void create_sync_metadata_schema(const TransactionRef& tr, std::vector<SyncMetadataTable>* tables);
void load_sync_metadata_schema(const TransactionRef& tr, std::vector<SyncMetadataTable>* tables);

// SyncMetadataSchemas manages the schema version numbers for different groups of internal tables used
// within sync.
class SyncMetadataSchemaVersions {
public:
    explicit SyncMetadataSchemaVersions(const TransactionRef& ref);

    util::Optional<int64_t> get_version_for(const TransactionRef& tr, std::string_view schema_group_name);
    void set_version_for(const TransactionRef& tr, std::string_view schema_group_name, int64_t version);

private:
    TableKey m_table;
    ColKey m_version_field;
    ColKey m_schema_group_field;
};

} // namespace realm::sync
