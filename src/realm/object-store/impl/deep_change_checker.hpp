////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#ifndef DEEP_CHANGE_CHECKER_HPP
#define DEEP_CHANGE_CHECKER_HPP

#include <realm/object-store/object_changeset.hpp>
#include <realm/object-store/impl/collection_change_builder.hpp>

#include <array>

namespace realm {
class Realm;
class Table;
class Transaction;

namespace _impl {
class RealmCoordinator;

struct ListChangeInfo {
    TableKey table_key;
    int64_t row_key;
    int64_t col_key;
    CollectionChangeBuilder* changes;
};

// FIXME: this should be in core
using TableKeyType = decltype(TableKey::value);
using ObjKeyType = decltype(ObjKey::value);

struct TransactionChangeInfo {
    std::vector<ListChangeInfo> lists;
    std::unordered_map<TableKeyType, ObjectChangeSet> tables;
    bool track_all;
    bool schema_changed;
};

class DeepChangeChecker {
public:
    struct OutgoingLink {
        int64_t col_key;
        bool is_list;
    };
    struct RelatedTable {
        TableKey table_key;
        std::vector<OutgoingLink> links;
    };

    DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                      std::vector<RelatedTable> const& related_tables);

    bool operator()(int64_t obj_key);

    // Recursively add `table` and all tables it links to to `out`, along with
    // information about the links from them
    static void find_related_tables(std::vector<RelatedTable>& out, Table const& table);

private:
    TransactionChangeInfo const& m_info;
    Table const& m_root_table;
    const TableKey m_root_table_key;
    ObjectChangeSet const* const m_root_object_changes;
    std::unordered_map<TableKeyType, std::unordered_set<ObjKeyType>> m_not_modified;
    std::vector<RelatedTable> const& m_related_tables;

    struct Path {
        int64_t obj_key;
        int64_t col_key;
        bool depth_exceeded;
    };
    std::array<Path, 4> m_current_path;

    bool check_row(Table const& table, ObjKeyType obj_key, size_t depth = 0);
    bool check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key, size_t depth = 0);
};

} // namespace _impl
} // namespace realm


#endif /* DEEP_CHANGE_CHECKER_HPP */
