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
class CollectionBase;
class Mixed;
class Realm;
class Table;
class TableRef;
class Transaction;

using KeyPath = std::vector<std::pair<TableKey, ColKey>>;
using KeyPathArray = std::vector<KeyPath>;

namespace _impl {
class RealmCoordinator;

struct ListChangeInfo {
    TableKey table_key;
    int64_t row_key;
    int64_t col_key;
    CollectionChangeBuilder* changes;
};

struct TransactionChangeInfo {
    std::vector<ListChangeInfo> lists;
    std::unordered_map<TableKeyType, ObjectChangeSet> tables;
    bool track_all;
    bool schema_changed;
};

/**
 * The `DeepChangeChecker` serves two purposes:
 * - Given an initial `Table` and an optional `KeyPathArray` it find all tables related to that initial table.
 *   A `RelatedTable` is a `Table` that can be reached via a link from another `Table`.
 * - The `DeepChangeChecker` also offers a way to check if a specific `ObjKey` was changed.
 */
class DeepChangeChecker {
public:
    struct RelatedTable {
        // The key of the table for which this struct holds all outgoing links.
        TableKey table_key;
        std::vector<ColKey> links;
    };
    typedef std::vector<RelatedTable> RelatedTables;
    DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table, RelatedTables const& related_tables,
                      const KeyPathArray& key_path_array, bool all_callbacks_filtered);

    bool operator()(ObjKeyType obj_key);

    // Recursively add `table` and all tables it links to to `out`, along with
    // information about the links from them
    static void find_related_tables0(std::vector<RelatedTable>& out, Table const& table,
                                     const KeyPathArray& key_path_array);
    static void find_related_tables1(std::vector<RelatedTable>& out, Table const& table,
                                     const KeyPathArray& key_path_array);
    static void find_related_tables2(std::vector<RelatedTable>& out, Table const& table,
                                     const KeyPathArray& key_path_array);

    TransactionChangeInfo const& m_info;

    // The `Table` this `DeepChangeChecker` is based on.
    Table const& m_root_table;

    // The `m_key_path_array` contains all columns filtered for. We need this when checking for
    // changes in `operator()` to make sure only columns actually filtered for send notifications.
    const KeyPathArray& m_key_path_array;

    // The `ObjectChangeSet` for `root_table` if it is contained in `m_info`.
    ObjectChangeSet const* const m_root_object_changes;
    std::unordered_map<TableKeyType, std::unordered_set<ObjKeyType>> m_not_modified;
    RelatedTables const& m_related_tables;

    struct Path {
        ObjKey obj_key;
        ColKey col_key;
        bool depth_exceeded;
    };
    std::array<Path, 4> m_current_path;

    bool check_row(Table const& table, ObjKeyType obj_key, const std::vector<ColKey>& filtered_columns,
                   size_t depth = 0);
    bool check_outgoing_links(Table const& table, ObjKey obj_key, const std::vector<ColKey>& filtered_columns,
                              size_t depth = 0);
    bool do_check_for_collection_modifications(std::unique_ptr<CollectionBase> coll,
                                               const std::vector<ColKey>& filtered_columns, size_t depth);
    template <typename T>
    bool do_check_for_collection_of_mixed(T* coll, const std::vector<ColKey>& filtered_columns, size_t depth);
    template <typename T>
    bool do_check_mixed_for_link(T* coll, TableRef& cached_linked_table, Mixed value,
                                 const std::vector<ColKey>& filtered_columns, size_t depth);

    // Contains all `ColKey`s that we filter for in the root table.
    std::vector<ColKey> m_filtered_columns_in_root_table;
    std::vector<ColKey> m_filtered_columns;
};

/**
 * The `CollectionKeyPathChangeChecker` is a specialised version of `DeepChangeChecker` that offers a check by
 * traversing and only traversing the given `KeyPathArray`. With this it supports any depth (as opposed to the maxium
 * depth of 4 on the `DeepChangeChecker`) and backlinks.
 */
class CollectionKeyPathChangeChecker : DeepChangeChecker {
public:
    CollectionKeyPathChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                   std::vector<RelatedTable> const& related_tables,
                                   const KeyPathArray& key_path_array, bool all_callbacks_filtered);

    /**
     * Check if the `Object` identified by `object_key` was changed and it is included in the `KeyPathArray` provided
     * when construction this `CollectionKeyPathChangeChecker`.
     *
     * @param object_key The `ObjKey::value` for the `Object` that is supposed to be checked.
     *
     * @return True if the `Object` was changed, false otherwise.
     */
    bool operator()(int64_t object_key);

private:
    friend class ObjectKeyPathChangeChecker;

    /**
     * Traverses down a given `KeyPath` and checks the objects along the way for changes.
     *
     * @param changed_columns The list of `ColKeyType`s that was changed in the root object.
     *                        A key will be added to this list if it turns out to be changed.
     * @param key_path The `KeyPath` used to traverse the given object with.
     * @param depth The current depth in the key_path.
     * @param table The `TableKey` for the current depth.
     * @param object_key_value The `ObjKeyType` that is to be checked for changes.
     */
    void find_changed_columns(std::vector<int64_t>& changed_columns, const KeyPath& key_path, size_t depth,
                              const Table& table, const ObjKeyType& object_key_value);
};

/**
 * The `ObjectKeyPathChangeChecker` is a specialised version of `CollectionKeyPathChangeChecker` that offers a deep
 * change check for `Object` which is different from the checks done for `Collection`. Like
 * `CollectionKeyPathChangeChecker` it is only traversing the given KeyPathArray and has no depth limit.
 *
 * This difference is mainly seen in the fact that for `Object` we notify about the specific columns that have been
 * changed which we do not for `Collection`.
 */
class ObjectKeyPathChangeChecker : CollectionKeyPathChangeChecker {
public:
    ObjectKeyPathChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                               std::vector<DeepChangeChecker::RelatedTable> const& related_tables,
                               const KeyPathArray& key_path_array, bool all_callbacks_filtered);

    /**
     * Check if the `Object` identified by `object_key` was changed and it is included in the `KeyPathArray` provided
     * when construction this `ObjectKeyPathChangeChecker`.
     *
     * @param object_key The `ObjKey::value` for the `Object` that is supposed to be checked.
     *
     * @return A list of columns changed in the root `Object`.
     */
    std::vector<int64_t> operator()(int64_t object_key);
};


} // namespace _impl
} // namespace realm

#endif /* DEEP_CHANGE_CHECKER_HPP */
