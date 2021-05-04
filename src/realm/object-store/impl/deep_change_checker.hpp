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

#include <realm/object-store/object_changeset.hpp>
#include <realm/object-store/impl/collection_change_builder.hpp>
#include <realm/object-store/util/checked_mutex.hpp>

#include <realm/util/assert.hpp>
#include <realm/version_id.hpp>
#include <realm/keys.hpp>
#include <realm/table_ref.hpp>

#include <array>
#include <atomic>
#include <exception>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace realm {
class Realm;
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

using KeyPathArray = std::vector<std::vector<std::pair<TableKey, ColKey>>>;

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
    struct OutgoingLink {
        int64_t col_key;
        bool is_list;
    };

    /**
     * `RelatedTable` is used to describe the connections of a `Table` to other tables.
     * Tables count as related if they can be reached via a forward link.
     * A table counts as being related to itself.
     */
    struct RelatedTable {
        // The key of the table for which this struct holds all outgoing links.
        TableKey table_key;
        // All outgoing links to the table specified by `table_key`.
        std::vector<OutgoingLink> links;
    };

    DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                      std::vector<RelatedTable> const& related_tables, std::vector<KeyPathArray> key_path_arrays);

    /**
     * Check if the object identified by `obj_key` was changed.
     *
     * @param obj_key The `ObjKey::value` for the object that is supposed to be checked.
     *
     * @return True if the object was changed, false otherwise.
     */
    bool operator()(int64_t obj_key);

    /**
     * Search for related tables within the specified `table`.
     * Related tables are all tables that can be reached via links from the `table`.
     * A table is always related to itself.
     *
     * Example schema:
     * {
     *   {"root_table",
     *       {
     *           {"link", PropertyType::Object | PropertyType::Nullable, "linked_table"},
     *       }
     *   },
     *   {"linked_table",
     *       {
     *           {"value", PropertyType::Int}
     *       }
     *   },
     * }
     *
     * Asking for related tables for `root_table` based on this schema will result in a `std::vector<RelatedTable>`
     * with two entries, one for `root_table` and one for `linked_table`. The function would be called once for
     * each table involved until there are no further links.
     *
     * Likewise a search for related tables starting with `linked_table` would only return this table.
     *
     * Filter:
     * Using a `key_path_array` that only consists of the table key for `root_table` would result
     * in `out` just having this one entry.
     *
     * @param out Return value containing all tables that can be reached from the given `table` including
     *            some additional information about those tables (see `OutgoingLink` in `RelatedTable`).
     * @param table The table that the related tables will be searched for.
     * @param key_path_arrays A collection of all `KeyPathArray`s passed to the `Callback`s for this
     * `CollectionNotifier`.
     * @param all_callback_have_filters The beheviour when filtering tables depends on all of them having a filter or
     * just some. In the latter case the related tables will be a combination of all tables for the non-filtered way
     * plus the explicitely filtered tables.
     */
    static void find_filtered_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                             std::vector<KeyPathArray> key_path_arrays,
                                             bool all_callback_have_filters);

    // This function is only used by `find_filtered_related_tables` internally.
    // It is however used in some tests and therefore exposed here.
    static void find_all_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                        std::vector<TableKey> tables_in_filters);

private:
    TransactionChangeInfo const& m_info;
    Table const& m_root_table;
    // The `ObjectChangeSet` for `root_table` if it is contained in `m_info`.
    ObjectChangeSet const* const m_root_object_changes;
    std::unordered_map<TableKeyType, std::unordered_set<ObjKeyType>> m_not_modified;
    std::vector<RelatedTable> const& m_related_tables;
    // The `m_key_path_array` contains all columns filtered for. We need this when checking for
    // changes in `operator()` to make sure only columns actually filtered for send notifications.
    std::vector<KeyPathArray> m_key_path_arrays;
    struct Path {
        int64_t obj_key;
        int64_t col_key;
        bool depth_exceeded;
    };
    std::array<Path, 4> m_current_path;

    /**
     * Checks if a specific object, identified by it's `ObjKeyType` in a given `Table` was changed.
     *
     * @param table The `Table` that contains the `ObjKeyType` that will be checked.
     * @param obj_key The `ObjKeyType` identifying the object to be checked for changes.
     * @param filtered_columns A `std::vector` of all `ColKey`s filtered in any of the `NotificationCallbacks`.
     * @param depth Determines how deep the search will be continued if the change could not be found
     *              on the first level.
     *
     * @return True if the object was changed, false otherwise.
     */
    bool check_row(Table const& table, ObjKeyType obj_key, std::vector<ColKey> filtered_columns, size_t depth = 0);

    /**
     * Check the `table` within `m_related_tables` for changes in it's outgoing links.
     *
     * @param table_key The `TableKey` for the `table` in question.
     * @param table The table to check for changed links.
     * @param obj_key The key for the object to look for.
     * @param depth The maximum depth that should be considered for this search.
     *
     * @return True if the specified `table` does have linked objects that have been changed.
     *         False if the `table` is not contained in `m_related_tables` or the `table` does not have any
     *         outgoing links at all or the `table` does not have linked objects with changes.
     */
    bool check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key,
                              std::vector<ColKey> filtered_columns, size_t depth = 0);
};

} // namespace _impl
} // namespace realm
