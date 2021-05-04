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

#include <realm/object-store/impl/deep_change_checker.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/shared_realm.hpp>

#include <realm/db.hpp>
#include <realm/list.hpp>

using namespace realm;
using namespace realm::_impl;

void DeepChangeChecker::find_all_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                                std::vector<TableKey> tables_in_filters)
{
    auto table_key = table.get_key();
    // If the currently looked at `table` is already part of the `std::vector<RelatedTable>` (possibly
    // due to another path involving it) we do not need to traverse further and can return.
    if (any_of(begin(out), end(out), [=](auto& tbl) {
            return tbl.table_key == table_key;
        }))
        return;

    // If a filter is set and the table is not part of the filter, it can be skipped.
    if (tables_in_filters.size() != 0) {
        if (none_of(begin(tables_in_filters), end(tables_in_filters), [=](auto& filtered_table_key) {
                return filtered_table_key == table_key;
            })) {
            return;
        }
    }

    // We need to add this table to `out` before recurring so that the check
    // above works, but we can't store a pointer to the thing being populated
    // because the recursive calls may resize `out`, so instead look it up by
    // index every time.
    size_t out_index = out.size();
    out.push_back({table_key, {}});

    for (auto col_key : table.get_column_keys()) {
        auto type = table.get_column_type(col_key);
        // If a column within the `table` does link to another table it needs to be added to `table`'s
        // links.
        if (type == type_Link || type == type_LinkList) {
            //            this does not seem to pick up the right links for objects

            out[out_index].links.push_back({col_key.value, type == type_LinkList});
            // Finally this function needs to be called again to traverse all linked tables using the
            // just found link.
            find_all_related_tables(out, *table.get_link_target(col_key), tables_in_filters);
        }
    }
    if (tables_in_filters.size() != 0) {
        //        table.for_each_backlink_column([&](ColKey column_key) {
        //            out[out_index].links.push_back({column_key.value, false});
        //            find_all_related_tables(out, *table.get_link_target(column_key), tables_in_filters);
        //            return false;
        //        });
    }
}

void DeepChangeChecker::find_filtered_related_tables(std::vector<RelatedTable>& out, Table const& table,
                                                     std::vector<KeyPathArray> key_path_arrays,
                                                     bool all_callback_have_filters)
{
    // If no callbacks have filters, use the current logic.
    if (key_path_arrays.size() == 0) {
        find_all_related_tables(out, table, {});
        return;
    }

    if (all_callback_have_filters) {
        // If all `callbacks` have keypath filters, `m_related_tables` is all tables which appear in
        // any of the filters.
        std::vector<TableKey> tables_in_filters = {};
        for (auto key_path_array : key_path_arrays) {
            for (auto key_path : key_path_array) {
                for (auto key_path_element : key_path) {
                    tables_in_filters.push_back(key_path_element.first);
                }
            }
        }
        find_all_related_tables(out, table, tables_in_filters);
    }
    else {
        // If some callbacks have filters, use the current logic and then add all tables present in
        // filters.
        // There could be additional tables that are not already part of the related tables in case
        // there are backlinks. Those are not included when not using a filter but will be when using
        // a filter.
        find_all_related_tables(out, table, {});
    }
}

DeepChangeChecker::DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                     std::vector<RelatedTable> const& related_tables,
                                     std::vector<KeyPathArray> key_path_arrays)
    : m_info(info)
    , m_root_table(root_table)
    , m_root_object_changes([&] {
        auto it = info.tables.find(root_table.get_key().value);
        return it != info.tables.end() ? &it->second : nullptr;
    }())
    , m_related_tables(related_tables)
    , m_key_path_arrays(key_path_arrays)
{
}

bool DeepChangeChecker::check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key,
                                             std::vector<ColKey> filtered_columns, size_t depth)
{
    // First we create an iterator pointing at the table identified by `table_key` within the `m_related_tables`.
    auto it = find_if(begin(m_related_tables), end(m_related_tables), [&](auto&& tbl) {
        return tbl.table_key == table_key;
    });
    // If no iterator could be found the table is not contained in `m_related_tables` and we cannot check any
    // outgoing links.
    if (it == m_related_tables.end())
        return false;
    // Likewise if the table could be found but does not have any (outgoing) links.
    if (it->links.empty())
        return false;

    // Check if we're already checking if the destination of the link is
    // modified, and if not add it to the stack
    auto already_checking = [&](int64_t col) {
        auto end = m_current_path.begin() + depth;
        auto match = std::find_if(m_current_path.begin(), end, [&](auto& p) {
            return p.obj_key == obj_key && p.col_key == col;
        });
        if (match != end) {
            for (; match < end; ++match)
                match->depth_exceeded = true;
            return true;
        }
        m_current_path[depth] = {obj_key, col, false};
        return false;
    };

    const Obj obj = table.get_object(ObjKey(obj_key));
    auto linked_object_changed = [&](OutgoingLink const& link) {
        if (already_checking(link.col_key))
            return false;
        if (ColKey(link.col_key).get_type() == col_type_BackLink) {
            // TODO
            return false;
        }
        if (!link.is_list) {
            if (obj.is_null(ColKey(link.col_key)))
                return false;
            auto object_key = obj.get<ObjKey>(ColKey(link.col_key)).value;
            return check_row(*table.get_link_target(ColKey(link.col_key)), object_key, filtered_columns, depth + 1);
        }

        auto& target = *table.get_link_target(ColKey(link.col_key));
        auto lvr = obj.get_linklist(ColKey(link.col_key));
        return std::any_of(lvr.begin(), lvr.end(), [&, this](auto key) {
            return this->check_row(target, key.value, filtered_columns, depth + 1);
        });
    };

    // Check the `links` of all `m_related_tables` and return true if any of them has a `linked_object_changed`.
    return std::any_of(begin(it->links), end(it->links), linked_object_changed);
}

bool DeepChangeChecker::check_row(Table const& table, ObjKeyType key, std::vector<ColKey> filtered_columns,
                                  size_t depth)
{
    // Arbitrary upper limit on the maximum depth to search
    if (depth >= m_current_path.size()) {
        // Don't mark any of the intermediate rows checked along the path as
        // not modified, as a search starting from them might hit a modification
        for (size_t i = 0; i < m_current_path.size(); ++i)
            m_current_path[i].depth_exceeded = true;
        return false;
    }

    TableKey table_key = table.get_key();

    // If the pair (table_key.value, key) can be found in `m_info.tables` we can
    // end the search and return here.
    if (depth > 0) {
        auto it = m_info.tables.find(table_key.value);
        if (it != m_info.tables.end() && it->second.modifications_contains(key, filtered_columns))
            return true;
    }

    // Look up the unmodified objects for the `table_key.value` and check if the
    // `key` can be found within them. If so, we can return without checking the
    // outgoing links.
    auto& not_modified = m_not_modified[table_key.value];
    auto it = not_modified.find(key);
    if (it != not_modified.end())
        return false;

    // If both of the above short cuts don't lead to a result we need to check the
    // outgoing links.
    bool ret = check_outgoing_links(table_key, table, key, filtered_columns, depth);
    if (!ret && (depth == 0 || !m_current_path[depth - 1].depth_exceeded))
        not_modified.insert(key);
    return ret;
}

bool DeepChangeChecker::operator()(ObjKeyType key)
{
    std::vector<ColKey> filtered_columns_in_root_table = {};
    std::vector<ColKey> filtered_columns = {};

    // If all callbacks do have a filter, every `KeyPathArray` will have entries.
    // In this case we need to check the `ColKey`s and pass the filtered columns
    // to the checker.
    // If at least one `Callback` does not have a filter we notify on any change.
    // This is signaled by leaving the `filtered_columns_in_root_table` and
    // `filtered_columns` empty.
    if (all_of(begin(m_key_path_arrays), end(m_key_path_arrays), [](auto key_path_array) {
            return key_path_array.size() > 0;
        })) {
        for (auto key_path_array : m_key_path_arrays) {
            for (auto key_path : key_path_array) {
                if (key_path.size() != 0) {
                    filtered_columns_in_root_table.push_back(key_path[0].second);
                }
                for (auto key_path_element : key_path) {
                    filtered_columns.push_back(key_path_element.second);
                }
            }
        }
    }

    // If the root object changes we do not need to iterate over every row since a notification needs to be sent
    // anyway.
    if (m_root_object_changes && m_root_object_changes->modifications_contains(key, filtered_columns_in_root_table)) {
        return true;
    }
    return check_row(m_root_table, key, filtered_columns, 0);
}
