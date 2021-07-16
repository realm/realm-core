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
#include <realm/dictionary.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>
#include <realm/table.hpp>

using namespace realm;
using namespace realm::_impl;

namespace {
template <typename T>
void sort_and_unique(T& container)
{
    std::sort(container.begin(), container.end());
    container.erase(std::unique(container.begin(), container.end()), container.end());
}
} // namespace

void DeepChangeChecker::find_related_tables(std::vector<RelatedTable>& related_tables, Table const& table,
                                            const KeyPathArray& key_path_array)
{
    struct LinkInfo {
        std::vector<ColKey> forward_links;
        std::vector<TableKey> forward_tables;

        std::vector<TableKey> backlink_tables;
        bool processed_table = false;
    };

    auto has_key_paths = std::any_of(begin(key_path_array), end(key_path_array), [&](auto key_path) {
        return key_path.size() > 0;
    });

    // Build up the complete forward mapping from the back links.
    // Following forward link columns does not account for TypedLink
    // values as part of Dictionary<String, Mixed> for example. But
    // we do not want to assume that all Mixed columns contain links,
    // so we rely on the fact that if there are any TypedLinks from a
    // Mixed value, there will be a corresponding backlink column
    // created at the destination table.
    std::unordered_map<TableKey, LinkInfo> complete_mapping;
    Group* group = table.get_parent_group();
    REALM_ASSERT(group);
    auto all_table_keys = group->get_table_keys();
    for (auto key : all_table_keys) {
        auto cur_table = group->get_table(key);
        REALM_ASSERT(cur_table);

        LinkInfo* backlinks = nullptr;
        if (has_key_paths) {
            backlinks = &complete_mapping[cur_table->get_key()];
        }
        cur_table->for_each_backlink_column([&](ColKey backlink_col_key) {
            auto origin_table_key = cur_table->get_opposite_table_key(backlink_col_key);
            auto origin_link_col = cur_table->get_opposite_column(backlink_col_key);
            auto& links = complete_mapping[origin_table_key];
            links.forward_links.push_back(origin_link_col);
            links.forward_tables.push_back(cur_table->get_key());

            if (any_of(key_path_array.begin(), key_path_array.end(), [&](const KeyPath& key_path) {
                    return any_of(key_path.begin(), key_path.end(), [&](std::pair<TableKey, ColKey> pair) {
                        return pair.first == cur_table->get_key() && pair.second == backlink_col_key;
                    });
                })) {
                backlinks->backlink_tables.push_back(cur_table->get_link_target(backlink_col_key)->get_key());
            }
            return false;
        });
    }

    // Remove duplicates:
    // duplicates in link_columns can occur when a Mixed(TypedLink) contain links to different tables
    // duplicates in connected_tables can occur when there are different link paths to the same table
    for (auto& [_, info] : complete_mapping) {
        sort_and_unique(info.forward_links);
        sort_and_unique(info.forward_tables);
    }

    std::vector<TableKey> tables_to_check = {table.get_key()};
    while (tables_to_check.size()) {
        auto table_key_to_check = tables_to_check.back();
        tables_to_check.pop_back();
        auto& link_info = complete_mapping[table_key_to_check];
        if (link_info.processed_table) {
            continue;
        }
        link_info.processed_table = true;

        related_tables.push_back({table_key_to_check, std::move(link_info.forward_links)});

        // Add all tables reachable via a forward link to the vector of tables that need to be checked
        for (auto linked_table_key : link_info.forward_tables) {
            tables_to_check.push_back(linked_table_key);
        }

        // Backlinks can only come into consideration when added via key paths.
        if (has_key_paths) {
            for (auto linked_table_key : link_info.backlink_tables) {
                tables_to_check.push_back(linked_table_key);
            }
        }
    }
}

DeepChangeChecker::DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                     DeepChangeChecker::RelatedTables const& related_tables,
                                     const KeyPathArray& key_path_array, bool all_callbacks_filtered)
    : m_info(info)
    , m_root_table(root_table)
    , m_key_path_array(key_path_array)
    , m_root_object_changes([&] {
        auto it = info.tables.find(root_table.get_key().value);
        return it != info.tables.end() ? &it->second : nullptr;
    }())
    , m_related_tables(related_tables)
{
    // If all callbacks do have a filter, every `KeyPathArray` will have entries.
    // In this case we need to check the `ColKey`s and pass the filtered columns
    // to the checker.
    // If at least one `NotificationCallback` does not have a filter we notify on any change.
    // This is signaled by leaving the `m_filtered_columns_in_root_table` and
    // `m_filtered_columns` empty.
    if (all_callbacks_filtered) {
        for (const auto& key_path : key_path_array) {
            if (key_path.size() != 0) {
                m_filtered_columns_in_root_table.push_back(key_path[0].second);
            }
            for (const auto& key_path_element : key_path) {
                m_filtered_columns.push_back(key_path_element.second);
            }
        }
    }
}

template <typename T>
bool DeepChangeChecker::do_check_mixed_for_link(T* coll, TableRef& cached_linked_table, Mixed value,
                                                const std::vector<ColKey>& filtered_columns, size_t depth)
{
    if (value.is_type(type_TypedLink)) {
        auto link = value.get_link();
        if (!link.is_unresolved()) {
            if (!cached_linked_table || cached_linked_table->get_key() != link.get_table_key()) {
                cached_linked_table = coll->get_table()->get_parent_group()->get_table(link.get_table_key());
                REALM_ASSERT_EX(cached_linked_table, link.get_table_key().value);
            }
            return check_row(*cached_linked_table, link.get_obj_key().value, filtered_columns, depth + 1);
        }
    }
    return false;
}

template <typename T>
bool DeepChangeChecker::do_check_for_collection_of_mixed(T* coll, const std::vector<ColKey>& filtered_columns,
                                                         size_t depth)
{
    TableRef cached_linked_table;
    return std::any_of(coll->begin(), coll->end(), [&](auto value) {
        return do_check_mixed_for_link(coll, cached_linked_table, value, filtered_columns, depth);
    });
}

bool DeepChangeChecker::do_check_for_collection_modifications(std::unique_ptr<CollectionBase> coll,
                                                              const std::vector<ColKey>& filtered_columns,
                                                              size_t depth)
{
    REALM_ASSERT(coll);
    if (auto lst = dynamic_cast<LnkLst*>(coll.get())) {
        TableRef target = lst->get_target_table();
        return std::any_of(lst->begin(), lst->end(), [&](auto key) {
            return check_row(*target, key.value, filtered_columns, depth + 1);
        });
    }
    else if (auto list = dynamic_cast<Lst<Mixed>*>(coll.get())) {
        return do_check_for_collection_of_mixed(list, filtered_columns, depth);
    }
    else if (auto dict = dynamic_cast<Dictionary*>(coll.get())) {
        TableRef cached_linked_table;
        return std::any_of(dict->begin(), dict->end(), [&](auto key_value_pair) {
            Mixed value = key_value_pair.second;
            // Here we rely on Dictionaries storing all links as a TypedLink
            // even if the dictionary is set to a single object type.
            REALM_ASSERT(!value.is_type(type_Link));
            return do_check_mixed_for_link(dict, cached_linked_table, value, filtered_columns, depth);
        });
    }
    else if (auto set = dynamic_cast<LnkSet*>(coll.get())) {
        auto target = set->get_target_table();
        REALM_ASSERT(target);
        return std::any_of(set->begin(), set->end(), [&](auto obj_key) {
            return obj_key && check_row(*target, obj_key.value, filtered_columns, depth + 1);
        });
    }
    else if (auto set = dynamic_cast<Set<Mixed>*>(coll.get())) {
        return do_check_for_collection_of_mixed(set, filtered_columns, depth);
    }
    // at this point, we have not handled all datatypes
    REALM_UNREACHABLE();
    return false;
}

bool DeepChangeChecker::check_outgoing_links(Table const& table, ObjKey obj_key,
                                             const std::vector<ColKey>& filtered_columns, size_t depth)
{
    auto table_key = table.get_key();

    // First we create an iterator pointing at the table identified by `table_key` within the `m_related_tables`.
    auto it = std::find_if(begin(m_related_tables), end(m_related_tables), [&](const auto& related_table) {
        return related_table.table_key == table_key;
    });
    if (it == m_related_tables.end())
        return false;
    // Likewise if the table could be found but does not have any (outgoing) links.
    if (it->links.empty())
        return false;

    // Check if we're already checking if the destination of the link is
    // modified, and if not add it to the stack
    auto already_checking = [&](ColKey col) {
        auto end = m_current_path.begin() + depth;
        auto match = std::find_if(m_current_path.begin(), end, [&](const auto& p) {
            return p.obj_key == obj_key && p.col_key == col;
        });
        if (match != end) {
            for (; match < end; ++match) {
                match->depth_exceeded = true;
            }
            return true;
        }
        m_current_path[depth] = {obj_key, col, false};
        return false;
    };

    const Obj obj = table.get_object(ObjKey(obj_key));
    auto linked_object_changed = [&](ColKey const& outgoing_link_column) {
        if (already_checking(outgoing_link_column))
            return false;
        if (!outgoing_link_column.is_collection()) {
            ObjKey dst_key = obj.get<ObjKey>(outgoing_link_column);
            if (!dst_key) // do not descend into a null or unresolved link
                return false;
            return check_row(*table.get_link_target(outgoing_link_column), dst_key.value, filtered_columns,
                             depth + 1);
        }
        auto collection_ptr = obj.get_collection_ptr(outgoing_link_column);
        return do_check_for_collection_modifications(std::move(collection_ptr), filtered_columns, depth);
    };

    // Check the `links` of all `m_related_tables` and return true if any of them has a `linked_object_changed`.
    return std::any_of(begin(it->links), end(it->links), linked_object_changed);
}

bool DeepChangeChecker::check_row(Table const& table, ObjKeyType object_key,
                                  const std::vector<ColKey>& filtered_columns, size_t depth)
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
        if (it != m_info.tables.end() && it->second.modifications_contains(object_key, filtered_columns))
            return true;
    }

    // Look up the unmodified objects for the `table_key.value` and check if the
    // `key` can be found within them. If so, we can return without checking the
    // outgoing links.
    auto& not_modified = m_not_modified[table_key.value];
    auto it = not_modified.find(object_key);
    if (it != not_modified.end())
        return false;

    bool ret = check_outgoing_links(table, ObjKey(object_key), filtered_columns, depth);
    if (!ret && (depth == 0 || !m_current_path[depth - 1].depth_exceeded))
        not_modified.insert(object_key);
    return ret;
}

bool DeepChangeChecker::operator()(ObjKeyType key)
{
    // If the root object changed we do not need to iterate over every row since a notification needs to be sent
    // anyway.
    if (m_root_object_changes &&
        m_root_object_changes->modifications_contains(key, m_filtered_columns_in_root_table)) {
        return true;
    }

    return check_row(m_root_table, key, m_filtered_columns, 0);
}

CollectionKeyPathChangeChecker::CollectionKeyPathChangeChecker(TransactionChangeInfo const& info,
                                                               Table const& root_table,
                                                               std::vector<RelatedTable> const& related_tables,
                                                               const KeyPathArray& key_path_array,
                                                               bool all_callbacks_filtered)
    : DeepChangeChecker(info, root_table, related_tables, key_path_array, all_callbacks_filtered)
{
}

bool CollectionKeyPathChangeChecker::operator()(ObjKeyType object_key)
{
    std::vector<int64_t> changed_columns;

    for (auto& key_path : m_key_path_array) {
        find_changed_columns(changed_columns, key_path, 0, m_root_table, object_key);
    }

    return changed_columns.size() > 0;
}

void CollectionKeyPathChangeChecker::find_changed_columns(std::vector<int64_t>& changed_columns,
                                                          const KeyPath& key_path, size_t depth, const Table& table,
                                                          const ObjKeyType& object_key_value)
{

    if (depth >= key_path.size()) {
        // We've reached the end of the key path.

        // For the special case of having a backlink at the end of a key path we need to check this level too.
        // Modifications to a backlink are found via the insertions on the origin table (which we are in right
        // now).
        auto last_key_path_element = key_path[key_path.size() - 1];
        auto last_column_key = last_key_path_element.second;
        if (last_column_key.get_type() == col_type_BackLink) {
            auto iterator = m_info.tables.find(table.get_key().value);
            if (iterator != m_info.tables.end() && !iterator->second.insertions_empty()) {
                auto root_column_key = key_path[0].second;
                changed_columns.push_back(root_column_key.value);
            }
        }

        return;
    }

    auto [table_key, column_key] = key_path.at(depth);

    // Check for a change on the current depth level.
    auto iterator = m_info.tables.find(table_key.value);
    if (iterator != m_info.tables.end() && (iterator->second.modifications_contains(object_key_value, {column_key}) ||
                                            iterator->second.insertions_contains(object_key_value))) {
        // If an object linked to the root object was changed we only mark the
        // property of the root objects as changed.
        // This is also the reason why we can return right after doing so because we would only mark the same root
        // property again in case we find another change deeper down the same path.
        auto root_column_key = key_path[0].second;
        changed_columns.push_back(root_column_key.value);
        return;
    }

    // Only continue for any kind of link.
    auto column_type = column_key.get_type();
    if (column_type != col_type_Link && column_type != col_type_LinkList && column_type != col_type_BackLink &&
        column_type != col_type_TypedLink && column_type != col_type_Mixed) {
        return;
    }

    auto check_mixed_object = [&](const Mixed& mixed_object) {
        if (mixed_object.is_type(type_Link, type_TypedLink)) {
            auto object_key = mixed_object.get<ObjKey>();
            auto target_table_key = mixed_object.get_link().get_table_key();
            Group* group = table.get_parent_group();
            auto target_table = group->get_table(target_table_key);
            find_changed_columns(changed_columns, key_path, depth + 1, *target_table, object_key.value);
        }
    };

    // Advance one level deeper into the key path.
    auto object = table.get_object(ObjKey(object_key_value));
    if (column_key.is_list()) {
        if (column_type == col_type_Mixed) {
            auto list = object.get_list<Mixed>(column_key);
            for (size_t i = 0; i < list.size(); i++) {
                auto target_object = list.get_any(i);
                check_mixed_object(target_object);
            }
        }
        else {
            REALM_ASSERT(column_type == col_type_Link || column_type == col_type_LinkList);
            auto list = object.get_linklist(column_key);
            auto target_table = table.get_link_target(column_key);
            for (size_t i = 0; i < list.size(); i++) {
                auto target_object = list.get(i);
                find_changed_columns(changed_columns, key_path, depth + 1, *target_table, target_object.value);
            }
        }
    }
    else if (column_key.is_set()) {
        if (column_type == col_type_Mixed) {
            auto set = object.get_set<Mixed>(column_key);
            for (size_t i = 0; i < set.size(); i++) {
                auto target_object = set.get(i);
                check_mixed_object(target_object);
            }
        }
        else {
            REALM_ASSERT(column_type == col_type_Link || column_type == col_type_LinkList);
            auto set = object.get_linkset(column_key);
            auto target_table = table.get_link_target(column_key);
            for (size_t i = 0; i < set.size(); i++) {
                auto target_object = set.get(i);
                find_changed_columns(changed_columns, key_path, depth + 1, *target_table, target_object.value);
            }
        }
    }
    else if (column_key.is_dictionary()) {
        if (column_type == col_type_Mixed) {
            auto dictionary = object.get_dictionary(column_key);
            for (size_t i = 0; i < dictionary.size(); i++) {
                auto target_object = dictionary.get(dictionary.get_key(i));
                check_mixed_object(target_object);
            }
        }
        else {
            REALM_ASSERT(column_type == col_type_Link || column_type == col_type_LinkList);
            auto dictionary = object.get_dictionary(column_key);
            auto linked_dictionary = std::make_unique<DictionaryLinkValues>(dictionary);
            auto target_table = table.get_link_target(column_key);
            for (size_t i = 0; i < linked_dictionary->size(); i++) {
                auto target_object = linked_dictionary->get_key(i);
                find_changed_columns(changed_columns, key_path, depth + 1, *target_table, target_object.value);
            }
        }
    }
    else if (column_type == col_type_Link) {
        // A forward link will only have one target object.
        auto target_object = object.get<ObjKey>(column_key);
        auto target_table = table.get_link_target(column_key);
        find_changed_columns(changed_columns, key_path, depth + 1, *target_table, target_object.value);
    }
    else if (column_type == col_type_BackLink) {
        // A backlink can have multiple origin objects. We need to iterate over all of them.
        auto origin_table = table.get_opposite_table(column_key);
        auto origin_column_key = table.get_opposite_column(column_key);
        size_t backlink_count = object.get_backlink_count(*origin_table, origin_column_key);
        for (size_t i = 0; i < backlink_count; i++) {
            auto origin_object = object.get_backlink(*origin_table, origin_column_key, i);
            find_changed_columns(changed_columns, key_path, depth + 1, *origin_table, origin_object.value);
        }
    }
    else {
        REALM_UNREACHABLE(); // unhandled column type
    }
}

ObjectKeyPathChangeChecker::ObjectKeyPathChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                                       std::vector<RelatedTable> const& related_tables,
                                                       const KeyPathArray& key_path_array,
                                                       bool all_callbacks_filtered)
    : CollectionKeyPathChangeChecker(info, root_table, related_tables, key_path_array, all_callbacks_filtered)
{
}

std::vector<int64_t> ObjectKeyPathChangeChecker::operator()(ObjKeyType object_key)
{
    std::vector<int64_t> changed_columns;

    for (auto& key_path : m_key_path_array) {
        find_changed_columns(changed_columns, key_path, 0, m_root_table, object_key);
    }

    return changed_columns;
}
