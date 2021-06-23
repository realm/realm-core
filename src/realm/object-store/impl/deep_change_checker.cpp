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
#include <realm/table.hpp>
#include <realm/dictionary.hpp>
#include <realm/list.hpp>
#include <realm/set.hpp>

using namespace realm;
using namespace realm::_impl;

void DeepChangeChecker::find_related_tables(DeepChangeChecker::RelatedTables& out, Table const& table)
{
    struct LinkInfo {
        std::vector<ColKey> link_columns;
        std::vector<TableKey> connected_tables;
    };

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
        auto incoming_link_columns = cur_table->get_incoming_link_columns();
        for (auto& incoming_link : incoming_link_columns) {
            REALM_ASSERT(incoming_link.first);
            auto& links = complete_mapping[incoming_link.first];
            links.link_columns.push_back(incoming_link.second);
            links.connected_tables.push_back(cur_table->get_key());
        }
    }

    // Remove duplicates:
    // duplicates in link_columns can occur when a Mixed(TypedLink) contain links to different tables
    // duplicates in connected_tables can occur when there are different link paths to the same table
    for (auto& kv_pair : complete_mapping) {
        auto& cols = kv_pair.second.link_columns;
        std::sort(cols.begin(), cols.end());
        cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
        auto& tables = kv_pair.second.connected_tables;
        std::sort(tables.begin(), tables.end());
        tables.erase(std::unique(tables.begin(), tables.end()), tables.end());
    }

    std::vector<TableKey> tables_to_check = {table.get_key()};
    auto get_out_relationships_for = [&out](TableKey key) -> std::vector<ColKey>& {
        auto it = find_if(begin(out), end(out), [&](auto&& tbl) {
            return tbl.table_key == key;
        });
        if (it == out.end()) {
            it = out.insert(out.end(), {key, {}});
        }
        return it->links;
    };

    while (tables_to_check.size()) {
        auto table_key_to_check = *tables_to_check.begin();
        tables_to_check.erase(tables_to_check.begin());
        auto outgoing_links = complete_mapping[table_key_to_check];
        auto& out_relations = get_out_relationships_for(table_key_to_check);
        auto& link_columns = complete_mapping[table_key_to_check].link_columns;
        out_relations.insert(out_relations.end(), link_columns.begin(), link_columns.end());
        for (auto linked_table_key : outgoing_links.connected_tables) {
            if (std::find_if(begin(out), end(out), [&](auto&& relation) {
                    return relation.table_key == linked_table_key;
                }) == out.end()) {
                tables_to_check.push_back(linked_table_key);
            }
        }
    }
}

DeepChangeChecker::DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                     DeepChangeChecker::RelatedTables const& related_tables)
    : m_info(info)
    , m_root_table(root_table)
    , m_root_table_key(root_table.get_key().value)
    , m_root_object_changes([&] {
        auto it = info.tables.find(m_root_table_key.value);
        return it != info.tables.end() ? &it->second : nullptr;
    }())
    , m_related_tables(related_tables)
{
}

template <typename T>
bool DeepChangeChecker::do_check_mixed_for_link(T* coll, TableRef& cached_linked_table, Mixed value, size_t depth)
{
    if (value.is_type(type_TypedLink)) {
        auto link = value.get_link();
        if (!link.is_unresolved()) {
            if (!cached_linked_table || cached_linked_table->get_key() != link.get_table_key()) {
                cached_linked_table = coll->get_table()->get_parent_group()->get_table(link.get_table_key());
                REALM_ASSERT_EX(cached_linked_table, link.get_table_key().value);
            }
            return check_row(*cached_linked_table, link.get_obj_key().value, depth + 1);
        }
    }
    return false;
}

template <typename T>
bool DeepChangeChecker::do_check_for_collection_of_mixed(T* coll, size_t depth)
{
    TableRef cached_linked_table;
    return std::any_of(coll->begin(), coll->end(), [&](auto value) {
        return do_check_mixed_for_link(coll, cached_linked_table, value, depth);
    });
}

bool DeepChangeChecker::do_check_for_collection_modifications(std::unique_ptr<CollectionBase> coll, size_t depth)
{
    REALM_ASSERT(coll);
    if (auto lst = dynamic_cast<LnkLst*>(coll.get())) {
        TableRef target = lst->get_target_table();
        return std::any_of(lst->begin(), lst->end(), [&](auto key) {
            return check_row(*target, key.value, depth + 1);
        });
    }
    else if (auto list = dynamic_cast<Lst<Mixed>*>(coll.get())) {
        return do_check_for_collection_of_mixed(list, depth);
    }
    else if (auto dict = dynamic_cast<Dictionary*>(coll.get())) {
        TableRef cached_linked_table;
        return std::any_of(dict->begin(), dict->end(), [&](auto key_value_pair) {
            Mixed value = key_value_pair.second;
            // Here we rely on Dictionaries storing all links as a TypedLink
            // even if the dictionary is set to a single object type.
            REALM_ASSERT(!value.is_type(type_Link));
            return do_check_mixed_for_link(dict, cached_linked_table, value, depth);
        });
    }
    else if (auto set = dynamic_cast<LnkSet*>(coll.get())) {
        auto target = set->get_target_table();
        REALM_ASSERT(target);
        return std::any_of(set->begin(), set->end(), [&](auto obj_key) {
            return obj_key && check_row(*target, obj_key.value, depth + 1);
        });
    }
    else if (auto set = dynamic_cast<Set<Mixed>*>(coll.get())) {
        return do_check_for_collection_of_mixed(set, depth);
    }
    // at this point, we have not handled all datatypes
    REALM_UNREACHABLE();
    return false;
}

bool DeepChangeChecker::check_outgoing_links(TableKey table_key, Table const& table, ObjKey obj_key, size_t depth)
{
    auto it = find_if(begin(m_related_tables), end(m_related_tables), [&](auto&& tbl) {
        return tbl.table_key == table_key;
    });

    if (it == m_related_tables.end())
        return false;
    if (it->links.empty())
        return false;

    // Check if we're already checking if the destination of the link is
    // modified, and if not add it to the stack
    auto already_checking = [&](ColKey col) {
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
    auto linked_object_changed = [&](ColKey const& outgoing_link_column) {
        if (already_checking(outgoing_link_column))
            return false;
        if (!outgoing_link_column.is_collection()) {
            ObjKey dst_key = obj.get<ObjKey>(outgoing_link_column);
            if (!dst_key) // do not descend into a null or unresolved link
                return false;
            return check_row(*table.get_link_target(outgoing_link_column), dst_key.value, depth + 1);
        }
        auto collection_ptr = obj.get_collection_ptr(outgoing_link_column);
        return do_check_for_collection_modifications(std::move(collection_ptr), depth);
    };

    return std::any_of(begin(it->links), end(it->links), linked_object_changed);
}

bool DeepChangeChecker::check_row(Table const& table, ObjKeyType key, size_t depth)
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
    if (depth > 0) {
        auto it = m_info.tables.find(table_key.value);
        if (it != m_info.tables.end() && it->second.modifications_contains(key))
            return true;
    }
    auto& not_modified = m_not_modified[table_key.value];
    auto it = not_modified.find(key);
    if (it != not_modified.end())
        return false;

    bool ret = check_outgoing_links(table_key, table, ObjKey(key), depth);
    if (!ret && (depth == 0 || !m_current_path[depth - 1].depth_exceeded))
        not_modified.insert(key);
    return ret;
}

bool DeepChangeChecker::operator()(ObjKeyType key)
{
    if (m_root_object_changes && m_root_object_changes->modifications_contains(key))
        return true;
    return check_row(m_root_table, key, 0);
}
