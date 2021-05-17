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
#include <realm/list.hpp>

using namespace realm;
using namespace realm::_impl;

void DeepChangeChecker::find_related_tables(std::vector<RelatedTable>& out, Table const& table)
{
    auto table_key = table.get_key();
    if (any_of(begin(out), end(out), [=](auto& tbl) {
            return tbl.table_key == table_key;
        }))
        return;

    // We need to add this table to `out` before recurring so that the check
    // above works, but we can't store a pointer to the thing being populated
    // because the recursive calls may resize `out`, so instead look it up by
    // index every time
    size_t out_index = out.size();
    out.push_back({table_key, {}});

    for (auto col_key : table.get_column_keys()) {
        auto type = table.get_column_type(col_key);
        if (type == type_Link || type == type_LinkList) {
            out[out_index].links.push_back({col_key.value, type == type_LinkList});
            find_related_tables(out, *table.get_link_target(col_key));
        }
    }
}

DeepChangeChecker::DeepChangeChecker(TransactionChangeInfo const& info, Table const& root_table,
                                     std::vector<RelatedTable> const& related_tables)
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

bool DeepChangeChecker::check_outgoing_links(TableKey table_key, Table const& table, int64_t obj_key, size_t depth)
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
        if (!link.is_list) {
            ObjKey dst_key = obj.get<ObjKey>(ColKey(link.col_key));
            if (!dst_key) // do not descend into a null or unresolved link
                return false;
            return check_row(*table.get_link_target(ColKey(link.col_key)), dst_key.value, depth + 1);
        }

        auto& target = *table.get_link_target(ColKey(link.col_key));
        auto lvr = obj.get_linklist(ColKey(link.col_key));
        return std::any_of(lvr.begin(), lvr.end(), [&, this](auto key) {
            return this->check_row(target, key.value, depth + 1);
        });
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

    bool ret = check_outgoing_links(table_key, table, key, depth);
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
