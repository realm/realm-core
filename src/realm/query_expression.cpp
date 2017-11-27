/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <realm/query_expression.hpp>
#include <realm/group.hpp>

namespace realm {

void LinkMap::set_base_table(const Table* table)
{
    if (table == base_table())
        return;

    m_tables.clear();
    m_link_column_names.clear();
    m_tables.push_back(table);
    m_link_types.clear();
    m_only_unary_links = true;

    Group* group = _impl::TableFriend::get_parent_group(*table);

    for (size_t i = 0; i < m_link_column_indexes.size(); i++) {
        size_t link_column_index = m_link_column_indexes[i];
        // Link column can be either LinkList or single Link
        const Table* t = m_tables.back();
        const Spec& spec = _impl::TableFriend::get_spec(*t);

        ColumnType type = t->get_real_column_type(link_column_index);
        REALM_ASSERT(Table::is_link_type(type) || type == col_type_BackLink);
        if (type == col_type_LinkList || type == col_type_BackLink) {
            m_only_unary_links = false;
        }

        m_link_types.push_back(type);
        m_link_column_names.emplace_back(spec.get_column_name(link_column_index));
        size_t target_table_index = spec.get_opposite_link_table_ndx(link_column_index);
        TableRef tt = group->get_table(target_table_index);
        m_tables.push_back(tt.get());
    }
}

std::string LinkMap::description() const
{
    std::string s;
    for (size_t i = 0; i < m_link_column_indexes.size(); ++i) {
        if (i < m_tables.size() && m_tables[i]) {
            if (i == 0) {
                s += std::string(m_tables[i]->get_name()) + metrics::value_separator;
            }
            if (m_link_types[i] == col_type_BackLink) {
                s += "backlink";
            }
            else if (m_link_column_indexes[i] < m_tables[i]->get_column_count()) {
                s += std::string(m_tables[i]->get_column_name(m_link_column_indexes[i]));
            }
            if (i != m_link_column_indexes.size() - 1) {
                s += metrics::value_separator;
            }
        }
    }
    return s;
}

void LinkMap::map_links(size_t column, Key key, LinkMapFunction& lm)
{
    bool last = (column + 1 == m_link_column_indexes.size());
    ColumnType type = m_link_types[column];
    ConstObj obj = m_tables[column]->get_object(key);
    if (type == col_type_Link) {
        if (Key k = obj.get<Key>(m_link_column_indexes[column])) {
            if (last)
                lm.consume(k);
            else
                map_links(column + 1, k, lm);
        }
    }
    else if (type == col_type_LinkList) {
        auto linklist = obj.get_list<Key>(m_link_column_indexes[column]);
        size_t sz = linklist.size();
        for (size_t t = 0; t < sz; t++) {
            Key k = linklist.get(t);
            if (last) {
                bool continue2 = lm.consume(k);
                if (!continue2)
                    return;
            }
            else
                map_links(column + 1, k, lm);
        }
    }
    else if (type == col_type_BackLink) {
        auto backlink_column = m_link_column_indexes[column];
        size_t sz = obj.get_backlink_count(backlink_column);
        for (size_t t = 0; t < sz; t++) {
            Key k = obj.get_backlink(backlink_column, t);
            if (last) {
                bool continue2 = lm.consume(k);
                if (!continue2)
                    return;
            }
            else
                map_links(column + 1, k, lm);
        }
    }
    else {
        REALM_ASSERT(false);
    }
}

void LinkMap::map_links(size_t column, size_t row, LinkMapFunction& lm)
{
    REALM_ASSERT(m_leaf_ptr != nullptr);

    bool last = (column + 1 == m_link_column_indexes.size());
    ColumnType type = m_link_types[column];
    if (type == col_type_Link) {
        auto keys = reinterpret_cast<const ArrayKey*>(m_leaf_ptr);
        if (Key k = keys->get(row)) {
            if (last)
                lm.consume(k);
            else
                map_links(column + 1, k, lm);
        }
    }
    else if (type == col_type_LinkList) {
        if (ref_type ref = m_leaf_ptr->get_as_ref(row)) {
            ArrayKey arr(base_table()->get_alloc());
            arr.init_from_ref(ref);
            size_t sz = arr.size();
            for (size_t t = 0; t < sz; t++) {
                Key k = arr.get(t);
                if (last) {
                    bool continue2 = lm.consume(k);
                    if (!continue2)
                        return;
                }
                else
                    map_links(column + 1, k, lm);
            }
        }
    }
    else if (type == col_type_BackLink) {
        auto back_links = reinterpret_cast<const ArrayBacklink*>(m_leaf_ptr);
        size_t sz = back_links->get_backlink_count(row);
        for (size_t t = 0; t < sz; t++) {
            Key k = back_links->get_backlink(row, t);
            if (last) {
                bool continue2 = lm.consume(k);
                if (!continue2)
                    return;
            }
            else
                map_links(column + 1, k, lm);
        }
    }
    else {
        REALM_ASSERT(false);
    }
}

void Columns<Link>::evaluate(size_t index, ValueBase& destination)
{
    // Destination must be of Key type. It only makes sense to
    // compare keys with keys
    auto d = dynamic_cast<Value<Key>*>(&destination);
    REALM_ASSERT(d != nullptr);
    std::vector<Key> links = m_link_map.get_links(index);

    if (m_link_map.only_unary_links()) {
        Key key;
        if (!links.empty()) {
            key = links[0];
        }
        d->init(false, 1, key);
    }
    else {
        d->init(true, links);
    }
}

void ColumnListBase::set_cluster(const Cluster* cluster)
{
    m_leaf_ptr = nullptr;
    m_array_ptr = nullptr;
    if (m_link_map.has_links()) {
        m_link_map.set_cluster(cluster);
    }
    else {
        // Create new Leaf
        m_array_ptr = LeafPtr(new (&m_leaf_cache_storage) Array(m_link_map.base_table()->get_alloc()));
        cluster->init_leaf(this->m_column_ndx, m_array_ptr.get());
        m_leaf_ptr = m_array_ptr.get();
    }
}

void ColumnListBase::get_lists(size_t index, Value<ref_type>& destination, size_t nb_elements)
{
    if (m_link_map.has_links()) {
        std::vector<Key> links = m_link_map.get_links(index);
        auto sz = links.size();

        if (m_link_map.only_unary_links()) {
            ref_type val = 0;
            if (sz == 1) {
                ConstObj obj = m_link_map.target_table()->get_object(links[0]);
                val = to_ref(obj.get<int64_t>(m_column_ndx));
            }
            destination.init(false, 1, val);
        }
        else {
            destination.init(true, sz);
            for (size_t t = 0; t < sz; t++) {
                ConstObj obj = m_link_map.target_table()->get_object(links[t]);
                ref_type val = to_ref(obj.get<int64_t>(m_column_ndx));
                destination.m_storage.set(t, val);
            }
        }
    }
    else {
        size_t rows = std::min(m_leaf_ptr->size() - index, nb_elements);

        destination.init(false, rows);

        for (size_t t = 0; t < rows; t++) {
            destination.m_storage.set(t, to_ref(m_leaf_ptr->get(index + t)));
        }
    }
}

Query Subexpr2<StringData>::equal(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Equal, EqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::equal(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Equal, EqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::not_equal(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, NotEqual, NotEqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::not_equal(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<NotEqual, NotEqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::begins_with(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, BeginsWith, BeginsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::begins_with(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<BeginsWith, BeginsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::ends_with(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, EndsWith, EndsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::ends_with(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<EndsWith, EndsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::contains(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Contains, ContainsIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::contains(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Contains, ContainsIns>(*this, col, case_sensitive);
}

Query Subexpr2<StringData>::like(StringData sd, bool case_sensitive)
{
    return string_compare<StringData, Like, LikeIns>(*this, sd, case_sensitive);
}

Query Subexpr2<StringData>::like(const Subexpr2<StringData>& col, bool case_sensitive)
{
    return string_compare<Like, LikeIns>(*this, col, case_sensitive);
}
}
