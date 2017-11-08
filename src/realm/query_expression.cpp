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

namespace realm {

void Columns<Link>::evaluate(size_t index, ValueBase& destination)
{
    std::vector<size_t> links = m_link_map.get_links(index);
    Value<RowIndex> v = make_value_for_link<RowIndex>(m_link_map.only_unary_links(), links.size());

    for (size_t t = 0; t < links.size(); t++) {
        v.m_storage.set(t, RowIndex(links[t]));
    }
    destination.import(v);
}

void ColumnListBase::set_cluster(const Cluster* cluster)
{
    m_array_ptr = nullptr;
    // Create new Leaf
    m_array_ptr = LeafPtr(new (&m_leaf_cache_storage) Array(m_link_map.base_table()->get_alloc()));
    cluster->init_leaf(this->m_column_ndx, m_array_ptr.get());
    m_leaf_ptr = m_array_ptr.get();
}

void ColumnListBase::get_lists(size_t index, Value<ref_type>& destination, size_t nb_elements)
{
    if (m_link_map.m_link_columns.size() > 0) {
        std::vector<size_t> links = m_link_map.get_links(index);
        auto sz = links.size();

        if (m_link_map.only_unary_links()) {
            ref_type val = 0;
            if (sz == 1) {
                val = to_ref(m_leaf_ptr->get(links[0]));
            }
            destination.init(false, 1, val);
        }
        else {
            destination.init(true, sz);
            for (size_t t = 0; t < sz; t++) {
                destination.m_storage.set(t, to_ref(m_leaf_ptr->get(links[t])));
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
