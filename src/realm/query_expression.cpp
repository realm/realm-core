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

std::vector<size_t> LinkMap::get_origin_ndxs(size_t index, size_t column) const
{
    if (column == m_link_columns.size()) {
        return {index};
    }
    std::vector<size_t> ndxs = get_origin_ndxs(index, column + 1);
    std::vector<size_t> ret;
    auto origin_col = m_link_column_indexes[column];
    auto origin = m_tables[column];
    auto link_type = m_link_types[column];
    if (link_type == col_type_BackLink) {
        auto table_ndx = origin->m_spec->get_opposite_link_table_ndx(origin_col);
        auto link_table = origin->get_parent_group()->get_table(table_ndx);
        size_t link_col_ndx = origin->m_spec->get_origin_column_ndx(origin_col);
        auto forward_type = link_table->get_column_type(link_col_ndx);

        for (size_t ndx : ndxs) {
            if (forward_type == type_Link) {
                ret.push_back(link_table->get_link(link_col_ndx, ndx));
            }
            else {
                REALM_ASSERT(forward_type == type_LinkList);
                auto ll = link_table->get_linklist(link_col_ndx, ndx);
                auto sz = ll->size();
                for (size_t i = 0; i < sz; i++) {
                    ret.push_back(ll->get(i).get_index());
                }
            }
        }
    }
    else {
        auto target = m_tables[column + 1];
        for (size_t ndx : ndxs) {
            auto cnt = target->get_backlink_count(ndx, *origin, origin_col);
            for (size_t i = 0; i < cnt; i++) {
                ret.push_back(target->get_backlink(ndx, *origin, origin_col, i));
            }
        }
    }
    return ret;
}

void Columns<Link>::evaluate(size_t index, ValueBase& destination)
{
    std::vector<size_t> links = m_link_map.get_links(index);
    Value<RowIndex> v = make_value_for_link<RowIndex>(m_link_map.only_unary_links(), links.size());

    for (size_t t = 0; t < links.size(); t++) {
        v.m_storage.set(t, RowIndex(links[t]));
    }
    destination.import(v);
}

void Columns<SubTable>::evaluate_internal(size_t index, ValueBase& destination, size_t nb_elements)
{
    REALM_ASSERT_DEBUG(dynamic_cast<Value<ConstTableRef>*>(&destination) != nullptr);
    Value<ConstTableRef>* d = static_cast<Value<ConstTableRef>*>(&destination);
    REALM_ASSERT(d);

    if (m_link_map.m_link_columns.size() > 0) {
        std::vector<size_t> links = m_link_map.get_links(index);
        auto sz = links.size();

        if (m_link_map.only_unary_links()) {
            ConstTableRef val;
            if (sz == 1) {
                val = m_column->get(links[0]);
            }
            d->init(false, 1, val);
        }
        else {
            d->init(true, sz);
            for (size_t t = 0; t < sz; t++) {
                d->m_storage.set(t, m_column->get(links[t]));
            }
        }
    }
    else {
        size_t rows = std::min(m_column->size() - index, nb_elements);

        d->init(false, rows);

        for (size_t t = 0; t < rows; t++) {
            d->m_storage.set(t, m_column->get(index + t));
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


// BinaryData

Query Subexpr2<BinaryData>::equal(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Equal, EqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::equal(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Equal, EqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::not_equal(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, NotEqual, NotEqualIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::not_equal(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<NotEqual, NotEqualIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::begins_with(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, BeginsWith, BeginsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::begins_with(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<BeginsWith, BeginsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::ends_with(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, EndsWith, EndsWithIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::ends_with(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<EndsWith, EndsWithIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::contains(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Contains, ContainsIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::contains(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Contains, ContainsIns>(*this, col, case_sensitive);
}

Query Subexpr2<BinaryData>::like(BinaryData sd, bool case_sensitive)
{
    return binary_compare<BinaryData, Like, LikeIns>(*this, sd, case_sensitive);
}

Query Subexpr2<BinaryData>::like(const Subexpr2<BinaryData>& col, bool case_sensitive)
{
    return binary_compare<Like, LikeIns>(*this, col, case_sensitive);
}

}
