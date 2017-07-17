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

#include <realm/column_linkbase.hpp>
#include <realm/column_backlink.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

using namespace realm;


void LinkColumnBase::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    IntegerColumn::refresh_accessor_tree(col_ndx, spec); // Throws
    ColumnAttr attr = spec.get_column_attr(col_ndx);
    m_weak_links = (attr & col_attr_StrongLinks) == 0;
}


void LinkColumnBase::check_cascade_break_backlinks_to(size_t target_table_ndx, size_t target_row_ndx,
                                                      CascadeState& state)
{
    // Stop if the target row was already visited
    CascadeState::row target_row;
    target_row.table_ndx = target_table_ndx;
    target_row.row_ndx = target_row_ndx;
    auto i = std::upper_bound(state.rows.begin(), state.rows.end(), target_row);
    bool already_seen = i != state.rows.begin() && i[-1] == target_row;
    if (already_seen)
        return;

    // Stop if there are any remaining strong links to this row (this scheme
    // fails to discover orphaned cycles)
    typedef _impl::TableFriend tf;
    size_t num_remaining = tf::get_backlink_count(*m_target_table, target_row_ndx, state.only_strong_links);
    if (num_remaining > 0)
        return;

    // Recurse
    state.rows.insert(i, target_row);                                       // Throws
    tf::cascade_break_backlinks_to(*m_target_table, target_row_ndx, state); // Throws
}


void LinkColumnBase::verify(const Table& table, size_t col_ndx) const
{
#ifdef REALM_DEBUG
    IntegerColumn::verify(table, col_ndx);

    // Check that the backlink column specifies the right origin
    REALM_ASSERT(&m_backlink_column->get_origin_table() == &table);
    REALM_ASSERT(&m_backlink_column->get_origin_column() == this);

    // Check that m_target_table is the table specified by the spec
    size_t target_table_ndx = m_target_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT(target_table_ndx == spec.get_opposite_link_table_ndx(col_ndx));

    // Check that m_backlink_column is the column specified by the target table spec
    const Spec& target_spec = tf::get_spec(*m_target_table);
    size_t backlink_col_ndx = target_spec.find_backlink_column(table.get_index_in_group(), col_ndx);
    REALM_ASSERT(m_backlink_column == &tf::get_column(*m_target_table, backlink_col_ndx));
#else
    static_cast<void>(table);
    static_cast<void>(col_ndx);
#endif // REALM_DEBUG
}
