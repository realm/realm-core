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

#include <algorithm>

#include <realm/column_link.hpp>

#include <realm/group.hpp>

#include <realm/replication.hpp>

using namespace realm;

void LinkColumn::remove_backlinks(size_t row_ndx)
{
    int_fast64_t value = LinkColumnBase::get(row_ndx);
    if (value != 0) {
        size_t target_row_ndx = to_size_t(value - 1);
        m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);
    }
}


void LinkColumn::clear(size_t, bool broken_reciprocal_backlinks)
{
    if (!broken_reciprocal_backlinks) {
        size_t num_target_rows = m_target_table->size();
        m_backlink_column->remove_all_backlinks(num_target_rows); // Throws
    }

    clear_without_updating_index(); // Throws
}


void LinkColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool insert_nulls)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx <= prior_num_rows);
    REALM_ASSERT(insert_nulls);

    // Update backlinks to the moved origin rows
    size_t num_rows_moved = prior_num_rows - row_ndx;
    for (size_t i = num_rows_moved; i > 0; --i) {
        size_t old_origin_row_ndx = row_ndx + i - 1;
        size_t new_origin_row_ndx = row_ndx + num_rows_to_insert + i - 1;
        uint_fast64_t value = LinkColumnBase::get_uint(old_origin_row_ndx);
        if (value != 0) { // Zero means null
            size_t target_row_ndx = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_ndx, old_origin_row_ndx, new_origin_row_ndx); // Throws
        }
    }

    LinkColumnBase::insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, false); // Throws
}


void LinkColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                            bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_ndx <= prior_num_rows - num_rows_to_erase);

    // Remove backlinks to the removed origin rows
    if (!broken_reciprocal_backlinks) {
        for (size_t i = 0; i < num_rows_to_erase; ++i)
            remove_backlinks(row_ndx + i);
    }

    // Update backlinks to the moved origin rows
    size_t num_rows_moved = prior_num_rows - (row_ndx + num_rows_to_erase);
    for (size_t i = 0; i < num_rows_moved; ++i) {
        size_t old_origin_row_ndx = row_ndx + num_rows_to_erase + i;
        size_t new_origin_row_ndx = row_ndx + i;
        uint_fast64_t value = LinkColumnBase::get_uint(old_origin_row_ndx);
        if (value != 0) { // Zero means null
            size_t target_row_ndx = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_ndx, old_origin_row_ndx, new_origin_row_ndx); // Throws
        }
    }

    LinkColumnBase::erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks); // Throws
}


void LinkColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows, bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx <= prior_num_rows);

    // Remove backlinks to the removed origin row
    if (!broken_reciprocal_backlinks)
        remove_backlinks(row_ndx);

    // Update backlinks to the moved origin row
    size_t last_row_ndx = prior_num_rows - 1;
    if (row_ndx != last_row_ndx) {
        int_fast64_t value = LinkColumnBase::get(last_row_ndx);
        if (value != 0) {
            size_t target_row_ndx = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_ndx, last_row_ndx, row_ndx);
        }
    }

    LinkColumnBase::move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks); // Throws
}


void LinkColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    REALM_ASSERT_DEBUG(row_ndx_1 != row_ndx_2);
    int_fast64_t value_1 = LinkColumnBase::get(row_ndx_1);
    int_fast64_t value_2 = LinkColumnBase::get(row_ndx_2);
    if (value_1 != 0) {
        size_t target_row_ndx = to_size_t(value_1 - 1);
        m_backlink_column->swap_backlinks(target_row_ndx, row_ndx_1, row_ndx_2);
    }
    if (value_2 != 0) {
        size_t target_row_ndx = to_size_t(value_2 - 1);
        m_backlink_column->swap_backlinks(target_row_ndx, row_ndx_1, row_ndx_2);
    }

    set(row_ndx_1, value_2);
    set(row_ndx_2, value_1);
}


void LinkColumn::cascade_break_backlinks_to(size_t row_ndx, CascadeState& state)
{
    int_fast64_t value = LinkColumnBase::get(row_ndx);
    bool value_is_null = value == 0;
    if (value_is_null)
        return;

    // Remove the reciprocal backlink at target_row_ndx that points to row_ndx
    size_t target_row_ndx = to_size_t(value - 1);
    m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);

    if (m_weak_links && state.only_strong_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    // Recurse on target row when appropriate
    auto target_table_key = m_target_table->get_key();
    check_cascade_break_backlinks_to(target_table_key, target_row_ndx, state); // Throws
}


void LinkColumn::cascade_break_backlinks_to_all_rows(size_t num_rows, CascadeState& state)
{
    size_t num_target_rows = m_target_table->size();
    m_backlink_column->remove_all_backlinks(num_target_rows);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    auto target_table_key = m_target_table->get_key();
    for (size_t i = 0; i < num_rows; ++i) {
        int_fast64_t value = LinkColumnBase::get(i);
        bool value_is_null = value == 0;
        if (value_is_null)
            continue;

        size_t target_row_ndx = to_size_t(value - 1);
        check_cascade_break_backlinks_to(target_table_key, target_row_ndx, state); // Throws
    }
}


void LinkColumn::do_nullify_link(size_t row_ndx, size_t)
{
    if (Replication* repl = get_root_array()->get_alloc().get_replication()) {
        repl->nullify_link(m_table, get_column_index(), row_ndx);
    }
    LinkColumnBase::set(row_ndx, 0);
}


void LinkColumn::verify(const Table& table, size_t col_ndx) const
{
#ifdef REALM_DEBUG
    LinkColumnBase::verify(table, col_ndx);

    std::vector<BacklinkColumn::VerifyPair> pairs;
    m_backlink_column->get_backlinks(pairs);

    // Check correspondence between forward nad backward links.
    size_t backlinks_seen = 0;
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (is_null_link(i))
            continue;
        size_t target_row_ndx = get_link(i);
        typedef std::vector<BacklinkColumn::VerifyPair>::const_iterator iter;
        BacklinkColumn::VerifyPair search_value;
        search_value.origin_row_ndx = i;
        std::pair<iter, iter> range = equal_range(pairs.begin(), pairs.end(), search_value);
        // Exactly one corresponding backlink must exist
        REALM_ASSERT(range.second - range.first == 1);
        REALM_ASSERT_3(range.first->target_row_ndx, ==, target_row_ndx);
        ++backlinks_seen;
    }

    // All backlinks must have been matched by a forward link
    REALM_ASSERT_3(backlinks_seen, ==, pairs.size());
#else
    static_cast<void>(table);
    static_cast<void>(col_ndx);
#endif
}
