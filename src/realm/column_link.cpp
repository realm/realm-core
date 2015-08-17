/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <algorithm>

#include "column_link.hpp"

#include <realm/group.hpp>

#include <realm/replication.hpp>

using namespace realm;

void LinkColumn::remove_backlinks(size_t row_index)
{
    int_fast64_t value = LinkColumnBase::get(row_index);
    if (value != 0) {
        size_t target_row_index = to_size_t(value - 1);
        m_backlink_column->remove_one_backlink(target_row_index, row_index);
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


void LinkColumn::insert_rows(size_t row_index, size_t num_rows_to_insert, size_t prior_num_rows)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_index <= prior_num_rows);

    // Update backlinks to the moved origin rows
    size_t num_rows_moved = prior_num_rows - row_index;
    for (size_t i = num_rows_moved; i > 0; --i) {
        size_t old_origin_row_index = row_index + i - 1;
        size_t new_origin_row_index = row_index + num_rows_to_insert + i - 1;
        uint_fast64_t value = LinkColumnBase::get_uint(old_origin_row_index);
        if (value != 0) { // Zero means null
            size_t target_row_index = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_index, old_origin_row_index,
                                               new_origin_row_index); // Throws
        }
    }

    LinkColumnBase::insert_rows(row_index, num_rows_to_insert, prior_num_rows); // Throws
}


void LinkColumn::erase_rows(size_t row_index, size_t num_rows_to_erase, size_t prior_num_rows,
                            bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_index <= prior_num_rows - num_rows_to_erase);

    // Remove backlinks to the removed origin rows
    if (!broken_reciprocal_backlinks) {
        for (size_t i = 0; i < num_rows_to_erase; ++i)
            remove_backlinks(row_index+i);
    }

    // Update backlinks to the moved origin rows
    size_t num_rows_moved = prior_num_rows - (row_index + num_rows_to_erase);
    for (size_t i = 0; i < num_rows_moved; ++i) {
        size_t old_origin_row_index = row_index + num_rows_to_erase + i;
        size_t new_origin_row_index = row_index + i;
        uint_fast64_t value = LinkColumnBase::get_uint(old_origin_row_index);
        if (value != 0) { // Zero means null
            size_t target_row_index = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_index, old_origin_row_index,
                                               new_origin_row_index); // Throws
        }
    }

    LinkColumnBase::erase_rows(row_index, num_rows_to_erase, prior_num_rows,
                               broken_reciprocal_backlinks); // Throws
}


void LinkColumn::move_last_row_over(size_t row_index, size_t prior_num_rows,
                                    bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_index <= prior_num_rows);

    // Remove backlinks to the removed origin row
    if (!broken_reciprocal_backlinks)
        remove_backlinks(row_index);

    // Update backlinks to the moved origin row
    size_t last_row_index = prior_num_rows - 1;
    if (row_index != last_row_index) {
        int_fast64_t value = LinkColumnBase::get(last_row_index);
        if (value != 0) {
            size_t target_row_index = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_index, last_row_index, row_index);
        }
    }

    LinkColumnBase::move_last_row_over(row_index, prior_num_rows,
                                       broken_reciprocal_backlinks); // Throws
}


void LinkColumn::cascade_break_backlinks_to(size_t row_index, CascadeState& state)
{
    int_fast64_t value = LinkColumnBase::get(row_index);
    bool is_null = value == 0;
    if (is_null)
        return;

    // Remove the reciprocal backlink at target_row_index that points to row_index
    size_t target_row_index = to_size_t(value - 1);
    m_backlink_column->remove_one_backlink(target_row_index, row_index);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    // Recurse on target row when appropriate
    size_t target_table_index = m_target_table->get_index_in_group();
    check_cascade_break_backlinks_to(target_table_index, target_row_index, state); // Throws
}


void LinkColumn::cascade_break_backlinks_to_all_rows(size_t num_rows, CascadeState& state)
{
    size_t num_target_rows = m_target_table->size();
    m_backlink_column->remove_all_backlinks(num_target_rows);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    size_t target_table_index = m_target_table->get_index_in_group();
    for (size_t i = 0; i < num_rows; ++i) {
        int_fast64_t value = LinkColumnBase::get(i);
        bool is_null = value == 0;
        if (is_null)
            continue;

        size_t target_row_index = to_size_t(value - 1);
        check_cascade_break_backlinks_to(target_table_index, target_row_index, state); // Throws
    }
}


void LinkColumn::do_nullify_link(size_t row_index, size_t)
{
    if (Replication* repl = get_root_array()->get_alloc().get_replication()) {
        repl->nullify_link(m_table, m_column_index, row_index);
    }
    LinkColumnBase::set(row_index, 0);
}


#ifdef REALM_DEBUG

void LinkColumn::verify(const Table& table, size_t column_index) const
{
    LinkColumnBase::verify(table, column_index);

    std::vector<BacklinkColumn::VerifyPair> pairs;
    m_backlink_column->get_backlinks(pairs);

    // Check correspondence between forward nad backward links.
    size_t backlinks_seen = 0;
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (is_null_link(i))
            continue;
        size_t target_row_index = get_link(i);
        typedef std::vector<BacklinkColumn::VerifyPair>::const_iterator iter;
        BacklinkColumn::VerifyPair search_value;
        search_value.origin_row_index = i;
        std::pair<iter,iter> range = equal_range(pairs.begin(), pairs.end(), search_value);
        // Exactly one corresponding backlink must exist
        REALM_ASSERT(range.second - range.first == 1);
        REALM_ASSERT_3(range.first->target_row_index, ==, target_row_index);
        ++backlinks_seen;
    }

    // All backlinks must have been matched by a forward link
    REALM_ASSERT_3(backlinks_seen, ==, pairs.size());
}

#endif // REALM_DEBUG
