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

using namespace realm;


void ColumnLink::remove_backlinks(size_t row_ndx)
{
    int_fast64_t value = ColumnLinkBase::get(row_ndx);
    if (value != 0) {
        size_t target_row_ndx = to_size_t(value - 1);
        m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);
    }
}


void ColumnLink::move_last_over(size_t row_ndx, size_t last_row_ndx,
                                bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_3(row_ndx, <=, last_row_ndx);
    REALM_ASSERT_3(last_row_ndx + 1, ==, size());

    // Remove backlinks to deleted row
    if (!broken_reciprocal_backlinks)
        remove_backlinks(row_ndx);

    // Update backlinks to last row to point to its new position
    if (row_ndx != last_row_ndx) {
        int_fast64_t value = ColumnLinkBase::get(last_row_ndx);
        if (value != 0) {
            size_t target_row_ndx = to_size_t(value - 1);
            m_backlink_column->update_backlink(target_row_ndx, last_row_ndx, row_ndx);
        }
    }

    do_move_last_over(row_ndx, last_row_ndx);
}


void ColumnLink::clear(size_t, bool broken_reciprocal_backlinks)
{
    if (!broken_reciprocal_backlinks) {
        size_t num_target_rows = m_target_table->size();
        m_backlink_column->remove_all_backlinks(num_target_rows); // Throws
    }

    do_clear(); // Throws
}


void ColumnLink::cascade_break_backlinks_to(size_t row_ndx, CascadeState& state)
{
    int_fast64_t value = ColumnLinkBase::get(row_ndx);
    bool is_null = value == 0;
    if (is_null)
        return;

    // Remove the reciprocal backlink at target_row_ndx that points to row_ndx
    size_t target_row_ndx = to_size_t(value - 1);
    m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    // Recurse on target row when appropriate
    size_t target_table_ndx = m_target_table->get_index_in_group();
    check_cascade_break_backlinks_to(target_table_ndx, target_row_ndx, state); // Throws
}


void ColumnLink::cascade_break_backlinks_to_all_rows(size_t num_rows, CascadeState& state)
{
    size_t num_target_rows = m_target_table->size();
    m_backlink_column->remove_all_backlinks(num_target_rows);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    size_t target_table_ndx = m_target_table->get_index_in_group();
    for (size_t i = 0; i < num_rows; ++i) {
        int_fast64_t value = ColumnLinkBase::get(i);
        bool is_null = value == 0;
        if (is_null)
            continue;

        size_t target_row_ndx = to_size_t(value - 1);
        check_cascade_break_backlinks_to(target_table_ndx, target_row_ndx, state); // Throws
    }
}


#ifdef REALM_DEBUG

void ColumnLink::Verify(const Table& table, size_t col_ndx) const
{
    ColumnLinkBase::Verify(table, col_ndx);

    std::vector<ColumnBackLink::VerifyPair> pairs;
    m_backlink_column->get_backlinks(pairs);

    // Check correspondence between forward nad backward links.
    size_t backlinks_seen = 0;
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (is_null_link(i))
            continue;
        size_t target_row_ndx = get_link(i);
        typedef std::vector<ColumnBackLink::VerifyPair>::const_iterator iter;
        ColumnBackLink::VerifyPair search_value;
        search_value.origin_row_ndx = i;
        std::pair<iter,iter> range = equal_range(pairs.begin(), pairs.end(), search_value);
        // Exactly one corresponding backlink must exist
        REALM_ASSERT(range.second - range.first == 1);
        REALM_ASSERT_3(range.first->target_row_ndx, ==, target_row_ndx);
        ++backlinks_seen;
    }

    // All backlinks must have been matched by a forward link
    REALM_ASSERT_3(backlinks_seen, ==, pairs.size());
}

#endif // REALM_DEBUG
