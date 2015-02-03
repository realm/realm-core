/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

#include <algorithm>

#include "column_link.hpp"

using namespace std;
using namespace tightdb;


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
    TIGHTDB_ASSERT(row_ndx <= last_row_ndx);
    TIGHTDB_ASSERT(last_row_ndx + 1 == size());

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


void ColumnLink::insert(std::size_t row_ndx, std::size_t num_rows, bool is_append)
{
    std::size_t row_ndx_2 = is_append ? tightdb::npos : row_ndx;
    int_fast64_t value = 0;
    do_insert(row_ndx_2, value, num_rows); // Throws

    // update all backlinks to reflect the insertion.
    size_t last_row_ndx = size()-1;
    for (size_t target_ndx = last_row_ndx; target_ndx >= row_ndx+num_rows; --target_ndx) {
        size_t source_ndx = target_ndx - num_rows;
        uint64_t forward_link_ndx = ColumnLinkBase::get_uint(target_ndx);
        if (forward_link_ndx) {
            // remember that the forward link index is of by 1 to make room
            // for 0 to mean "no link" - so subtract one in the call to update_backlink
            m_backlink_column->update_backlink(forward_link_ndx-1,source_ndx,target_ndx);
        }
    }
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


#ifdef TIGHTDB_DEBUG

void ColumnLink::Verify(const Table& table, size_t col_ndx) const
{
    ColumnLinkBase::Verify(table, col_ndx);

    vector<ColumnBackLink::VerifyPair> pairs;
    m_backlink_column->get_backlinks(pairs);

    // Check correspondence between forward nad backward links.
    size_t backlinks_seen = 0;
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (is_null_link(i))
            continue;
        size_t target_row_ndx = get_link(i);
        typedef vector<ColumnBackLink::VerifyPair>::const_iterator iter;
        ColumnBackLink::VerifyPair search_value;
        search_value.origin_row_ndx = i;
        pair<iter,iter> range = equal_range(pairs.begin(), pairs.end(), search_value);
        // Exactly one corresponding backlink must exist
        TIGHTDB_ASSERT(range.second - range.first == 1);
        TIGHTDB_ASSERT(range.first->target_row_ndx == target_row_ndx);
        ++backlinks_seen;
    }

    // All backlinks must have been matched by a forward link
    TIGHTDB_ASSERT(backlinks_seen == pairs.size());
}

#endif // TIGHTDB_DEBUG
