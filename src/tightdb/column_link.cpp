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


void ColumnLink::set_link(size_t row_ndx, size_t target_row_ndx)
{
    size_t ref = ColumnLinkBase::get(row_ndx);
    if (ref != 0) {
        size_t old_target_row_ndx = ref - 1;
        m_backlink_column->remove_backlink(old_target_row_ndx, row_ndx);
    }

    // Row pos is offset by one, to allow null refs
    ColumnLinkBase::set(row_ndx, target_row_ndx + 1);

    m_backlink_column->add_backlink(target_row_ndx, row_ndx);
}


void ColumnLink::nullify_link(size_t row_ndx)
{
    size_t ref = ColumnLinkBase::get(row_ndx);
    if (ref == 0)
        return;

    size_t old_target_row_ndx = ref - 1;
    m_backlink_column->remove_backlink(old_target_row_ndx, row_ndx);

    ColumnLinkBase::set(row_ndx, 0);
}


void ColumnLink::remove_backlinks(size_t row_ndx)
{
    size_t ref = ColumnLinkBase::get(row_ndx);
    if (ref != 0) {
        size_t old_target_row_ndx = ref - 1;
        m_backlink_column->remove_backlink(old_target_row_ndx, row_ndx);
    }
}


void ColumnLink::move_last_over(size_t target_row_ndx, size_t last_row_ndx)
{
    TIGHTDB_ASSERT(target_row_ndx < last_row_ndx);
    TIGHTDB_ASSERT(last_row_ndx + 1 == size());

    // Remove backlinks to deleted row
    remove_backlinks(target_row_ndx);

    // Update backlinks to last row to point to its new position
    size_t ref2 = ColumnLinkBase::get(last_row_ndx);
    if (ref2 != 0) {
        size_t last_target_row_ndx = ref2 - 1;
        m_backlink_column->update_backlink(last_target_row_ndx, last_row_ndx, target_row_ndx);
    }

    // Do the actual move
    ColumnLinkBase::move_last_over(target_row_ndx, last_row_ndx);
}


void ColumnLink::erase(size_t row_ndx, bool is_last)
{
    TIGHTDB_ASSERT(is_last);

    // Remove backlinks to deleted row
    remove_backlinks(row_ndx);

    ColumnLinkBase::erase(row_ndx, is_last);
}


void ColumnLink::clear()
{
    size_t count = size();
    for (size_t i = 0; i < count; ++i)
        remove_backlinks(i);
    ColumnLinkBase::clear();
}


void ColumnLink::erase_cascade(size_t row_ndx, size_t stop_on_table_ndx, cascade_rows& rows) const
{
    if (m_weak_links || is_null_link(row_ndx))
        return;

    size_t target_table_ndx = m_target_table->get_index_in_group();
    if (target_table_ndx == stop_on_table_ndx)
        return;

    size_t target_row_ndx = get_link(row_ndx);
    erase_cascade_target_row(target_table_ndx, target_row_ndx, stop_on_table_ndx, rows); // Throws
}


void ColumnLink::clear_cascade(size_t table_ndx, size_t num_rows, cascade_rows& rows) const
{
    if (m_weak_links)
        return;

    size_t target_table_ndx = m_target_table->get_index_in_group();
    if (target_table_ndx == table_ndx)
        return;

    for (size_t i = 0; i < num_rows; ++i) {
        if (is_null_link(i))
            continue;
        size_t target_row_ndx = get_link(i);
        erase_cascade_target_row(target_table_ndx, target_row_ndx, table_ndx, rows); // Throws
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
