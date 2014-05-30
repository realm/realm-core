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

#include "column_link.hpp"

using namespace std;
using namespace tightdb;


void ColumnLink::set_link(size_t row_ndx, size_t target_row_ndx)
{
    size_t ref = Column::get(row_ndx);
    if (ref != 0) {
        size_t old_target_row_ndx = ref - 1;
        m_backlinks->remove_backlink(old_target_row_ndx, row_ndx);
    }

    // Row pos is offset by one, to allow null refs
    Column::set(row_ndx, target_row_ndx + 1);

    m_backlinks->add_backlink(target_row_ndx, row_ndx);
}

void ColumnLink::nullify_link(size_t row_ndx)
{
    size_t ref = Column::get(row_ndx);
    if (ref == 0)
        return;

    size_t old_target_row_ndx = ref - 1;
    m_backlinks->remove_backlink(old_target_row_ndx, row_ndx);

    Column::set(row_ndx, 0);
}

void ColumnLink::remove_backlinks(size_t row_ndx)
{
    size_t ref = Column::get(row_ndx);
    if (ref != 0) {
        size_t old_target_row_ndx = ref - 1;
        m_backlinks->remove_backlink(old_target_row_ndx, row_ndx);
    }
}

void ColumnLink::move_last_over(size_t target_row_ndx, size_t last_row_ndx)
{
    TIGHTDB_ASSERT(target_row_ndx < last_row_ndx);
    TIGHTDB_ASSERT(last_row_ndx + 1 == size());

    // Remove backlinks to deleted row
    remove_backlinks(target_row_ndx);

    // Update backlinks to last row to point to its new position
    size_t ref2 = Column::get(last_row_ndx);
    if (ref2 != 0) {
        size_t last_target_row_ndx = ref2 - 1;
        m_backlinks->update_backlink(last_target_row_ndx, last_row_ndx, target_row_ndx);
    }

    // Do the actual move
    Column::move_last_over(target_row_ndx, last_row_ndx);
}

void ColumnLink::erase(size_t row_ndx, bool is_last)
{
    TIGHTDB_ASSERT(is_last);

    // Remove backlinks to deleted row
    remove_backlinks(row_ndx);

    Column::erase(row_ndx, is_last);
}

void ColumnLink::clear()
{
    size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        remove_backlinks(i);
    }
    Column::clear();
}


