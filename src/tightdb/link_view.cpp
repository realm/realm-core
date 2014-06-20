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

#include <tightdb/link_view.hpp>
#include <tightdb/column_linklist.hpp>

using namespace tightdb;

void LinkView::insert(std::size_t ins_pos, std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() || ins_pos == 0);
    TIGHTDB_ASSERT(!m_refs.is_attached() || ins_pos <= m_refs.size());
    TIGHTDB_ASSERT(target_row_ndx < m_column.get_target_table()->size());

    // if there are no links yet, we have to create list
    if (!m_refs.is_attached()) {
        TIGHTDB_ASSERT(ins_pos == 0);
        ref_type col_ref = Column::create(Array::type_Normal, 0, 0, m_column.get_alloc());
        m_column.set_row_ref(m_row_ndx, col_ref);
        m_refs.get_root_array()->init_from_parent(); // re-attach
    }

    m_refs.insert(ins_pos, target_row_ndx);
    m_table->bump_version();
    m_column.add_backlink(target_row_ndx, m_row_ndx);
}

void LinkView::set(std::size_t row_ndx, std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() && row_ndx < m_refs.size());
    TIGHTDB_ASSERT(target_row_ndx < m_column.get_target_table()->size());

    // update backlinks
    size_t old_target_row_ndx = m_refs.get(row_ndx);
    m_column.remove_backlink(old_target_row_ndx, m_row_ndx);
    m_column.add_backlink(target_row_ndx, m_row_ndx);
    m_table->bump_version();

    m_refs.set(row_ndx, target_row_ndx);
}

void LinkView::move(size_t old_link_ndx, size_t new_link_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached());
    TIGHTDB_ASSERT(old_link_ndx < m_refs.size());
    TIGHTDB_ASSERT(new_link_ndx <= m_refs.size());

    if (old_link_ndx == new_link_ndx)
        return;
    size_t ins_pos = (new_link_ndx <= old_link_ndx) ? new_link_ndx : new_link_ndx-1;

    size_t target_row_ndx = m_refs.get(old_link_ndx);
    bool is_last = (old_link_ndx+1 == m_refs.size());
    m_refs.erase(old_link_ndx, is_last);
    m_refs.insert(ins_pos, target_row_ndx);
    m_table->bump_version();
}

void LinkView::remove(std::size_t row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() && row_ndx < m_refs.size());

    // update backlinks
    size_t target_row_ndx = m_refs.get(row_ndx);
    m_column.remove_backlink(target_row_ndx, m_row_ndx);

    bool is_last = (row_ndx+1 == m_refs.size());
    m_refs.erase(row_ndx, is_last);

    if (m_refs.is_empty()) {
        m_refs.detach();
        m_column.set_row_ref(m_row_ndx, 0);
    }
    m_table->bump_version();
}

void LinkView::clear()
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs.is_attached())
        return;

    // Update backlinks
    size_t count = m_refs.size();
    for (size_t i = 0; i < count; ++i) {
        size_t target_row_ndx = m_refs.get(i);
        m_column.remove_backlink(target_row_ndx, m_row_ndx);
    }

    m_refs.destroy();
    m_column.set_row_ref(m_row_ndx, 0);
    m_table->bump_version();
}

void LinkView::do_nullify_link(std::size_t old_target_row_ndx)
{
    TIGHTDB_ASSERT(m_refs.is_attached());

    size_t pos = m_refs.find_first(old_target_row_ndx);
    TIGHTDB_ASSERT(pos != not_found);

    bool is_last = (pos+1 == m_refs.size());
    m_refs.erase(pos, is_last);

    if (m_refs.is_empty()) {
        m_refs.destroy();
        m_column.set_row_ref(m_row_ndx, 0);
    }
    m_table->bump_version();
}

void LinkView::do_update_link(size_t old_target_row_ndx, std::size_t new_target_row_ndx)
{
    TIGHTDB_ASSERT(m_refs.is_attached());

    size_t pos = m_refs.find_first(old_target_row_ndx);
    TIGHTDB_ASSERT(pos != not_found);

    m_refs.set(pos, new_target_row_ndx);
    m_table->bump_version();
}
