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
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#endif

using namespace tightdb;


void LinkView::insert(std::size_t link_ndx, std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() || link_ndx == 0);
    TIGHTDB_ASSERT(!m_refs.is_attached() || link_ndx <= m_refs.size());
    TIGHTDB_ASSERT(target_row_ndx < m_column.get_target_table()->size());
    m_table->bump_version();

    size_t row_ndx = get_origin_row_index();

    // if there are no links yet, we have to create list
    if (!m_refs.is_attached()) {
        TIGHTDB_ASSERT(link_ndx == 0);
        ref_type col_ref = Column::create(Array::type_Normal, 0, 0, m_column.get_alloc());
        m_column.set_row_ref(row_ndx, col_ref);
        m_refs.get_root_array()->init_from_parent(); // re-attach
    }

    m_refs.insert(link_ndx, target_row_ndx);
    m_column.add_backlink(target_row_ndx, row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->link_list_insert(*this, link_ndx, target_row_ndx); // Throws
#endif
}


void LinkView::set(std::size_t link_ndx, std::size_t target_row_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() && link_ndx < m_refs.size());
    TIGHTDB_ASSERT(target_row_ndx < m_column.get_target_table()->size());
    m_table->bump_version();

    // update backlinks
    size_t row_ndx = get_origin_row_index();
    size_t old_target_row_ndx = m_refs.get(link_ndx);
    m_column.remove_backlink(old_target_row_ndx, row_ndx);
    m_column.add_backlink(target_row_ndx, row_ndx);
    m_refs.set(link_ndx, target_row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->link_list_set(*this, link_ndx, target_row_ndx); // Throws
#endif
}


void LinkView::move(size_t old_link_ndx, size_t new_link_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached());
    TIGHTDB_ASSERT(old_link_ndx < m_refs.size());
    TIGHTDB_ASSERT(new_link_ndx <= m_refs.size());

    if (old_link_ndx == new_link_ndx)
        return;
    m_table->bump_version();

    size_t link_ndx = (new_link_ndx <= old_link_ndx) ? new_link_ndx : new_link_ndx-1;
    size_t target_row_ndx = m_refs.get(old_link_ndx);
    bool is_last = (old_link_ndx+1 == m_refs.size());
    m_refs.erase(old_link_ndx, is_last);
    m_refs.insert(link_ndx, target_row_ndx);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->link_list_move(*this, old_link_ndx, new_link_ndx); // Throws
#endif
}


void LinkView::remove(std::size_t link_ndx)
{
    TIGHTDB_ASSERT(is_attached());
    TIGHTDB_ASSERT(m_refs.is_attached() && link_ndx < m_refs.size());
    m_table->bump_version();

    // update backlinks
    size_t target_row_ndx = m_refs.get(link_ndx);
    size_t row_ndx = get_origin_row_index();
    m_column.remove_backlink(target_row_ndx, row_ndx);

    bool is_last = (link_ndx+1 == m_refs.size());
    m_refs.erase(link_ndx, is_last);

    if (m_refs.is_empty()) {
        m_refs.detach();
        m_column.set_row_ref(row_ndx, 0);
    }

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->link_list_erase(*this, link_ndx); // Throws
#endif
}


void LinkView::clear()
{
    TIGHTDB_ASSERT(is_attached());

    if (!m_refs.is_attached())
        return;

    m_table->bump_version();

    // Update backlinks
    size_t row_ndx = get_origin_row_index();
    size_t n = m_refs.size();
    for (size_t i = 0; i < n; ++i) {
        size_t target_row_ndx = m_refs.get(i);
        m_column.remove_backlink(target_row_ndx, row_ndx);
    }

    m_refs.destroy();
    m_column.set_row_ref(row_ndx, 0);

#ifdef TIGHTDB_ENABLE_REPLICATION
    if (Replication* repl = get_repl())
        repl->link_list_clear(*this); // Throws
#endif
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
        size_t row_ndx = get_origin_row_index();
        m_column.set_row_ref(row_ndx, 0);
    }
}


void LinkView::do_update_link(size_t old_target_row_ndx, std::size_t new_target_row_ndx)
{
    TIGHTDB_ASSERT(m_refs.is_attached());

    size_t pos = m_refs.find_first(old_target_row_ndx);
    TIGHTDB_ASSERT(pos != not_found);

    m_refs.set(pos, new_target_row_ndx);
}


#ifdef TIGHTDB_ENABLE_REPLICATION

void LinkView::repl_unselect() TIGHTDB_NOEXCEPT
{
        if (Replication* repl = get_repl())
            repl->on_link_list_destroyed(*this);
}

#endif // TIGHTDB_ENABLE_REPLICATION
