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

#include <tightdb/column_linklist.hpp>
#include <tightdb/link_view.hpp>

using namespace tightdb;

ColumnLinkList::~ColumnLinkList() TIGHTDB_NOEXCEPT
{
    // Detach all accessors
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        (*p)->detach();
    }
    m_views.clear();
}

void ColumnLinkList::add_link(size_t row_ndx, size_t target_row_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);

    // if there are no links yet, we have to create list
    if (ref == 0) {
        ref_type col_ref = Column::create(Array::type_Normal, 0, 0, get_alloc());
        Column col(col_ref, this, row_ndx, get_alloc());

        col.add(target_row_ndx);
        Column::set(row_ndx, col_ref);
    }
    else {
        // Otherwise just add to existing list
        Column col(ref, this, row_ndx, get_alloc());
        col.add(target_row_ndx);
    }

    m_backlinks->add_backlink(target_row_ndx, row_ndx);
}

void ColumnLinkList::insert_link(size_t row_ndx, size_t ins_pos, size_t target_row_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);

    // if there are no links yet, we have to create list
    if (ref == 0) {
        TIGHTDB_ASSERT(ins_pos == 0);
        ref_type col_ref = Column::create(Array::type_Normal, 0, 0, get_alloc());
        Column col(col_ref, this, row_ndx, get_alloc());

        col.add(target_row_ndx);
        Column::set(row_ndx, col_ref);
    }
    else {
        // Otherwise just add to existing list
        Column col(ref, this, row_ndx, get_alloc());
        col.insert(ins_pos, target_row_ndx);
    }

    m_backlinks->add_backlink(target_row_ndx, row_ndx);
}

void ColumnLinkList::remove_link(size_t row_ndx, size_t link_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);
    TIGHTDB_ASSERT(ref != 0);
    Column col(ref, this, row_ndx, get_alloc());
    TIGHTDB_ASSERT(row_ndx < col.size());

    // update backlinks
    size_t target_row_ndx = col.get(link_ndx);
    m_backlinks->remove_backlink(target_row_ndx, row_ndx);

    bool is_last = (link_ndx+1 == col.size());
    col.erase(link_ndx, is_last);

    if (col.is_empty()) {
        col.destroy();
        Column::set(row_ndx, 0);
    }
}

void ColumnLinkList::remove_all_links(size_t row_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);
    if (ref == 0)
        return;

    Column col(ref, this, row_ndx, get_alloc());

    // Update backlinks
    size_t count = col.size();
    for (size_t i = 0; i < count; ++i) {
        size_t target_row_ndx = col.get(i);
        m_backlinks->remove_backlink(target_row_ndx, row_ndx);
    }

    col.destroy();
    Column::set(row_ndx, 0);
}

void ColumnLinkList::set_link(size_t row_ndx, size_t link_ndx, size_t target_row_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);
    TIGHTDB_ASSERT(ref != 0);
    Column col(ref, this, row_ndx, get_alloc());
    TIGHTDB_ASSERT(link_ndx < col.size());

    // update backlinks
    size_t old_target_row_ndx = col.get(link_ndx);
    m_backlinks->remove_backlink(old_target_row_ndx, row_ndx);
    m_backlinks->add_backlink(target_row_ndx, row_ndx);

    col.set(link_ndx, target_row_ndx);
}

void ColumnLinkList::move_link(size_t row_ndx, size_t old_link_ndx, size_t new_link_ndx)
{
    ref_type ref = Column::get_as_ref(row_ndx);
    TIGHTDB_ASSERT(ref != 0);
    Column col(ref, this, row_ndx, get_alloc());
    TIGHTDB_ASSERT(old_link_ndx < col.size());
    TIGHTDB_ASSERT(new_link_ndx <= col.size());

    if (old_link_ndx == new_link_ndx)
        return;
    size_t ins_pos = (new_link_ndx <= old_link_ndx) ? new_link_ndx : new_link_ndx-1;

    size_t target_row_ndx = col.get(old_link_ndx);
    bool is_last = (old_link_ndx+1 == col.size());
    col.erase(old_link_ndx, is_last);
    col.insert(ins_pos, target_row_ndx);
}

void ColumnLinkList::clear()
{
    // Detach all accessors
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        (*p)->detach();
    }
    m_views.clear();

    Column::clear();

    //TODO: update backlinks
}

void ColumnLinkList::move_last_over(std::size_t ndx)
{
    // Detach accessors to the deleted row
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == ndx) {
            (*p)->detach();
            m_views.erase(p);
            break;
        }
    }

    // Update accessors to the moved row
    size_t last_row = size()-1;
    end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == last_row) {
            (*p)->set_parent_row(ndx);
            break;
        }
    }

    Column::destroy_subtree(ndx, false);
    Column::move_last_over(ndx);

    //TODO: update backlinks
}

void ColumnLinkList::erase(std::size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(is_last);

    // Detach accessors to the deleted row
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == ndx) {
            (*p)->detach();
            m_views.erase(p);
            break;
        }
    }

    Column::destroy_subtree(ndx, false);
    Column::erase(ndx, is_last);

    //TODO: update backlinks
}

void ColumnLinkList::do_nullify_link(size_t row_ndx, size_t old_target_row_ndx)
{
    LinkViewRef links = get_link_view(row_ndx);
    links->do_nullify_link(old_target_row_ndx);
}

void ColumnLinkList::do_update_link(size_t row_ndx, size_t old_target_row_ndx, std::size_t new_target_row_ndx)
{
    LinkViewRef links = get_link_view(row_ndx);
    links->do_update_link(old_target_row_ndx, new_target_row_ndx);
}

LinkViewRef ColumnLinkList::get_link_view(std::size_t row_ndx)
{
    TIGHTDB_ASSERT(row_ndx < size());

    // Check if we already have a linkview for this row
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == row_ndx) {
            return LinkViewRef(*p);
        }
    }

    LinkView* view = new LinkView(*this, row_ndx);
    m_views.push_back(view);
    return LinkViewRef(view);
}

void ColumnLinkList::update_child_ref(size_t child_ndx, ref_type new_ref)
{
    Column::set(child_ndx, new_ref);
}

ref_type ColumnLinkList::get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT
{
    return Column::get(child_ndx);
}

#ifdef TIGHTDB_DEBUG

std::pair<ref_type, size_t> ColumnLinkList::get_to_dot_parent(size_t ndx_in_parent) const
{
    std::pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_parent);
    return std::make_pair(p.first.m_ref, p.second);
}

#endif //TIGHTDB_DEBUG

