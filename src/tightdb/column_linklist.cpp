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

using namespace std;
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

void ColumnLinkList::clear()
{
    // Remove all backlinks to the delete rows
    size_t rowcount = size();
    for (size_t r = 0; r < rowcount; ++r) {
        ref_type ref = Column::get_as_ref(r);
        if (ref == 0)
            continue;

        const Column linkcol(ref, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlinks->remove_backlink(old_target_row_ndx, r);
        }
    }

    // Do the actual deletion
    Column::clear();

    // Detach all accessors
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        (*p)->detach();
    }
    m_views.clear();
}

void ColumnLinkList::move_last_over(size_t target_row_ndx, size_t last_row_ndx)
{
    // Remove backlinks to the delete row
    ref_type ref = Column::get_as_ref(target_row_ndx);
    if (ref) {
        const Column linkcol(ref, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlinks->remove_backlink(old_target_row_ndx, target_row_ndx);
        }
    }

    // Update backlinks to last row to point to its new position
    ref_type ref2 = Column::get_as_ref(last_row_ndx);
    if (ref2) {
        const Column linkcol(ref2, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlinks->update_backlink(old_target_row_ndx, last_row_ndx, target_row_ndx);
        }
    }

    // Do the actual delete and move
    Column::destroy_subtree(target_row_ndx, false);
    Column::move_last_over(target_row_ndx, last_row_ndx);

    // Detach accessors to the deleted row
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == target_row_ndx) {
            (*p)->detach();
            m_views.erase(p);
            break;
        }
    }

    // Update accessors to the moved row
    end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == last_row_ndx) {
            (*p)->set_parent_row(target_row_ndx);
            break;
        }
    }
}

void ColumnLinkList::erase(std::size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx+1 == size());
    TIGHTDB_ASSERT(is_last);

    // Remove backlinks to the delete row
    ref_type ref = Column::get_as_ref(ndx);
    if (ref) {
        const Column linkcol(ref, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlinks->remove_backlink(old_target_row_ndx, ndx);
        }
    }

    // Do the actual delete
    Column::destroy_subtree(ndx, false);
    Column::erase(ndx, is_last);

    // Detach accessors to the deleted row
    std::vector<LinkView*>::iterator end = m_views.end();
    for (std::vector<LinkView*>::iterator p = m_views.begin(); p != end; ++p) {
        if ((*p)->m_row_ndx == ndx) {
            (*p)->detach();
            m_views.erase(p);
            break;
        }
    }
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

void ColumnLinkList::to_json_row(size_t row_ndx, std::ostream& out) const
{
    LinkViewRef links1 = const_cast<ColumnLinkList*>(this)->get_link_view(row_ndx);
    for (size_t t = 0; t < links1->size(); t++) {
        if (t > 0)
            out << ", ";
        size_t target = links1->get_target_row(t);
        out << target;
    }
    
}


#ifdef TIGHTDB_DEBUG

pair<ref_type, size_t> ColumnLinkList::get_to_dot_parent(size_t ndx_in_parent) const
{
    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_parent);
    return make_pair(p.first.m_ref, p.second);
}

#endif //TIGHTDB_DEBUG

