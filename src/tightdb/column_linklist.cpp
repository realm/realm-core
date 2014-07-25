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
#include <set>

#include <tightdb/column_linklist.hpp>
#include <tightdb/link_view.hpp>

using namespace std;
using namespace tightdb;


void ColumnLinkList::clear()
{
    discard_child_accessors();

    // Remove all backlinks to the delete rows
    size_t row_count = size();
    for (size_t r = 0; r < row_count; ++r) {
        ref_type ref = ColumnLinkBase::get_as_ref(r);
        if (ref == 0)
            continue;

        Column link_col(ref, null_ptr, 0, get_alloc());
        size_t n = link_col.size();
        for (size_t i = 0; i < n; ++i) {
            size_t old_target_row_ndx = link_col.get(i);
            m_backlink_column->remove_backlink(old_target_row_ndx, r);
        }
    }

    // Do the actual deletion
    ColumnLinkBase::clear(); // Throws
    // FIXME: This one is needed because Column::clear() forgets about the leaf
    // type. A better solution should probably be sought after.
    m_array->set_type(Array::type_HasRefs);
}


void ColumnLinkList::move_last_over(size_t target_row_ndx, size_t last_row_ndx)
{
    // Remove backlinks to the delete row
    ref_type ref = ColumnLinkBase::get_as_ref(target_row_ndx);
    if (ref) {
        const Column linkcol(ref, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlink_column->remove_backlink(old_target_row_ndx, target_row_ndx);
        }
    }

    // Update backlinks to last row to point to its new position
    ref_type ref2 = ColumnLinkBase::get_as_ref(last_row_ndx);
    if (ref2) {
        const Column linkcol(ref2, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlink_column->update_backlink(old_target_row_ndx, last_row_ndx, target_row_ndx);
        }
    }

    // Do the actual delete and move
    ColumnLinkBase::destroy_subtree(target_row_ndx, false);
    ColumnLinkBase::move_last_over(target_row_ndx, last_row_ndx);

    const bool fix_ndx_in_parent = true;
    adj_move_last_over<fix_ndx_in_parent>(target_row_ndx, last_row_ndx);
}


void ColumnLinkList::erase(size_t row_ndx, bool is_last)
{
    TIGHTDB_ASSERT(row_ndx+1 == size());
    TIGHTDB_ASSERT(is_last);

    // Remove backlinks to the delete row
    ref_type ref = ColumnLinkBase::get_as_ref(row_ndx);
    if (ref) {
        const Column linkcol(ref, null_ptr, 0, get_alloc());
        size_t count = linkcol.size();
        for (size_t i = 0; i < count; ++i) {
            size_t old_target_row_ndx = linkcol.get(i);
            m_backlink_column->remove_backlink(old_target_row_ndx, row_ndx);
        }
    }

    // Do the actual delete
    ColumnLinkBase::destroy_subtree(row_ndx, false);
    ColumnLinkBase::erase(row_ndx, is_last);

    // Detach accessor, if any
    typedef list_accessors::iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i) {
        if (i->m_row_ndx == row_ndx) {
            i->m_list->detach();
            m_list_accessors.erase(i);
            break;
        }
    }
}


bool ColumnLinkList::compare_link_list(const ColumnLinkList& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i < n; ++i) {
        if (*get(i) != *c.get(i))
            return false;
    }
    return true;
}


void ColumnLinkList::do_nullify_link(size_t row_ndx, size_t old_target_row_ndx)
{
    LinkViewRef links = get(row_ndx);
    links->do_nullify_link(old_target_row_ndx);
}


void ColumnLinkList::do_update_link(size_t row_ndx, size_t old_target_row_ndx, size_t new_target_row_ndx)
{
    LinkViewRef links = get(row_ndx);
    links->do_update_link(old_target_row_ndx, new_target_row_ndx);
}


LinkView* ColumnLinkList::get_ptr(size_t row_ndx) const
{
    TIGHTDB_ASSERT(row_ndx < size());

    // Check if we already have a linkview for this row
    typedef list_accessors::const_iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i) {
        if (i->m_row_ndx == row_ndx)
            return i->m_list;
    }

    m_list_accessors.reserve(m_list_accessors.size() + 1); // Throws
    list_entry entry;
    entry.m_row_ndx = row_ndx;
    entry.m_list = new LinkView(m_table, const_cast<ColumnLinkList&>(*this), row_ndx); // Throws
    m_list_accessors.push_back(entry); // Not throwing due to space reservation
    return entry.m_list;
}


void ColumnLinkList::update_child_ref(size_t child_ndx, ref_type new_ref)
{
    ColumnLinkBase::set(child_ndx, new_ref);
}


ref_type ColumnLinkList::get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT
{
    return ColumnLinkBase::get(child_ndx);
}


void ColumnLinkList::to_json_row(size_t row_ndx, ostream& out) const
{
    LinkViewRef links1 = const_cast<ColumnLinkList*>(this)->get(row_ndx);
    for (size_t t = 0; t < links1->size(); t++) {
        if (t > 0)
            out << ", ";
        size_t target = links1->get(t).get_index();
        out << target;
    }
}


void ColumnLinkList::discard_child_accessors() TIGHTDB_NOEXCEPT
{
    typedef list_accessors::const_iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i)
        i->m_list->detach();
    m_list_accessors.clear();
}


void ColumnLinkList::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    ColumnLinkBase::refresh_accessor_tree(col_ndx, spec); // Throws
    m_column_ndx = col_ndx;
    typedef list_accessors::const_iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i)
        i->m_list->refresh_accessor_tree(i->m_row_ndx);
}


void ColumnLinkList::adj_accessors_move_last_over(size_t target_row_ndx,
                                                  size_t last_row_ndx) TIGHTDB_NOEXCEPT
{
    ColumnLinkBase::adj_accessors_move_last_over(target_row_ndx, last_row_ndx);

    const bool fix_ndx_in_parent = false;
    adj_move_last_over<fix_ndx_in_parent>(target_row_ndx, last_row_ndx);
}


void ColumnLinkList::adj_acc_clear_root_table() TIGHTDB_NOEXCEPT
{
    ColumnLinkBase::adj_acc_clear_root_table();
    discard_child_accessors();
}


template<bool fix_ndx_in_parent>
void ColumnLinkList::adj_move_last_over(size_t target_row_ndx,
                                        size_t last_row_ndx) TIGHTDB_NOEXCEPT
{
    // Search for either index in a tight loop for speed
    bool last_seen = false;
    size_t i = 0, n = m_list_accessors.size();
    for (;;) {
        if (i == n)
            return;
        const list_entry& e = m_list_accessors[i];
        if (e.m_row_ndx == target_row_ndx)
            goto target;
        if (e.m_row_ndx == last_row_ndx)
            break;
        ++i;
    }

    // Move list accessor at `last_row_ndx`, then look for `target_row_ndx`
    {
        list_entry& e = m_list_accessors[i];
        e.m_row_ndx = target_row_ndx;
        if (fix_ndx_in_parent)
            e.m_list->set_origin_row_index(target_row_ndx);
    }
    for (;;) {
        ++i;
        if (i == n)
            return;
        const list_entry& e = m_list_accessors[i];
        if (e.m_row_ndx == target_row_ndx)
            break;
    }
    last_seen = true;

    // Detach and remove original list accessor at `target_row_ndx`, then
    // look for `last_row_ndx
  target:
    {
        list_entry& e = m_list_accessors[i];
        // Must hold a counted reference while detaching
        LinkViewRef list(e.m_list);
        list->detach();
        // Delete entry by moving last over (faster and avoids invalidating
        // iterators)
        e = m_list_accessors[--n];
        m_list_accessors.pop_back();
    }
    if (!last_seen) {
        for (;;) {
            if (i == n)
                return;
            const list_entry& e = m_list_accessors[i];
            if (e.m_row_ndx == last_row_ndx)
                break;
            ++i;
        }
        {
            list_entry& e = m_list_accessors[i];
            e.m_row_ndx = target_row_ndx;
            if (fix_ndx_in_parent)
                e.m_list->set_origin_row_index(target_row_ndx);
        }
    }
}


void ColumnLinkList::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    if (!m_array->update_from_parent(old_baseline))
        return;

    typedef list_accessors::const_iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i)
        i->m_list->update_from_parent(old_baseline);
}


#ifdef TIGHTDB_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.Verify();
    TIGHTDB_ASSERT(leaf.has_refs());
    return leaf.size();
}

} // anonymous namespace

void ColumnLinkList::Verify() const
{
    if (root_is_leaf()) {
        m_array->Verify();
        TIGHTDB_ASSERT(m_array->has_refs());
        return;
    }

    m_array->verify_bptree(&verify_leaf);
}


void ColumnLinkList::Verify(const Table& table, size_t col_ndx) const
{
    ColumnLinkBase::Verify(table, col_ndx);

    vector<ColumnBackLink::VerifyPair> pairs;
    m_backlink_column->get_backlinks(pairs);

    // For each link list, verify the accessor, then check that the contents of
    // the list is in agreement with the corresponding backlinks. A forward link
    // (origin_row_ndx -> target_row_ndx) with multiplicity N must exists if,
    // and only if there exists a backward link (target_row_ndx ->
    // origin_row_ndx) with multiplicity N.
    size_t backlinks_seen = 0;
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        ConstLinkViewRef link_list = get(i);
        link_list->Verify(i);
        multiset<size_t> links_1, links_2;
        size_t m = link_list->size();
        for (size_t j = 0; j < m; ++j)
            links_1.insert(link_list->get(j).get_index());
        typedef vector<ColumnBackLink::VerifyPair>::const_iterator iter;
        ColumnBackLink::VerifyPair search_value;
        search_value.origin_row_ndx = i;
        pair<iter,iter> range = equal_range(pairs.begin(), pairs.end(), search_value);
        for (iter j = range.first; j != range.second; ++j)
            links_2.insert(j->target_row_ndx);
        TIGHTDB_ASSERT(links_1 == links_2);
        backlinks_seen += links_2.size();
    }

    // All backlinks must have been matched by a forward link
    TIGHTDB_ASSERT(backlinks_seen == pairs.size());
}


pair<ref_type, size_t> ColumnLinkList::get_to_dot_parent(size_t ndx_in_parent) const
{
    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_parent);
    return make_pair(p.first.m_ref, p.second);
}

#endif // TIGHTDB_DEBUG
