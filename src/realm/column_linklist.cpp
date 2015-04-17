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
#include <set>

#include <realm/column_linklist.hpp>
#include <realm/link_view.hpp>

using namespace std;
using namespace realm;


void ColumnLinkList::move_last_over(size_t row_ndx, size_t last_row_ndx,
                                    bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_3(row_ndx, <=, last_row_ndx);
    REALM_ASSERT_3(last_row_ndx + 1, ==, size());

    // Remove backlinks to the delete row
    if (!broken_reciprocal_backlinks) {
        if (ref_type ref = get_as_ref(row_ndx)) {
            Column link_list(get_alloc(), ref);
            size_t n = link_list.size();
            for (size_t i = 0; i < n; ++i) {
                size_t target_row_ndx = to_size_t(link_list.get(i));
                m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);
            }
        }
    }

    // Update backlinks to last row to point to its new position
    if (row_ndx != last_row_ndx) {
        if (ref_type ref = get_as_ref(last_row_ndx)) {
            Column link_list(get_alloc(), ref);
            size_t n = link_list.size();
            for (size_t i = 0; i < n; ++i) {
                size_t target_row_ndx = to_size_t(link_list.get(i));
                m_backlink_column->update_backlink(target_row_ndx, last_row_ndx, row_ndx);
            }
        }
    }

    // Do the actual delete and move
    bool clear_value = false;
    destroy_subtree(row_ndx, clear_value);
    Column::move_last_over(row_ndx, last_row_ndx);

    const bool fix_ndx_in_parent = true;
    adj_move_over<fix_ndx_in_parent>(last_row_ndx, row_ndx);
}


void ColumnLinkList::clear(size_t, bool broken_reciprocal_backlinks)
{
    if (!broken_reciprocal_backlinks) {
        size_t num_target_rows = m_target_table->size();
        m_backlink_column->remove_all_backlinks(num_target_rows); // Throws
    }

    // Do the actual deletion
    clear_without_updating_index(); // Throws
    // FIXME: This one is needed because Column::do_clear() forgets about the
    // leaf type. A better solution should probably be sought after.
    get_root_array()->set_type(Array::type_HasRefs); // Throws

    discard_child_accessors();
}


void ColumnLinkList::cascade_break_backlinks_to(size_t row_ndx, CascadeState& state)
{
    if (row_ndx == state.stop_on_link_list_row_ndx && this == state.stop_on_link_list_column)
        return;

    // Avoid the construction of both a LinkView and a Column instance, since
    // both would involve heap allocations.
    ref_type ref = get_as_ref(row_ndx);
    if (ref == 0)
        return;
    Array root(get_alloc());
    root.init_from_ref(ref);

    if (!root.is_inner_bptree_node()) {
        cascade_break_backlinks_to__leaf(row_ndx, root, state); // Throws
        return;
    }

    Array leaf(get_alloc());
    size_t link_ndx = 0;
    size_t num_links = root.get_bptree_size();
    while (link_ndx < num_links) {
        pair<MemRef, size_t> p = root.get_bptree_leaf(link_ndx);
        MemRef leaf_mem = p.first;
        leaf.init_from_mem(leaf_mem);
        cascade_break_backlinks_to__leaf(row_ndx, leaf, state); // Throws
        link_ndx += leaf.size();
    }
}


void ColumnLinkList::cascade_break_backlinks_to__leaf(size_t row_ndx, const Array& link_list_leaf,
                                                      CascadeState& state)
{
    size_t target_table_ndx = m_target_table->get_index_in_group();

    size_t num_links = link_list_leaf.size();
    for (size_t i = 0; i < num_links; ++i) {
        size_t target_row_ndx = to_size_t(link_list_leaf.get(i));

        // Remove the reciprocal backlink at target_row_ndx that points to row_ndx
        m_backlink_column->remove_one_backlink(target_row_ndx, row_ndx);

        if (m_weak_links)
            continue;
        if (m_target_table == state.stop_on_table)
            continue;

        // Recurse on target row when appropriate
        check_cascade_break_backlinks_to(target_table_ndx, target_row_ndx, state); // Throws
    }
}


void ColumnLinkList::cascade_break_backlinks_to_all_rows(size_t num_rows, CascadeState& state)
{
    size_t num_target_rows = m_target_table->size();
    m_backlink_column->remove_all_backlinks(num_target_rows);

    if (m_weak_links)
        return;
    if (m_target_table == state.stop_on_table)
        return;

    // Avoid the construction of both a LinkView and a Column instance, since
    // both would involve heap allocations.
    Array root(get_alloc()), leaf(get_alloc());
    for (size_t i = 0; i < num_rows; ++i) {
        ref_type ref = get_as_ref(i);
        if (ref == 0)
            continue;
        root.init_from_ref(ref);

        if (!root.is_inner_bptree_node()) {
            cascade_break_backlinks_to_all_rows__leaf(root, state); // Throws
            continue;
        }

        size_t link_ndx = 0;
        size_t num_links = root.get_bptree_size();
        while (link_ndx < num_links) {
            pair<MemRef, size_t> p = root.get_bptree_leaf(link_ndx);
            MemRef leaf_mem = p.first;
            leaf.init_from_mem(leaf_mem);
            cascade_break_backlinks_to_all_rows__leaf(leaf, state); // Throws
            link_ndx += leaf.size();
        }
    }
}


void ColumnLinkList::cascade_break_backlinks_to_all_rows__leaf(const Array& link_list_leaf,
                                                               CascadeState& state)
{
    size_t target_table_ndx = m_target_table->get_index_in_group();

    size_t num_links = link_list_leaf.size();
    for (size_t i = 0; i < num_links; ++i) {
        size_t target_row_ndx = to_size_t(link_list_leaf.get(i));

        // Recurse on target row when appropriate
        check_cascade_break_backlinks_to(target_table_ndx, target_row_ndx, state); // Throws
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
    REALM_ASSERT_3(row_ndx, <, size());

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


ref_type ColumnLinkList::get_child_ref(size_t child_ndx) const REALM_NOEXCEPT
{
    return ColumnLinkBase::get_as_ref(child_ndx);
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


void ColumnLinkList::discard_child_accessors() REALM_NOEXCEPT
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


void ColumnLinkList::adj_acc_move_over(size_t from_row_ndx, size_t to_row_ndx) REALM_NOEXCEPT
{
    ColumnLinkBase::adj_acc_move_over(from_row_ndx, to_row_ndx);

    const bool fix_ndx_in_parent = false;
    adj_move_over<fix_ndx_in_parent>(from_row_ndx, to_row_ndx);
}


void ColumnLinkList::adj_acc_clear_root_table() REALM_NOEXCEPT
{
    ColumnLinkBase::adj_acc_clear_root_table();
    discard_child_accessors();
}

template<bool fix_ndx_in_parent>
void ColumnLinkList::adj_move_over(size_t from_row_ndx, size_t to_row_ndx) REALM_NOEXCEPT
{
    size_t i = 0, n = m_list_accessors.size();
    while (i < n) {
        list_entry& e = m_list_accessors[i];
        if (REALM_UNLIKELY(e.m_row_ndx == to_row_ndx)) {
            // Must hold a counted reference while detaching
            LinkViewRef list(e.m_list);
            list->detach();
            // Delete entry by moving last over (faster and avoids invalidating
            // iterators)
            e = m_list_accessors[--n];
            m_list_accessors.pop_back();
        }
        else {
            if (REALM_UNLIKELY(e.m_row_ndx == from_row_ndx)) {
                e.m_row_ndx = to_row_ndx;
                if (fix_ndx_in_parent)
                    e.m_list->set_origin_row_index(to_row_ndx);
            }
            ++i;
        }
    }
}


void ColumnLinkList::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    if (!get_root_array()->update_from_parent(old_baseline))
        return;

    typedef list_accessors::const_iterator iter;
    iter end = m_list_accessors.end();
    for (iter i = m_list_accessors.begin(); i != end; ++i)
        i->m_list->update_from_parent(old_baseline);
}


#ifdef REALM_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.Verify();
    REALM_ASSERT(leaf.has_refs());
    return leaf.size();
}

} // anonymous namespace

void ColumnLinkList::Verify() const
{
    if (root_is_leaf()) {
        get_root_array()->Verify();
        REALM_ASSERT(get_root_array()->has_refs());
        return;
    }

    get_root_array()->verify_bptree(&verify_leaf);
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
        REALM_ASSERT(links_1 == links_2);
        backlinks_seen += links_2.size();
    }

    // All backlinks must have been matched by a forward link
    REALM_ASSERT_3(backlinks_seen, ==, pairs.size());
}


pair<ref_type, size_t> ColumnLinkList::get_to_dot_parent(size_t ndx_in_parent) const
{
    pair<MemRef, size_t> p = get_root_array()->get_bptree_leaf(ndx_in_parent);
    return make_pair(p.first.m_ref, p.second);
}

#endif // REALM_DEBUG
