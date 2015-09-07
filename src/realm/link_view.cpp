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

#include <realm/link_view.hpp>
#include <realm/column_linklist.hpp>
#include <realm/replication.hpp>
#include <realm/table_view.hpp>

using namespace realm;

void LinkView::generate_patch(const ConstLinkViewRef& ref, std::unique_ptr<Handover_patch>& patch)
{
    if (bool(ref)) {
        patch.reset(new Handover_patch);
        patch->m_table_num = ref->m_origin_table->get_index_in_group();
        patch->m_col_num = ref->m_origin_column.m_column_ndx;
        patch->m_row_ndx = ref->get_origin_row_index();
    }
    else
        patch.reset();
}


LinkViewRef LinkView::create_from_and_consume_patch(std::unique_ptr<Handover_patch>& patch, Group& group) 
{
    if (patch) {
        TableRef tr(group.get_table(patch->m_table_num));
        LinkViewRef result = tr->get_linklist(patch->m_col_num, patch->m_row_ndx);
        patch.reset();
        return result;
    }
    else
        return LinkViewRef();
}


void LinkView::insert(size_t link_ndx, size_t target_row_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_7(m_row_indexes.is_attached(), ==, true, ||, link_ndx, ==, 0);
    REALM_ASSERT_7(m_row_indexes.is_attached(), ==, false, ||, link_ndx, <=, m_row_indexes.size());
    REALM_ASSERT_3(target_row_ndx, <, m_origin_column.get_target_table().size());
    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);

    size_t origin_row_ndx = get_origin_row_index();

    // if there are no links yet, we have to create list
    if (!m_row_indexes.is_attached()) {
        REALM_ASSERT_3(link_ndx, ==, 0);
        ref_type ref = IntegerColumn::create(m_origin_column.get_alloc()); // Throws
        m_origin_column.set_row_ref(origin_row_ndx, ref); // Throws
        m_row_indexes.init_from_parent(); // re-attach
    }

    m_row_indexes.insert(link_ndx, target_row_ndx); // Throws
    m_origin_column.add_backlink(target_row_ndx, origin_row_ndx); // Throws

    if (Replication* repl = get_repl())
        repl->link_list_insert(*this, link_ndx, target_row_ndx); // Throws
}


void LinkView::set(size_t link_ndx, size_t target_row_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_7(m_row_indexes.is_attached(), ==, true, &&, link_ndx, <, m_row_indexes.size());
    REALM_ASSERT_3(target_row_ndx, <, m_origin_column.get_target_table().size());

    if (Replication* repl = get_repl())
        repl->link_list_set(*this, link_ndx, target_row_ndx); // Throws

    size_t old_target_row_ndx = do_set(link_ndx, target_row_ndx); // Throws
    if (m_origin_column.m_weak_links)
        return;

    Table& target_table = m_origin_column.get_target_table();
    size_t num_remaining = target_table.get_num_strong_backlinks(old_target_row_ndx);
    if (num_remaining > 0)
        return;

    CascadeState::row target_row;
    target_row.table_ndx = target_table.get_index_in_group();
    target_row.row_ndx   = old_target_row_ndx;
    CascadeState state;
    state.rows.push_back(target_row);

    typedef _impl::TableFriend tf;
    tf::cascade_break_backlinks_to(target_table, old_target_row_ndx, state); // Throws
    tf::remove_backlink_broken_rows(target_table, state); // Throws
}


// Replication instruction 'link-list-set' calls this function directly.
size_t LinkView::do_set(size_t link_ndx, size_t target_row_ndx)
{
    size_t old_target_row_ndx = m_row_indexes.get(link_ndx);
    size_t origin_row_ndx = get_origin_row_index();
    m_origin_column.remove_backlink(old_target_row_ndx, origin_row_ndx); // Throws
    m_origin_column.add_backlink(target_row_ndx, origin_row_ndx); // Throws
    m_row_indexes.set(link_ndx, target_row_ndx); // Throws
    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);
    return old_target_row_ndx;
}


void LinkView::move(size_t old_link_ndx, size_t new_link_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT(m_row_indexes.is_attached());
    REALM_ASSERT_3(old_link_ndx, <, m_row_indexes.size());
    REALM_ASSERT_3(new_link_ndx, <, m_row_indexes.size());

    if (old_link_ndx == new_link_ndx)
        return;
    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);

    size_t target_row_ndx = m_row_indexes.get(old_link_ndx);
    m_row_indexes.erase(old_link_ndx);
    m_row_indexes.insert(new_link_ndx, target_row_ndx);

    if (Replication* repl = get_repl())
        repl->link_list_move(*this, old_link_ndx, new_link_ndx); // Throws
}

void LinkView::swap(size_t link_ndx_1, size_t link_ndx_2)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (REALM_UNLIKELY(!m_row_indexes.is_attached() || link_ndx_1 >= m_row_indexes.size() ||
                       link_ndx_2 >= m_row_indexes.size()))
        throw LogicError(LogicError::link_index_out_of_range);

    // Internally, core requires that the first link index is strictly less than
    // the second one. The changeset merge mechanism is written to take
    // advantage of it, and requires it.
    if (link_ndx_1 == link_ndx_2)
        return;
    if (link_ndx_1 > link_ndx_2)
        std::swap(link_ndx_1, link_ndx_2);

    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);

    size_t target_row_ndx = m_row_indexes.get(link_ndx_1);
    m_row_indexes.set(link_ndx_1, m_row_indexes.get(link_ndx_2));
    m_row_indexes.set(link_ndx_2, target_row_ndx);

    if (Replication* repl = get_repl())
        repl->link_list_swap(*this, link_ndx_1, link_ndx_2); // Throws
}


void LinkView::remove(size_t link_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_7(m_row_indexes.is_attached(), ==, true, &&, link_ndx, <, m_row_indexes.size());

    if (Replication* repl = get_repl())
        repl->link_list_erase(*this, link_ndx); // Throws

    size_t target_row_ndx = do_remove(link_ndx); // Throws
    if (m_origin_column.m_weak_links)
        return;

    Table& target_table = m_origin_column.get_target_table();
    size_t num_remaining = target_table.get_num_strong_backlinks(target_row_ndx);
    if (num_remaining > 0)
        return;

    CascadeState::row target_row;
    target_row.table_ndx = target_table.get_index_in_group();
    target_row.row_ndx   = target_row_ndx;
    CascadeState state;
    state.rows.push_back(target_row);

    typedef _impl::TableFriend tf;
    tf::cascade_break_backlinks_to(target_table, target_row_ndx, state); // Throws
    tf::remove_backlink_broken_rows(target_table, state); // Throws
}


// Replication instruction 'link-list-erase' calls this function directly.
size_t LinkView::do_remove(size_t link_ndx)
{
    size_t target_row_ndx = m_row_indexes.get(link_ndx);
    size_t origin_row_ndx = get_origin_row_index();
    m_origin_column.remove_backlink(target_row_ndx, origin_row_ndx); // Throws
    m_row_indexes.erase(link_ndx); // Throws
    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);
    return target_row_ndx;
}


void LinkView::clear()
{
    REALM_ASSERT(is_attached());

    if (!m_row_indexes.is_attached())
        return;

    if (Replication* repl = get_repl())
        repl->link_list_clear(*this); // Throws

    if (m_origin_column.m_weak_links) {
        bool broken_reciprocal_backlinks = false;
        do_clear(broken_reciprocal_backlinks); // Throws
        return;
    }

    size_t origin_row_ndx = get_origin_row_index();
    CascadeState state;
    state.stop_on_link_list_column  = &m_origin_column;
    state.stop_on_link_list_row_ndx = origin_row_ndx;

    typedef _impl::TableFriend tf;
    size_t num_links = m_row_indexes.size();
    for (size_t link_ndx = 0; link_ndx < num_links; ++link_ndx) {
        size_t target_row_ndx = m_row_indexes.get(link_ndx);
        m_origin_column.remove_backlink(target_row_ndx, origin_row_ndx); // Throws
        Table& target_table = m_origin_column.get_target_table();
        size_t num_remaining = target_table.get_num_strong_backlinks(target_row_ndx);
        if (num_remaining > 0)
            continue;
        CascadeState::row target_row;
        target_row.table_ndx = target_table.get_index_in_group();
        target_row.row_ndx   = target_row_ndx;
        auto i = std::upper_bound(state.rows.begin(), state.rows.end(), target_row);
        // This target row cannot already be in state.rows
        REALM_ASSERT(i == state.rows.begin() || i[-1] != target_row);
        state.rows.insert(i, target_row);
        tf::cascade_break_backlinks_to(target_table, target_row_ndx, state); // Throws
    }

    bool broken_reciprocal_backlinks = true;
    do_clear(broken_reciprocal_backlinks); // Throws

    tf::remove_backlink_broken_rows(*m_origin_table, state); // Throws
}


// Replication instruction 'link-list-clear' calls this function directly.
void LinkView::do_clear(bool broken_reciprocal_backlinks)
{
    size_t origin_row_ndx = get_origin_row_index();
    if (!broken_reciprocal_backlinks) {
        size_t num_links = m_row_indexes.size();
        for (size_t link_ndx = 0; link_ndx < num_links; ++link_ndx) {
            size_t target_row_ndx = m_row_indexes.get(link_ndx);
            m_origin_column.remove_backlink(target_row_ndx, origin_row_ndx); // Throws
        }
    }

    m_row_indexes.destroy();
    m_origin_column.set_row_ref(origin_row_ndx, 0); // Throws

    typedef _impl::TableFriend tf;
    tf::bump_version(*m_origin_table);
}


void LinkView::sort(size_t column, bool ascending)
{
    std::vector<size_t> c;
    std::vector<bool> a;
    c.push_back(column);
    a.push_back(ascending);
    sort(c, a);
}


void LinkView::sort(std::vector<size_t> columns, std::vector<bool> ascending)
{
    if (Replication* repl = get_repl()) {
        // todo, write to the replication log that we're doing a sort
        repl->set_link_list(*this, m_row_indexes); // Throws
    }
    Sorter predicate(columns, ascending);
    RowIndexes::sort(predicate);
}

TableView LinkView::get_sorted_view(std::vector<size_t> column_indexes, std::vector<bool> ascending) const
{
    TableView v(m_origin_column.get_target_table()); // sets m_table
    v.m_last_seen_version = m_origin_table->m_version;
    // sets m_linkview_source to indicate that this TableView was generated from a LinkView
    v.m_linkview_source = ConstLinkViewRef(this);
    if (m_row_indexes.is_attached()) {
        for (size_t t = 0; t < m_row_indexes.size(); t++) // todo, simpler way?
            v.m_row_indexes.add(get(t).get_index());
        v.sort(column_indexes, ascending);
    }
    return v;
}

TableView LinkView::get_sorted_view(size_t column_index, bool ascending) const
{
    std::vector<size_t> vec;
    std::vector<bool> a;
    vec.push_back(column_index);
    a.push_back(ascending);
    TableView v = get_sorted_view(vec, a);
    return v;
}


void LinkView::remove_target_row(size_t link_ndx)
{
    REALM_ASSERT(is_attached());
    REALM_ASSERT_7(m_row_indexes.is_attached(), ==, true, &&, link_ndx, <, m_row_indexes.size());

    size_t target_row_ndx = m_row_indexes.get(link_ndx);
    Table& target_table = get_target_table();

    // Deleting the target row will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    target_table.move_last_over(target_row_ndx);
}


void LinkView::remove_all_target_rows()
{
    REALM_ASSERT(is_attached());

    if (m_row_indexes.is_attached()) {
        Table& target_table = get_target_table();
        bool is_move_last_over = true;
        target_table.batch_erase_rows(m_row_indexes, is_move_last_over);
    }
}


void LinkView::do_nullify_link(size_t old_target_row_ndx)
{
    REALM_ASSERT(m_row_indexes.is_attached());

    size_t pos = m_row_indexes.find_first(old_target_row_ndx);
    REALM_ASSERT_3(pos, !=, realm::not_found);

    if (Replication* repl = m_origin_table->get_repl())
        repl->link_list_nullify(*this, pos);

    m_row_indexes.erase(pos);

    if (m_row_indexes.is_empty()) {
        m_row_indexes.destroy();
        size_t origin_row_ndx = get_origin_row_index();
        m_origin_column.set_row_ref(origin_row_ndx, 0);
    }
}


void LinkView::do_update_link(size_t old_target_row_ndx, size_t new_target_row_ndx)
{
    REALM_ASSERT(m_row_indexes.is_attached());

    // While there may be multiple links in this list pointing to the specified
    // old target row index, This function is supposed to only update the first
    // one. If there are more links pointing to the same target row, they will
    // be updated by subsequent involcations of this function. I.e., it is the
    // responsibility of the caller to call this function the right number of
    // times.
    size_t pos = m_row_indexes.find_first(old_target_row_ndx);
    REALM_ASSERT_3(pos, !=, realm::not_found);

    m_row_indexes.set(pos, new_target_row_ndx);
}


void LinkView::repl_unselect() noexcept
{
    if (Replication* repl = get_repl())
        repl->on_link_list_destroyed(*this);
}


#ifdef REALM_DEBUG

void LinkView::verify(size_t row_ndx) const
{
    // Only called for attached lists
    REALM_ASSERT(is_attached());

    REALM_ASSERT_3(m_row_indexes.get_root_array()->get_ndx_in_parent(), ==, row_ndx);
    bool not_degenerate = m_row_indexes.get_root_array()->get_ref_from_parent() != 0;
    REALM_ASSERT_3(not_degenerate, ==, m_row_indexes.is_attached());
    if (m_row_indexes.is_attached())
        m_row_indexes.verify();
}

#endif // REALM_DEBUG
