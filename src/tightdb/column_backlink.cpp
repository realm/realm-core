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

#include <tightdb/column_backlink.hpp>
#include <tightdb/column_link.hpp>
#include <tightdb/table.hpp>

using namespace std;
using namespace tightdb;


void ColumnBackLink::add_backlink(size_t row_ndx, size_t origin_row_ndx)
{
    uint64_t value = Column::get_uint(row_ndx);

    // A backlink list of size 1 is stored as a single non-ref column value.
    if (value == 0) {
        Column::set_uint(row_ndx, origin_row_ndx << 1 | 1); // Throws
        return;
    }

    ref_type ref;
    // When increasing the size of the backlink list from 1 to 2, we need to
    // convert from the single non-ref column value representation, to a B+-tree
    // representation.
    if ((value & 1) != 0) {
        // Create new column to hold backlinks
        size_t size = 1;
        int_fast64_t value_2 = value >> 1;
        ref = Column::create(get_alloc(), Array::type_Normal, size, value_2); // Throws
        Column::set_as_ref(row_ndx, ref); // Throws
    }
    else {
        ref = to_ref(value);
    }
    Column backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    backlink_list.add(int_fast64_t(origin_row_ndx)); // Throws
}


size_t ColumnBackLink::get_backlink_count(size_t row_ndx) const TIGHTDB_NOEXCEPT
{
    uint64_t value = Column::get_uint(row_ndx);

    if (value == 0)
        return 0;

    if ((value & 1) != 0)
        return 1;

    // get list size
    ref_type ref = to_ref(value);
    return ColumnBase::get_size_from_ref(ref, get_alloc());
}


size_t ColumnBackLink::get_backlink(size_t row_ndx, size_t backlink_ndx) const TIGHTDB_NOEXCEPT
{
    uint64_t value = Column::get_uint(row_ndx);
    TIGHTDB_ASSERT_NEW(value, !=, 0);

    size_t origin_row_ndx;
    if ((value & 1) != 0) {
        TIGHTDB_ASSERT_NEW(backlink_ndx, ==, 0);
        origin_row_ndx = to_size_t(value >> 1);
    }
    else {
        ref_type ref = to_ref(value);
        TIGHTDB_ASSERT_NEW(backlink_ndx, <, ColumnBase::get_size_from_ref(ref, get_alloc()));
        // FIXME: Optimize with direct access (that is, avoid creation of a
        // Column instance, since that implies dynamic allocation).
        Column backlink_list(get_alloc(), ref); // Throws
        uint64_t value_2 = backlink_list.get_uint(backlink_ndx);
        origin_row_ndx = to_size_t(value_2);
    }
    return origin_row_ndx;
}


void ColumnBackLink::remove_one_backlink(size_t row_ndx, size_t origin_row_ndx)
{
    uint64_t value = Column::get_uint(row_ndx);
    TIGHTDB_ASSERT_NEW(value, !=, 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        TIGHTDB_ASSERT_NEW(to_size_t(value >> 1), ==, origin_row_ndx);
        Column::set(row_ndx, 0);
        return;
    }

    // if there is a list of backlinks we have to find
    // the right one and remove it.
    ref_type ref = to_ref(value);
    Column backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    int_fast64_t value_2 = int_fast64_t(origin_row_ndx);
    size_t backlink_ndx = backlink_list.find_first(value_2);
    TIGHTDB_ASSERT_NEW(backlink_ndx, !=, not_found);
    size_t num_links = backlink_list.size();
    bool is_last = backlink_ndx + 1 == num_links;
    backlink_list.erase(backlink_ndx, is_last);
    --num_links;

    // If there is only one backlink left we can inline it as tagged value
    if (num_links == 1) {
        uint64_t value_3 = backlink_list.get_uint(0);
        backlink_list.destroy();

        int_fast64_t value_4 = value_3 << 1 | 1;
        Column::set_uint(row_ndx, value_4);
    }
}


void ColumnBackLink::remove_all_backlinks(size_t num_rows)
{
    Allocator& alloc = m_array->get_alloc();
    for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
        // List lists with more than one element are represented by a B+ tree,
        // whose nodes need to be freed.
        uint64_t value = Column::get(row_ndx);
        if (value && (value & 1) == 0) {
            ref_type ref = to_ref(value);
            Array::destroy_deep(ref, alloc);
        }
        Column::set(row_ndx, 0);
    }
}


void ColumnBackLink::update_backlink(size_t row_ndx, size_t old_origin_row_ndx,
                                     size_t new_origin_row_ndx)
{
    uint64_t value = Column::get_uint(row_ndx);
    TIGHTDB_ASSERT_NEW(value, !=, 0);

    if ((value & 1) != 0) {
        TIGHTDB_ASSERT_NEW(to_size_t(value >> 1), ==, old_origin_row_ndx);
        uint64_t value_2 = new_origin_row_ndx << 1 | 1;
        Column::set_uint(row_ndx, value_2);
        return;
    }

    // Find match in backlink list and replace
    ref_type ref = to_ref(value);
    Column backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    int_fast64_t value_2 = int_fast64_t(old_origin_row_ndx);
    size_t backlink_ndx = backlink_list.find_first(value_2);
    TIGHTDB_ASSERT_NEW(backlink_ndx, !=, not_found);
    int_fast64_t value_3 = int_fast64_t(new_origin_row_ndx);
    backlink_list.set(backlink_ndx, value_3);
}


void ColumnBackLink::nullify_links(size_t row_ndx, bool do_destroy)
{
    // Nullify all links pointing to the row being deleted
    uint64_t value = Column::get_uint(row_ndx);
    if (value != 0) {
        if ((value & 1) != 0) {
            size_t origin_row_ndx = to_size_t(value >> 1);
            m_origin_column->do_nullify_link(origin_row_ndx, row_ndx); // Throws
        }
        else {
            // nullify entire list of links
            ref_type ref = to_ref(value);
            Column backlink_list(get_alloc(), ref); // Throws

            size_t n = backlink_list.size();
            for (size_t i = 0; i < n; ++i) {
                int_fast64_t value_2 = backlink_list.get(i);
                size_t origin_row_ndx = to_size_t(value_2);
                m_origin_column->do_nullify_link(origin_row_ndx, row_ndx); // Throws
            }

            if (do_destroy)
                backlink_list.destroy();
        }
    }
}


void ColumnBackLink::erase(size_t, bool)
{
    // This operation is not available for unordered tables, and only unordered
    // tables may have link columns.
    TIGHTDB_ASSERT(false);
}


void ColumnBackLink::move_last_over(size_t row_ndx, size_t last_row_ndx, bool)
{
    TIGHTDB_ASSERT_DEBUG(row_ndx <= last_row_ndx);
    TIGHTDB_ASSERT_DEBUG(last_row_ndx + 1 == size());

    // Nullify all links pointing to the row being deleted
    bool do_destroy = true;
    nullify_links(row_ndx, do_destroy); // Throws

    // Update all links to the last row to point to the new row instead
    uint64_t value = Column::get_uint(last_row_ndx);
    if (row_ndx != last_row_ndx) {
        if (value != 0) {
            if ((value & 1) != 0) {
                size_t origin_row_ndx = to_size_t(value >> 1);
                m_origin_column->do_update_link(origin_row_ndx, last_row_ndx,
                                                row_ndx); // Throws
            }
            else {
                // update entire list of links
                ref_type ref = to_ref(value);
                Column backlink_list(get_alloc(), ref); // Throws

                size_t n = backlink_list.size();
                for (size_t i = 0; i < n; ++i) {
                    int_fast64_t value_2 = backlink_list.get(i);
                    size_t origin_row_ndx = to_size_t(value_2);
                    m_origin_column->do_update_link(origin_row_ndx, last_row_ndx,
                                                    row_ndx); // Throws
                }
            }
        }
    }

    // Do the actual move
    Column::set_uint(row_ndx, value); // Throws
    bool is_last = true;
    do_erase(last_row_ndx, is_last); // Throws
}


void ColumnBackLink::clear(std::size_t num_rows, bool)
{
    for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
        // Column::clear() handles the destruction of subtrees
        bool do_destroy = false;
        nullify_links(row_ndx, do_destroy); // Throws
    }

    do_clear(); // Throws
    // FIXME: This one is needed because Column::do_clear() forgets about
    // the leaf type. A better solution should probably be found.
    m_array->set_type(Array::type_HasRefs);
}


void ColumnBackLink::update_child_ref(size_t child_ndx, ref_type new_ref)
{
    Column::set(child_ndx, new_ref); // Throws
}


ref_type ColumnBackLink::get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT
{
    return Column::get_as_ref(child_ndx);
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

void ColumnBackLink::Verify() const
{
    if (root_is_leaf()) {
        m_array->Verify();
        TIGHTDB_ASSERT(m_array->has_refs());
        return;
    }

    m_array->verify_bptree(&verify_leaf);
}

void ColumnBackLink::Verify(const Table& table, size_t col_ndx) const
{
    Column::Verify(table, col_ndx);

    // Check that the origin column specifies the right target
    TIGHTDB_ASSERT(&m_origin_column->get_target_table() == &table);
    TIGHTDB_ASSERT(&m_origin_column->get_backlink_column() == this);

    // Check that m_origin_table is the table specified by the spec
    size_t origin_table_ndx = m_origin_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    TIGHTDB_ASSERT_NEW(origin_table_ndx, ==, spec.get_opposite_link_table_ndx(col_ndx));
}


void ColumnBackLink::get_backlinks(vector<VerifyPair>& pairs)
{
    VerifyPair pair;
    size_t n = size();
    for (size_t i = 0; i < n ; ++i) {
        pair.target_row_ndx = i;
        size_t m = get_backlink_count(i);
        for (size_t j = 0; j < m; ++j) {
            pair.origin_row_ndx = get_backlink(i,j);
            pairs.push_back(pair);
        }
    }
    sort(pairs.begin(), pairs.end());
}


pair<ref_type, size_t> ColumnBackLink::get_to_dot_parent(size_t ndx_in_parent) const
{
    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_parent);
    return make_pair(p.first.m_ref, p.second);
}

#endif // TIGHTDB_DEBUG
