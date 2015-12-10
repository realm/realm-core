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
#include <set> // FIXME: Used for swap

#include <realm/column_backlink.hpp>
#include <realm/column_link.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

#include <realm/util/miscellaneous.hpp>

using namespace realm;


void BacklinkColumn::add_backlink(size_t row_ndx, size_t origin_row_ndx)
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);

    // A backlink list of size 1 is stored as a single non-ref column value.
    if (value == 0) {
        IntegerColumn::set_uint(row_ndx, origin_row_ndx << 1 | 1); // Throws
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
        ref = IntegerColumn::create(get_alloc(), Array::type_Normal, size, value_2); // Throws
        IntegerColumn::set_as_ref(row_ndx, ref); // Throws
    }
    else {
        ref = to_ref(value);
    }
    IntegerColumn backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    backlink_list.add(int_fast64_t(origin_row_ndx)); // Throws
}


size_t BacklinkColumn::get_backlink_count(size_t row_ndx) const noexcept
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);

    if (value == 0)
        return 0;

    if ((value & 1) != 0)
        return 1;

    // get list size
    ref_type ref = to_ref(value);
    return ColumnBase::get_size_from_ref(ref, get_alloc());
}


size_t BacklinkColumn::get_backlink(size_t row_ndx, size_t backlink_ndx) const noexcept
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);
    REALM_ASSERT_3(value, !=, 0);

    size_t origin_row_ndx;
    if ((value & 1) != 0) {
        REALM_ASSERT_3(backlink_ndx, ==, 0);
        origin_row_ndx = to_size_t(value >> 1);
    }
    else {
        ref_type ref = to_ref(value);
        REALM_ASSERT_3(backlink_ndx, <, ColumnBase::get_size_from_ref(ref, get_alloc()));
        // FIXME: Optimize with direct access (that is, avoid creation of a
        // Column instance, since that implies dynamic allocation).
        IntegerColumn backlink_list(get_alloc(), ref); // Throws
        uint64_t value_2 = backlink_list.get_uint(backlink_ndx);
        origin_row_ndx = to_size_t(value_2);
    }
    return origin_row_ndx;
}


void BacklinkColumn::remove_one_backlink(size_t row_ndx, size_t origin_row_ndx)
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);
    REALM_ASSERT_3(value, !=, 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        REALM_ASSERT_3(to_size_t(value >> 1), ==, origin_row_ndx);
        IntegerColumn::set(row_ndx, 0);
        return;
    }

    // if there is a list of backlinks we have to find
    // the right one and remove it.
    ref_type ref = to_ref(value);
    IntegerColumn backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    int_fast64_t value_2 = int_fast64_t(origin_row_ndx);
    size_t backlink_ndx = backlink_list.find_first(value_2);
    REALM_ASSERT_3(backlink_ndx, !=, not_found);
    backlink_list.erase(backlink_ndx); // Throws

    // If there is only one backlink left we can inline it as tagged value
    if (backlink_list.size() == 1) {
        uint64_t value_3 = backlink_list.get_uint(0);
        backlink_list.destroy();

        int_fast64_t value_4 = value_3 << 1 | 1;
        IntegerColumn::set_uint(row_ndx, value_4);
    }
}


void BacklinkColumn::remove_all_backlinks(size_t num_rows)
{
    Allocator& alloc = get_alloc();
    for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
        // List lists with more than one element are represented by a B+ tree,
        // whose nodes need to be freed.
        uint64_t value = IntegerColumn::get(row_ndx);
        if (value && (value & 1) == 0) {
            ref_type ref = to_ref(value);
            Array::destroy_deep(ref, alloc);
        }
        IntegerColumn::set(row_ndx, 0);
    }
}


void BacklinkColumn::update_backlink(size_t row_ndx, size_t old_origin_row_ndx,
                                     size_t new_origin_row_ndx)
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);
    REALM_ASSERT_3(value, !=, 0);

    if ((value & 1) != 0) {
        REALM_ASSERT_3(to_size_t(value >> 1), ==, old_origin_row_ndx);
        uint64_t value_2 = new_origin_row_ndx << 1 | 1;
        IntegerColumn::set_uint(row_ndx, value_2);
        return;
    }

    // Find match in backlink list and replace
    ref_type ref = to_ref(value);
    IntegerColumn backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    int_fast64_t value_2 = int_fast64_t(old_origin_row_ndx);
    size_t backlink_ndx = backlink_list.find_first(value_2);
    REALM_ASSERT_3(backlink_ndx, !=, not_found);
    int_fast64_t value_3 = int_fast64_t(new_origin_row_ndx);
    backlink_list.set(backlink_ndx, value_3);
}

void BacklinkColumn::swap_backlinks(size_t row_ndx, size_t origin_row_ndx_1,
                                    size_t origin_row_ndx_2)
{
    uint64_t value = Column::get_uint(row_ndx);
    REALM_ASSERT_3(value, !=, 0);

    if ((value & 1) != 0) {
        uint64_t r = value >> 1;
        if (r == origin_row_ndx_1) {
            IntegerColumn::set_uint(row_ndx, origin_row_ndx_2 << 1 | 1);
        }
        else if (r == origin_row_ndx_2) {
            IntegerColumn::set_uint(row_ndx, origin_row_ndx_1 << 1 | 1);
        }
        return;
    }

    // Find matches in backlink list and replace
    ref_type ref = to_ref(value);
    IntegerColumn backlink_list(get_alloc(), ref); // Throws
    backlink_list.set_parent(this, row_ndx);
    size_t num_backlinks = backlink_list.size();
    for (size_t i = 0; i < num_backlinks; ++i) {
        uint64_t r = backlink_list.get_uint(i);
        if (r == origin_row_ndx_1) {
            backlink_list.set(i, origin_row_ndx_2);
        }
        else if (r == origin_row_ndx_2) {
            backlink_list.set(i, origin_row_ndx_1);
        }
    }
}


template<typename Func>
size_t BacklinkColumn::for_each_link(size_t row_ndx, bool do_destroy, Func&& func)
{
    uint64_t value = IntegerColumn::get_uint(row_ndx);
    if (value != 0) {
        if ((value & 1) != 0) {
            size_t origin_row_ndx = to_size_t(value >> 1);
            func(origin_row_ndx); // Throws
        }
        else {
            ref_type ref = to_ref(value);
            IntegerColumn backlink_list(get_alloc(), ref); // Throws

            size_t n = backlink_list.size();
            for (size_t i = 0; i < n; ++i) {
                int_fast64_t value_2 = backlink_list.get(i);
                size_t origin_row_ndx = to_size_t(value_2);
                func(origin_row_ndx); // Throws
            }

            if (do_destroy)
                backlink_list.destroy();
        }
    }
    return to_size_t(value);
}


void BacklinkColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool insert_nulls)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx <= prior_num_rows);
    REALM_ASSERT(!insert_nulls);

    // Update forward links to the moved target rows
    size_t num_rows_moved = prior_num_rows - row_ndx;
    for (size_t i = num_rows_moved; i > 0; --i) {
        size_t old_target_row_ndx = row_ndx + i - 1;
        size_t new_target_row_ndx = row_ndx + num_rows_to_insert + i - 1;
        auto handler = [=](size_t origin_row_ndx) {
            m_origin_column->do_update_link(origin_row_ndx, old_target_row_ndx,
                                            new_target_row_ndx); // Throws
        };
        bool do_destroy = false;
        for_each_link(old_target_row_ndx, do_destroy, handler); // Throws
    }

    IntegerColumn::insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, insert_nulls); // Throws
}


void BacklinkColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                                bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_ndx <= prior_num_rows - num_rows_to_erase);

    // Nullify forward links to the removed target rows
    for (size_t i = 0; i < num_rows_to_erase; ++i) {
        auto handler = [=](size_t origin_row_ndx) {
            m_origin_column->do_nullify_link(origin_row_ndx, row_ndx+i); // Throws
        };
        bool do_destroy = true;
        for_each_link(row_ndx, do_destroy, handler); // Throws
    }

    // Update forward links to the moved target rows
    size_t num_rows_moved = prior_num_rows - (row_ndx + num_rows_to_erase);
    for (size_t i = 0; i < num_rows_moved; ++i) {
        size_t old_target_row_ndx = row_ndx + num_rows_to_erase + i;
        size_t new_target_row_ndx = row_ndx + i;
        auto handler = [=](size_t origin_row_ndx) {
            m_origin_column->do_update_link(origin_row_ndx, old_target_row_ndx,
                                            new_target_row_ndx); // Throws
        };
        bool do_destroy = false;
        for_each_link(old_target_row_ndx, do_destroy, handler); // Throws
    }

    IntegerColumn::erase_rows(row_ndx, num_rows_to_erase, prior_num_rows,
                              broken_reciprocal_backlinks); // Throws
}


void BacklinkColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows,
                                        bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx < prior_num_rows);

    // Nullify forward links to the removed target row
    auto handler = [=](size_t origin_row_ndx) {
        m_origin_column->do_nullify_link(origin_row_ndx, row_ndx);  // Throws
    };
    bool do_destroy = true;
    for_each_link(row_ndx, do_destroy, handler); // Throws

    // Update forward links to the moved target row
    size_t last_row_ndx = prior_num_rows - 1;
    if (row_ndx != last_row_ndx) {
        do_destroy = false;
        for_each_link(last_row_ndx, do_destroy, [=](size_t origin_row_ndx) {
            m_origin_column->do_update_link(origin_row_ndx, last_row_ndx, row_ndx); // Throws
        });
    }

    IntegerColumn::move_last_row_over(row_ndx, prior_num_rows,
                                      broken_reciprocal_backlinks); // Throws
}


void BacklinkColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    std::set<size_t> unique_origin_rows;
    const bool do_destroy = false;
    for_each_link(row_ndx_1, do_destroy, [&](size_t origin_row_ndx) {
        unique_origin_rows.insert(origin_row_ndx);
    });
    for_each_link(row_ndx_2, do_destroy, [&](size_t origin_row_ndx) {
        unique_origin_rows.insert(origin_row_ndx);
    });

    for (auto& origin_row : util::as_const(unique_origin_rows)) {
        m_origin_column->do_swap_link(origin_row, row_ndx_1, row_ndx_2);
    }

    IntegerColumn::swap_rows(row_ndx_1, row_ndx_2);
}


void BacklinkColumn::clear(size_t num_rows, bool)
{
    for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
        // IntegerColumn::clear() handles the destruction of subtrees
        bool do_destroy = false;
        for_each_link(row_ndx, do_destroy, [=](size_t origin_row_ndx) {
            m_origin_column->do_nullify_link(origin_row_ndx, row_ndx);  // Throws
        });
    }

    clear_without_updating_index(); // Throws
    // FIXME: This one is needed because
    // IntegerColumn::clear_without_updating_index() forgets about the leaf
    // type. A better solution should probably be found.
    get_root_array()->set_type(Array::type_HasRefs);
}


void BacklinkColumn::update_child_ref(size_t child_ndx, ref_type new_ref)
{
    IntegerColumn::set(child_ndx, new_ref); // Throws
}


ref_type BacklinkColumn::get_child_ref(size_t child_ndx) const noexcept
{
    return IntegerColumn::get_as_ref(child_ndx);
}

void BacklinkColumn::cascade_break_backlinks_to(size_t row_ndx, CascadeState& state)
{
    if (state.track_link_nullifications) {
        bool do_destroy = false;
        for_each_link(row_ndx, do_destroy, [&](size_t origin_row_ndx) {
            state.links.push_back({m_origin_table.get(), m_origin_column_ndx, origin_row_ndx, row_ndx});
        });
    }
}

void BacklinkColumn::cascade_break_backlinks_to_all_rows(size_t num_rows, CascadeState& state)
{
    if (state.track_link_nullifications) {
        for (size_t row_ndx = 0; row_ndx < num_rows; ++row_ndx) {
            // IntegerColumn::clear() handles the destruction of subtrees
            bool do_destroy = false;
            for_each_link(row_ndx, do_destroy, [&](size_t origin_row_ndx) {
                state.links.push_back({m_origin_table.get(), m_origin_column_ndx, origin_row_ndx, row_ndx});
            });
        }
    }
}


#ifdef REALM_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.verify();
    REALM_ASSERT(leaf.has_refs());
    return leaf.size();
}

} // anonymous namespace

void BacklinkColumn::verify() const
{
    if (root_is_leaf()) {
        get_root_array()->verify();
        REALM_ASSERT(get_root_array()->has_refs());
        return;
    }

    get_root_array()->verify_bptree(&verify_leaf);
}

void BacklinkColumn::verify(const Table& table, size_t col_ndx) const
{
    IntegerColumn::verify(table, col_ndx);

    // Check that the origin column specifies the right target
    REALM_ASSERT(&m_origin_column->get_target_table() == &table);
    REALM_ASSERT(&m_origin_column->get_backlink_column() == this);

    // Check that m_origin_table is the table specified by the spec
    size_t origin_table_ndx = m_origin_table->get_index_in_group();
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT_3(origin_table_ndx, ==, spec.get_opposite_link_table_ndx(col_ndx));
}


void BacklinkColumn::get_backlinks(std::vector<VerifyPair>& pairs)
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


std::pair<ref_type, size_t> BacklinkColumn::get_to_dot_parent(size_t ndx_in_parent) const
{
    std::pair<MemRef, size_t> p = get_root_array()->get_bptree_leaf(ndx_in_parent);
    return std::make_pair(p.first.m_ref, p.second);
}

#endif // REALM_DEBUG
