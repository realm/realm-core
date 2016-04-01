/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
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

#include <realm/column_datetime.hpp>
#include <realm/index_string.hpp>

namespace realm {


DateTimeColumn::DateTimeColumn(Allocator& alloc, ref_type ref)
{
    char* header = alloc.translate(ref);
    MemRef mem(header, ref);

    Array* root = new Array(alloc); // Throws
    root->init_from_mem(mem);
    m_array.reset(root);

    ref_type seconds = m_array->get_as_ref(0);
    ref_type nano = m_array->get_as_ref(1);

    m_seconds.init_from_ref(alloc, seconds);
    m_nanoseconds.init_from_ref(alloc, nano);

    m_seconds.set_parent(root, 0);
    m_nanoseconds.set_parent(root, 1);
}


DateTimeColumn::~DateTimeColumn() noexcept
{

}


ref_type DateTimeColumn::create(Allocator& alloc, size_t size)
{
    Array top(alloc);
    top.create(Array::type_HasRefs, false /* context_flag */, 2);

    MemRef seconds = BpTree<util::Optional<int64_t>>::create_leaf(Array::type_Normal, size, null{}, alloc);
    MemRef nano = BpTree<int64_t>::create_leaf(Array::type_Normal, size, 0, alloc);

    top.set_as_ref(0, seconds.m_ref);
    top.set_as_ref(1, nano.m_ref);

    ref_type top_ref = top.get_ref();
    return top_ref;
}


/// Get the number of entries in this column. This operation is relatively
/// slow.
size_t DateTimeColumn::size() const noexcept
{
    // FIXME: Consider debug asserts on the columns having the same size
    return m_seconds.size();
}

/// Whether or not this column is nullable.
bool DateTimeColumn::is_nullable() const noexcept
{
    return true;
}

/// Whether or not the value at \a row_ndx is NULL. If the column is not
/// nullable, always returns false.
bool DateTimeColumn::is_null(size_t row_ndx) const noexcept
{
    return m_seconds.is_null(row_ndx);
}

/// Sets the value at \a row_ndx to be NULL.
/// \throw LogicError Thrown if this column is not nullable.
void DateTimeColumn::set_null(size_t row_ndx)
{
    m_seconds.set_null(row_ndx);
    if (has_search_index()) {
        m_search_index->set(row_ndx, null{});
    }
}

void DateTimeColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows,
    bool nullable)
{
    static_cast<void>(prior_num_rows);
    if (row_ndx == size())
        row_ndx = npos;
    if (nullable)
        m_seconds.insert(row_ndx, null{}, num_rows_to_insert);
    else
        m_seconds.insert(row_ndx, 0, num_rows_to_insert);
    m_nanoseconds.insert(row_ndx, 0, num_rows_to_insert);

    if (has_search_index()) {
        size_t size = this->size();
        bool is_append = row_ndx == npos || row_ndx == size;
        if (is_append)
            row_ndx = size;
        if (nullable) {
            m_search_index->insert(row_ndx, null{}, num_rows_to_insert, is_append);
        }
        else {
            m_search_index->insert(row_ndx, NewDate{0, 0}, num_rows_to_insert, is_append);
        }
    }
}

void DateTimeColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
    bool broken_reciprocal_backlinks)
{
    static_cast<void>(broken_reciprocal_backlinks);
    bool is_last = (row_ndx + num_rows_to_erase) == size();
    for (size_t i = 0; i < num_rows_to_erase; ++i) {
        m_seconds.erase(row_ndx + num_rows_to_erase - i - 1, is_last);
        m_nanoseconds.erase(row_ndx + num_rows_to_erase - i - 1, is_last);
        
        if (has_search_index()) {
            m_search_index->erase<StringData>(row_ndx + num_rows_to_erase - i - 1, is_last);
        }
    }
}

void DateTimeColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows,
    bool broken_reciprocal_backlinks)
{
    static_cast<void>(broken_reciprocal_backlinks);
    
    size_t last_row_ndx = prior_num_rows - 1;
    
    if (has_search_index()) {
        // remove the value to be overwritten from index
        bool is_last = true; // This tells StringIndex::erase() to not adjust subsequent indexes
        m_search_index->erase<StringData>(row_ndx, is_last); // Throws

        // update index to point to new location
        if (row_ndx != last_row_ndx) {
            auto moved_value = get(last_row_ndx);
            m_search_index->update_ref(moved_value, last_row_ndx, row_ndx); // Throws
        }
    }

    m_seconds.move_last_over(row_ndx, prior_num_rows);
    m_nanoseconds.move_last_over(row_ndx, prior_num_rows);
}

void DateTimeColumn::clear(size_t num_rows, bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_EX(num_rows == m_seconds.size(), num_rows, m_seconds.size());
    static_cast<void>(broken_reciprocal_backlinks);
    m_seconds.clear();
    m_nanoseconds.clear();
    if (has_search_index()) {
        m_search_index->clear();
    }
}

void DateTimeColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    if (has_search_index()) {
        auto value_1 = get(row_ndx_1);
        auto value_2 = get(row_ndx_2);
        size_t size = this->size();
        bool row_ndx_1_is_last = row_ndx_1 == size - 1;
        bool row_ndx_2_is_last = row_ndx_2 == size - 1;
        m_search_index->erase<StringData>(row_ndx_1, row_ndx_1_is_last);
        m_search_index->insert(row_ndx_1, value_2, 1, row_ndx_1_is_last);
        m_search_index->erase<StringData>(row_ndx_2, row_ndx_2_is_last);
        m_search_index->insert(row_ndx_2, value_1, 1, row_ndx_2_is_last);
    }

    auto tmp1 = m_seconds.get(row_ndx_1);
    m_seconds.set(row_ndx_1, m_seconds.get(row_ndx_2));
    m_seconds.set(row_ndx_2, tmp1);
    auto tmp2 = m_nanoseconds.get(row_ndx_1);
    m_nanoseconds.set(row_ndx_1, m_nanoseconds.get(row_ndx_2));
    m_nanoseconds.set(row_ndx_2, tmp2);
}

void DateTimeColumn::destroy() noexcept
{
    m_seconds.destroy();
    m_nanoseconds.destroy();
    if (m_array)
        m_array->destroy();
}

StringData DateTimeColumn::get_index_data(size_t ndx, StringIndex::StringConversionBuffer& buffer) const noexcept
{
    return GetIndexData<NewDate>::get_index_data(get(ndx), buffer);
}

void DateTimeColumn::populate_search_index()
{
    REALM_ASSERT(has_search_index());
    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        bool is_append = true;
        auto value = get(row_ndx);
        m_search_index->insert(row_ndx, value, 1, is_append); // Throws
    }
}

StringIndex* DateTimeColumn::create_search_index()
{
    REALM_ASSERT(!has_search_index());
    m_search_index.reset(new StringIndex(this, get_alloc())); // Throws
    populate_search_index();
    return m_search_index.get();
}

void DateTimeColumn::destroy_search_index() noexcept
{
    m_search_index.reset();
}

void DateTimeColumn::set_search_index_ref(ref_type ref, ArrayParent* parent,
        size_t ndx_in_parent, bool allow_duplicate_values)
{
    REALM_ASSERT(!m_search_index);
    m_search_index.reset(new StringIndex(ref, parent, ndx_in_parent, this,
                !allow_duplicate_values, get_alloc())); // Throws
}



MemRef DateTimeColumn::clone_deep(Allocator& /*alloc*/) const
{
    // FIXME: Dummy implementation
    return MemRef();
}


ref_type DateTimeColumn::write(size_t /*slice_offset*/, size_t /*slice_size*/, size_t /*table_size*/,
    _impl::OutputStream&) const
{
    // FIXME: Dummy implementation
    return 0;
}

void DateTimeColumn::set_ndx_in_parent(size_t ndx) noexcept
{
    m_array->set_ndx_in_parent(ndx);
    if (has_search_index()) {
        m_search_index->set_ndx_in_parent(ndx + 1);
    }
}

void DateTimeColumn::update_from_parent(size_t old_baseline) noexcept
{
    ColumnBaseSimple::update_from_parent(old_baseline);
    if (has_search_index()) {
        m_search_index->update_from_parent(old_baseline);
    }
}

void DateTimeColumn::refresh_accessor_tree(size_t new_col_ndx, const Spec& spec)
{
    // FIXME: Dummy implementation
    
    if (has_search_index()) {
        m_search_index->refresh_accessor_tree(new_col_ndx, spec);
    }
}

#ifdef REALM_DEBUG

void DateTimeColumn::verify() const
{
    // FIXME: Dummy implementation
}

void DateTimeColumn::to_dot(std::ostream&, StringData /*title*/) const
{
    // FIXME: Dummy implementation
}

void DateTimeColumn::do_dump_node_structure(std::ostream&, int /*level*/) const
{
    // FIXME: Dummy implementation
}

void DateTimeColumn::leaf_to_dot(MemRef, ArrayParent*, size_t /*ndx_in_parent*/, std::ostream&) const
{
    // FIXME: Dummy implementation
}

#endif

void DateTimeColumn::add(const NewDate& ndt)
{
    bool is_null = ndt.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ndt.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ndt.m_nanoseconds;
    m_seconds.insert(npos, seconds);
    m_nanoseconds.insert(npos, nanoseconds);

    if (has_search_index()) {
        size_t ndx = size() - 1; // Slow
        m_search_index->insert(ndx, ndt, 1, true);
    }
}

NewDate DateTimeColumn::get(size_t row_ndx) const noexcept
{
    util::Optional<int64_t> seconds = m_seconds.get(row_ndx);
    return seconds ? NewDate(*seconds, int32_t(m_nanoseconds.get(row_ndx))) : NewDate(null());
}

void DateTimeColumn::set(size_t row_ndx, const NewDate& ndt)
{
    bool is_null = ndt.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ndt.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ndt.m_nanoseconds;
    m_seconds.set(row_ndx, seconds);
    m_nanoseconds.set(row_ndx, nanoseconds);

    if (has_search_index()) {
        m_search_index->set(row_ndx, ndt);
    }
}

bool DateTimeColumn::compare(const DateTimeColumn& c) const noexcept
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i < n; ++i) {
        bool left_is_null = is_null(i);
        bool right_is_null = c.is_null(i);
        if (left_is_null != right_is_null) {
            return false;
        }
        if (!left_is_null) {
            if (get(i) != c.get(i))
                return false;
        }
    }
    return true;
}


}
