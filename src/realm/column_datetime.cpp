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

    ref_type seconds = IntNullColumn::create(alloc, Array::type_Normal, size);
    ref_type nano = IntegerColumn::create(alloc, Array::type_Normal, size);

    top.set_as_ref(0, seconds);
    top.set_as_ref(1, nano);

    ref_type top_ref = top.get_ref();
    return top_ref;
}


/// Get the number of entries in this column. This operation is relatively
/// slow.
inline size_t DateTimeColumn::size() const noexcept
{
    // FIXME: Consider debug asserts on the columns having the same size
    return m_seconds.size();
}

/// Whether or not this column is nullable.
inline bool DateTimeColumn::is_nullable() const noexcept
{
    return m_seconds.is_nullable();
}

/// Whether or not the value at \a row_ndx is NULL. If the column is not
/// nullable, always returns false.
inline bool DateTimeColumn::is_null(size_t row_ndx) const noexcept
{
    return m_seconds.is_null(row_ndx);
}

/// Sets the value at \a row_ndx to be NULL.
/// \throw LogicError Thrown if this column is not nullable.
inline void DateTimeColumn::set_null(size_t row_ndx)
{
    m_seconds.set_null(row_ndx);
}

inline void DateTimeColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows,
    bool nullable)
{
    m_seconds.insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, nullable);
    m_nanoseconds.insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, false);
}

inline void DateTimeColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
    bool broken_reciprocal_backlinks)
{
    m_seconds.erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks);
    m_nanoseconds.erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks);
}

inline void DateTimeColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows,
    bool broken_reciprocal_backlinks)
{
    m_seconds.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks);
    m_nanoseconds.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks);
}

inline void DateTimeColumn::clear(size_t num_rows, bool broken_reciprocal_backlinks)
{
    m_seconds.clear(num_rows, broken_reciprocal_backlinks);
    m_nanoseconds.clear(num_rows, broken_reciprocal_backlinks);
}

inline void DateTimeColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    m_seconds.swap_rows(row_ndx_1, row_ndx_2);
    m_nanoseconds.swap_rows(row_ndx_1, row_ndx_2);
}

inline void DateTimeColumn::destroy() noexcept
{
    m_seconds.destroy();
    m_nanoseconds.destroy();
}

inline StringData DateTimeColumn::get_index_data(size_t, StringIndex::StringConversionBuffer& /*buffer*/) const noexcept
{
    // FIXME: Dummy implementation
    return null();
}

inline MemRef DateTimeColumn::clone_deep(Allocator& /*alloc*/) const
{
    // FIXME: Dummy implementation
    return MemRef();
}


inline ref_type DateTimeColumn::write(size_t /*slice_offset*/, size_t /*slice_size*/, size_t /*table_size*/,
    _impl::OutputStream&) const
{
    // FIXME: Dummy implementation
    return 0;
}

inline void DateTimeColumn::update_from_parent(size_t old_baseline) noexcept
{
    m_seconds.update_from_parent(old_baseline);
    m_nanoseconds.update_from_parent(old_baseline);
}

inline void DateTimeColumn::refresh_accessor_tree(size_t /*new_col_ndx*/, const Spec&)
{
    // FIXME: Dummy implementation
}

#ifdef REALM_DEBUG

inline void DateTimeColumn::verify() const
{
    // FIXME: Dummy implementation
}

inline void DateTimeColumn::to_dot(std::ostream&, StringData /*title*/) const
{
    // FIXME: Dummy implementation
}

inline void DateTimeColumn::do_dump_node_structure(std::ostream&, int /*level*/) const
{
    // FIXME: Dummy implementation
}

inline void DateTimeColumn::leaf_to_dot(MemRef, ArrayParent*, size_t /*ndx_in_parent*/, std::ostream&) const
{
    // FIXME: Dummy implementation
}

#endif

void DateTimeColumn::add(const NewDate& ndt)
{
    bool is_null = ndt.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ndt.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ndt.m_nanoseconds;
    m_seconds.add(seconds);
    m_nanoseconds.add(nanoseconds);
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
