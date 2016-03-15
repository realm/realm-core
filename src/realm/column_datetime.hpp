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
#ifndef REALM_COLUMN_DATETIME_HPP
#define REALM_COLUMN_DATETIME_HPP

#include <realm/column.hpp>

namespace realm {

struct NewDate {
    NewDate(int64_t seconds, uint32_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds), m_is_null(false) 
    {
        REALM_ASSERT_3(nanoseconds, <, 1000000000);
    }
    NewDate(const null&) : m_is_null(true) { }
    NewDate() : NewDate(null()) { }

    bool is_null() const { return m_is_null; }
    bool operator == (const NewDate& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator != (const NewDate& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator > (const NewDate& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds > rhs.m_nanoseconds); }
    bool operator < (const NewDate& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds < rhs.m_nanoseconds); }
    bool operator <= (const NewDate& rhs) const { return *this < rhs || *this == rhs; }
    bool operator >= (const NewDate& rhs) const { return *this > rhs || *this == rhs; }
    NewDate& operator = (const NewDate& rhs) = default;

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const DateTime&);

    int64_t m_seconds;
    uint32_t m_nanoseconds;
    bool m_is_null;
};

template<class C, class T>
inline std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, const NewDate& d)
{
    out << "NewDate(" << d.m_seconds << ", " << d.m_nanoseconds << ")";
    return out;
}

class DateTimeColumn : public ColumnBase, public ColumnTemplate<NewDate> {
public:
    DateTimeColumn();
    /// Get the number of entries in this column. This operation is relatively
    /// slow.
    size_t size() const noexcept override;
    /// Whether or not this column is nullable.
    bool is_nullable() const noexcept override;
    /// Whether or not the value at \a row_ndx is NULL. If the column is not
    /// nullable, always returns false.
    bool is_null(size_t row_ndx) const noexcept override;
    /// Sets the value at \a row_ndx to be NULL.
    /// \throw LogicError Thrown if this column is not nullable.
    void set_null(size_t row_ndx) override;
    void insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool nullable) override;
    void erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                    bool broken_reciprocal_backlinks) override;
    void move_last_row_over(size_t row_ndx, size_t prior_num_rows,
                            bool broken_reciprocal_backlinks) override;
    void clear(size_t num_rows, bool broken_reciprocal_backlinks) override;
    void swap_rows(size_t row_ndx_1, size_t row_ndx_2) override;
    void destroy() noexcept override;
    StringData get_index_data(size_t, StringIndex::StringConversionBuffer& buffer) const noexcept override;
    Allocator& get_alloc() const noexcept override;
    /// Returns the 'ref' of the root array.
    ref_type get_ref() const noexcept override;
    MemRef get_mem() const noexcept override;
    void replace_root_array(std::unique_ptr<Array> leaf) override;
    MemRef clone_deep(Allocator& alloc) const override;
    void detach() override;
    bool is_attached() const noexcept override;
    ref_type write(size_t slice_offset, size_t slice_size, size_t table_size, _impl::OutputStream&) const override;
    void set_parent(ArrayParent*, size_t ndx_in_parent) noexcept override;
    size_t get_ndx_in_parent() const noexcept override;
    void set_ndx_in_parent(size_t ndx_in_parent) noexcept override;
    void update_from_parent(size_t old_baseline) noexcept override;
    void refresh_accessor_tree(size_t new_col_ndx, const Spec&) override;
#ifdef REALM_DEBUG
    void verify() const override;
    void to_dot(std::ostream&, StringData title = StringData()) const override;
    void do_dump_node_structure(std::ostream&, int level) const override;
    void leaf_to_dot(MemRef, ArrayParent*, size_t ndx_in_parent, std::ostream&) const override;
#endif
    void add(const NewDate& ndt = NewDate{});
    NewDate get(size_t row_ndx) const noexcept;
    NewDate get_val(size_t row_ndx) const override;
    void set(size_t row_ndx, const NewDate& ndt);
private:
    IntNullColumn m_seconds;
    IntegerColumn m_nanoseconds;
};


// Implementation

inline DateTimeColumn::DateTimeColumn() :
    m_seconds(Allocator::get_default(), IntNullColumn::create(Allocator::get_default())),
    m_nanoseconds(Allocator::get_default(), IntegerColumn::create(Allocator::get_default()))
{
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

inline Allocator& DateTimeColumn::get_alloc() const noexcept
{
    // FIXME: Dummy implementation
    return Allocator::get_default();
}

inline ref_type DateTimeColumn::get_ref() const noexcept
{
    // FIXME: Dummy implementation
    return 0;
}

inline MemRef DateTimeColumn::get_mem() const noexcept
{
    // FIXME: Dummy implementation
    return MemRef();
}

inline void DateTimeColumn::replace_root_array(std::unique_ptr<Array> /*leaf*/)
{
    // FIXME: Dummy implementation
}

inline MemRef DateTimeColumn::clone_deep(Allocator& /*alloc*/) const
{
    // FIXME: Dummy implementation
    return MemRef();
}

inline void DateTimeColumn::detach()
{
    m_seconds.detach();
    m_nanoseconds.detach();
}

inline bool DateTimeColumn::is_attached() const noexcept
{
    // FIXME: Assert on both columns having same attached state?
    return m_seconds.is_attached();
}

inline ref_type DateTimeColumn::write(size_t /*slice_offset*/, size_t /*slice_size*/, size_t /*table_size*/,
                                      _impl::OutputStream&) const
{
    // FIXME: Dummy implementation
    return 0;
}

inline void DateTimeColumn::set_parent(ArrayParent*, size_t /*ndx_in_parent*/) noexcept
{
    // FIXME: Dummy implementation
}

inline size_t DateTimeColumn::get_ndx_in_parent() const noexcept
{
    // FIXME: Dummy implementation
    return 0;
}

inline void DateTimeColumn::set_ndx_in_parent(size_t /*ndx_in_parent*/) noexcept
{
    // FIXME: Dummy implementation
}

inline void DateTimeColumn::update_from_parent(size_t /*old_baseline*/) noexcept
{
    // FIXME: Dummy implementation
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

inline void DateTimeColumn::add(const NewDate& ndt)
{
    bool is_null = ndt.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ndt.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ndt.m_nanoseconds;
    m_seconds.add(seconds);
    m_nanoseconds.add(nanoseconds);
}

inline NewDate DateTimeColumn::get(size_t row_ndx) const noexcept
{
    util::Optional<int64_t> seconds = m_seconds.get(row_ndx);
    return seconds ? NewDate(*seconds, int32_t(m_nanoseconds.get(row_ndx))) : NewDate(null());
}

inline NewDate DateTimeColumn::get_val(size_t row_ndx) const
{
    return get(row_ndx);
}

inline void DateTimeColumn::set(size_t row_ndx, const NewDate& ndt)
{
    bool is_null = ndt.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ndt.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ndt.m_nanoseconds;
    m_seconds.set(row_ndx, seconds);
    m_nanoseconds.set(row_ndx, nanoseconds);
}

} // namespace realm

#endif // REALM_COLUMN_DATETIME_HPP
