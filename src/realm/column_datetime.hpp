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
    NewDate(int64_t seconds, int32_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds), m_is_null(false) { }
    NewDate(const null&) : m_is_null(true) { }
    NewDate() : NewDate(null()) { }

    bool is_null() const { return m_is_null; }
    bool operator == (const NewDate& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator != (const NewDate& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator > (const NewDate& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds > rhs.m_nanoseconds); }
    bool operator < (const NewDate& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_seconds < rhs.m_nanoseconds); }
    NewDate& operator = (const NewDate& rhs) = default;

    int64_t m_seconds;
    int32_t m_nanoseconds;
    bool m_is_null;
};

class DateTimeColumn : public ColumnBase {
public:
    /// Get the number of entries in this column. This operation is relatively
    /// slow.
    size_t size() const noexcept override {
        // FIXME: Consider debug asserts on the columns having the same size
        return m_seconds.size();
    }

    /// Whether or not this column is nullable.
    bool is_nullable() const noexcept override {
        return m_seconds.is_nullable();
    }

    /// Whether or not the value at \a row_ndx is NULL. If the column is not
    /// nullable, always returns false.
    bool is_null(size_t row_ndx) const noexcept override {
        return m_seconds.is_null(row_ndx);
    }

    /// Sets the value at \a row_ndx to be NULL.
    /// \throw LogicError Thrown if this column is not nullable.
    void set_null(size_t row_ndx) override {
        m_seconds.set_null(row_ndx);
    }

    void insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t prior_num_rows, bool nullable) override {
        m_seconds.insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, nullable);
        m_nanoseconds.insert_rows(row_ndx, num_rows_to_insert, prior_num_rows, nullable);
    }
    
    void erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                    bool broken_reciprocal_backlinks) override {
        m_seconds.erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks);
        m_nanoseconds.erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks);
    }

    void move_last_row_over(size_t row_ndx, size_t prior_num_rows,
                            bool broken_reciprocal_backlinks) override {
        m_seconds.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks);
        m_nanoseconds.move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks);
    }

    void clear(size_t num_rows, bool broken_reciprocal_backlinks) override {
        m_seconds.clear(num_rows, broken_reciprocal_backlinks);
        m_nanoseconds.clear(num_rows, broken_reciprocal_backlinks);
    }

    void swap_rows(size_t row_ndx_1, size_t row_ndx_2) override {
        m_seconds.swap_rows(row_ndx_1, row_ndx_2);
        m_nanoseconds.swap_rows(row_ndx_1, row_ndx_2);
    }

    void destroy() noexcept override {
        m_seconds.destroy();
        m_nanoseconds.destroy();
    }

    StringData get_index_data(size_t, StringIndex::StringConversionBuffer& buffer) const noexcept override {
        // FIXME: Dummy implementation
        return null();
    }

    Allocator& get_alloc() const noexcept override {
        // FIXME: Dummy implementation
        return Allocator::get_default();
    }

    /// Returns the 'ref' of the root array.
    ref_type get_ref() const noexcept override {
        // FIXME: Dummy implementation
        return 0;
    }
    MemRef get_mem() const noexcept override {
        // FIXME: Dummy implementation
        return MemRef();
    }

    void replace_root_array(std::unique_ptr<Array> leaf) override {
        // FIXME: Dummy implementation

    }

    MemRef clone_deep(Allocator& alloc) const override {
        // FIXME: Dummy implementation
        return MemRef();

    }

    void detach() override {
        m_seconds.detach();
        m_nanoseconds.detach();
    }

    bool is_attached() const noexcept override {
        // FIXME: Assert on both columns having same attached state?
        return m_seconds.is_attached();
    }

    ref_type write(size_t slice_offset, size_t slice_size,
                   size_t table_size, _impl::OutputStream&) const override {
        // FIXME: Dummy implementation
        return 0;
    }

    void set_parent(ArrayParent*, size_t ndx_in_parent) noexcept override {
        // FIXME: Dummy implementation

    }

    size_t get_ndx_in_parent() const noexcept override {
        // FIXME: Dummy implementation
        return 0;
    }

    void set_ndx_in_parent(size_t ndx_in_parent) noexcept override {
        // FIXME: Dummy implementation
    }

    void update_from_parent(size_t old_baseline) noexcept override {
        // FIXME: Dummy implementation
    }

    void refresh_accessor_tree(size_t new_col_ndx, const Spec&) override {
        // FIXME: Dummy implementation
    }

    void verify() const override {
        // FIXME: Dummy implementation
    }

    void to_dot(std::ostream&, StringData title = StringData()) const override {
        // FIXME: Dummy implementation
    }

    void do_dump_node_structure(std::ostream&, int level) const override {
        // FIXME: Dummy implementation
    }

    void leaf_to_dot(MemRef, ArrayParent*, size_t ndx_in_parent,
                     std::ostream&) const override {
        // FIXME: Dummy implementation
    }

    void add(const NewDate& ndt = NewDate{}) {
        util::Optional<int64_t> seconds = ndt.is_null() ? util::none : util::make_optional(ndt.m_seconds);
        int32_t nanoseconds = ndt.is_null() ? 0 : ndt.m_nanoseconds;
        m_seconds.add(seconds);
        m_nanoseconds.add(nanoseconds);
    }

    NewDate get(size_t row_ndx) const noexcept {
        util::Optional<int64_t> seconds = m_seconds.get(row_ndx);
        return seconds ? NewDate(*seconds, m_nanoseconds.get(row_ndx)) : NewDate();
    }

private:
    IntNullColumn m_seconds;
    IntegerColumn m_nanoseconds;
};

} // namespace realm

#endif // REALM_COLUMN_DATETIME_HPP
