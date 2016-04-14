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
#ifndef REALM_COLUMN_TIMESTAMP_HPP
#define REALM_COLUMN_TIMESTAMP_HPP

#include <realm/column.hpp>

namespace realm {

struct Timestamp {
    Timestamp(int64_t seconds, uint32_t nanoseconds) : m_seconds(seconds), m_nanoseconds(nanoseconds), m_is_null(false) 
    {
        REALM_ASSERT_3(nanoseconds, <, 1000000000);
    }
    Timestamp(const null&) : m_is_null(true) { }
    Timestamp() : Timestamp(null()) { }

    bool is_null() const { return m_is_null; }
    
    // Note that nullability is handeled by query system. These operators are only invoked for non-null dates.
    bool operator==(const Timestamp& rhs) const { return m_seconds == rhs.m_seconds && m_nanoseconds == rhs.m_nanoseconds; }
    bool operator!=(const Timestamp& rhs) const { return m_seconds != rhs.m_seconds || m_nanoseconds != rhs.m_nanoseconds; }
    bool operator>(const Timestamp& rhs) const { return (m_seconds > rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds > rhs.m_nanoseconds); }
    bool operator<(const Timestamp& rhs) const { return (m_seconds < rhs.m_seconds) || (m_seconds == rhs.m_seconds && m_nanoseconds < rhs.m_nanoseconds); }
    bool operator<=(const Timestamp& rhs) const { return *this < rhs || *this == rhs; }
    bool operator>=(const Timestamp& rhs) const { return *this > rhs || *this == rhs; }
    Timestamp& operator=(const Timestamp& rhs) = default;

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Timestamp&);

    int64_t m_seconds;
    uint32_t m_nanoseconds;
    bool m_is_null;
};

template<class C, class T>
inline std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, const Timestamp& d)
{
    out << "Timestamp(" << d.m_seconds << ", " << d.m_nanoseconds << ")";
    return out;
}

// Inherits from ColumnTemplate to get a compare_values() that can be called without knowing the
// column type
class TimestampColumn : public ColumnBaseSimple, public ColumnTemplate<Timestamp> {
public:
    TimestampColumn(Allocator& alloc, ref_type ref);
    ~TimestampColumn() noexcept override;

    static ref_type create(Allocator& alloc, size_t size = 0);

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

    bool has_search_index() const noexcept final { return bool(m_search_index); }
    StringIndex* get_search_index() noexcept final { return m_search_index.get(); }
    void destroy_search_index() noexcept override;
    void set_search_index_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent,
            bool allow_duplicate_values) final;
    void populate_search_index();
    StringIndex* create_search_index() override;
    
    StringData get_index_data(size_t, StringIndex::StringConversionBuffer& buffer) const noexcept override;
    ref_type write(size_t slice_offset, size_t slice_size, size_t table_size, _impl::OutputStream&) const override;
    void update_from_parent(size_t old_baseline) noexcept override;
    void set_ndx_in_parent(size_t ndx) noexcept override;
    void refresh_accessor_tree(size_t new_col_ndx, const Spec&) override;
#ifdef REALM_DEBUG
    void verify() const override;
    void to_dot(std::ostream&, StringData title = StringData()) const override;
    void do_dump_node_structure(std::ostream&, int level) const override;
    void leaf_to_dot(MemRef, ArrayParent*, size_t ndx_in_parent, std::ostream&) const override;
#endif
    void add(const Timestamp& ts = Timestamp{});
    Timestamp get(size_t row_ndx) const noexcept;
    Timestamp get_val(size_t row_ndx) const noexcept override { return get(row_ndx); }
    void set(size_t row_ndx, const Timestamp& ts);
    bool compare(const TimestampColumn& c) const noexcept;

    Timestamp maximum(size_t, size_t, size_t, size_t*) const { return Timestamp(); }
    size_t count(Timestamp) const;
    void erase(size_t row_ndx, bool is_last);

    template <class Condition> size_t find(Timestamp value, size_t begin, size_t end) const noexcept
    {
        // FIXME: Here we can do all sorts of clever optimizations. Use bithack-search on seconds, then for each match check
        // nanoseconds, etc, etc, etc. Lots of possibilities. Below code is naive and slow but works.

        Condition cond;
        for (size_t t = begin; t < end; t++) {
            Timestamp ts = get(t);
            if (cond(ts, value, ts.is_null(), value.is_null()))
                return t;
        }
        return npos;
    }

    typedef Timestamp value_type;

private:
    std::unique_ptr<BpTree<util::Optional<int64_t>>> m_seconds;
    std::unique_ptr<BpTree<int64_t>> m_nanoseconds;

    std::unique_ptr<StringIndex> m_search_index;

    template<class BT>
    class CreateHandler;
};

} // namespace realm

#endif // REALM_COLUMN_TIMESTAMP_HPP
