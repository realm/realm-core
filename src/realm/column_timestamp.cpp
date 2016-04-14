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

#include <realm/column_timestamp.hpp>
#include <realm/index_string.hpp>

namespace realm {


TimestampColumn::TimestampColumn(Allocator& alloc, ref_type ref)
{
    std::unique_ptr<Array> top;
    std::unique_ptr<BpTree<util::Optional<int64_t>>> seconds;
    std::unique_ptr<BpTree<int64_t>> nanoseconds;

    top.reset(new Array(alloc)); // Throws
    top->init_from_ref(ref);

    ref_type seconds_ref = top->get_as_ref(0);
    ref_type nanoseconds_ref = top->get_as_ref(1);

    seconds.reset(new BpTree<util::Optional<int64_t>>(alloc)); // Throws
    seconds->init_from_ref(alloc, seconds_ref);
    seconds->set_parent(&*top, 0);

    nanoseconds.reset(new BpTree<int64_t>(alloc)); // Throws
    nanoseconds->init_from_ref(alloc, nanoseconds_ref);
    nanoseconds->set_parent(&*top, 1);

    m_array = std::move(top);
    m_seconds = std::move(seconds);
    m_nanoseconds = std::move(nanoseconds);
}


TimestampColumn::~TimestampColumn() noexcept
{

}

template<class BT>
class TimestampColumn::CreateHandler: public ColumnBase::CreateHandler {
public:
    CreateHandler(typename BT::value_type value, Allocator& alloc):
    m_value(value), m_alloc(alloc) {}

    ref_type create_leaf(size_t size) override
    {
        MemRef mem = BT::create_leaf(Array::type_Normal, size, m_value, m_alloc); // Throws
        return mem.m_ref;
    }
private:
    const typename BT::value_type m_value;
    Allocator& m_alloc;
};


ref_type TimestampColumn::create(Allocator& alloc, size_t size)
{
    Array top(alloc);
    top.create(Array::type_HasRefs, false /* context_flag */, 2);

    CreateHandler<BpTree<util::Optional<int64_t>>> create_handler{null{}, alloc};
    ref_type seconds_ref = ColumnBase::create(alloc, size, create_handler);

    CreateHandler<BpTree<int64_t>> nano_create_handler{0, alloc};
    ref_type nanoseconds_ref = ColumnBase::create(alloc, size, nano_create_handler);

    top.set_as_ref(0, seconds_ref);
    top.set_as_ref(1, nanoseconds_ref);

    return top.get_ref();
}


/// Get the number of entries in this column. This operation is relatively
/// slow.
size_t TimestampColumn::size() const noexcept
{
    // FIXME: Consider debug asserts on the columns having the same size
    return m_seconds->size();
}

/// Whether or not this column is nullable.
bool TimestampColumn::is_nullable() const noexcept
{
    return true;
}

/// Whether or not the value at \a row_ndx is NULL. If the column is not
/// nullable, always returns false.
bool TimestampColumn::is_null(size_t row_ndx) const noexcept
{
    return m_seconds->is_null(row_ndx);
}

/// Sets the value at \a row_ndx to be NULL.
/// \throw LogicError Thrown if this column is not nullable.
void TimestampColumn::set_null(size_t row_ndx)
{
    if (has_search_index()) {
        m_search_index->set(row_ndx, null{});
    }
    m_seconds->set_null(row_ndx);
}

void TimestampColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t /*prior_num_rows*/,
    bool nullable)
{
    bool is_append = row_ndx == size();
    size_t row_ndx_or_npos = is_append ? realm::npos : row_ndx;

    if (nullable)
        m_seconds->insert(row_ndx_or_npos, null{}, num_rows_to_insert);
    else
        m_seconds->insert(row_ndx_or_npos, 0, num_rows_to_insert);
    m_nanoseconds->insert(row_ndx_or_npos, 0, num_rows_to_insert);

    if (has_search_index()) {
        if (nullable) {
            m_search_index->insert(row_ndx, null{}, num_rows_to_insert, is_append);
        }
        else {
            m_search_index->insert(row_ndx, Timestamp{0, 0}, num_rows_to_insert, is_append);
        }
    }
}

void TimestampColumn::erase(size_t row_ndx, bool is_last)
{
    if (has_search_index()) {
        m_search_index->erase<StringData>(row_ndx, is_last);
    }
    m_seconds->erase(row_ndx, is_last);
    m_nanoseconds->erase(row_ndx, is_last);
}

void TimestampColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t /*prior_num_rows*/,
    bool /*broken_reciprocal_backlinks*/)
{
    bool is_last = (row_ndx + num_rows_to_erase) == size();
    for (size_t i = 0; i < num_rows_to_erase; ++i) {
        if (has_search_index()) {
            m_search_index->erase<StringData>(row_ndx + num_rows_to_erase - i - 1, is_last);
        }

        m_seconds->erase(row_ndx + num_rows_to_erase - i - 1, is_last);
        m_nanoseconds->erase(row_ndx + num_rows_to_erase - i - 1, is_last);
    }
}

void TimestampColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows,
    bool /*broken_reciprocal_backlinks*/)
{
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

    m_seconds->move_last_over(row_ndx, prior_num_rows);
    m_nanoseconds->move_last_over(row_ndx, prior_num_rows);
}

void TimestampColumn::clear(size_t num_rows, bool /*broken_reciprocal_backlinks*/)
{
    REALM_ASSERT_EX(num_rows == m_seconds->size(), num_rows, m_seconds->size());
    static_cast<void>(num_rows);
    m_seconds->clear();
    m_nanoseconds->clear();
    if (has_search_index()) {
        m_search_index->clear();
    }
}

void TimestampColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
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

    auto tmp1 = m_seconds->get(row_ndx_1);
    m_seconds->set(row_ndx_1, m_seconds->get(row_ndx_2));
    m_seconds->set(row_ndx_2, tmp1);
    auto tmp2 = m_nanoseconds->get(row_ndx_1);
    m_nanoseconds->set(row_ndx_1, m_nanoseconds->get(row_ndx_2));
    m_nanoseconds->set(row_ndx_2, tmp2);
}

void TimestampColumn::destroy() noexcept
{
    m_seconds->destroy();
    m_nanoseconds->destroy();
    if (m_array)
        m_array->destroy();
}

StringData TimestampColumn::get_index_data(size_t ndx, StringIndex::StringConversionBuffer& buffer) const noexcept
{
    return GetIndexData<Timestamp>::get_index_data(get(ndx), buffer);
}

void TimestampColumn::populate_search_index()
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

StringIndex* TimestampColumn::create_search_index()
{
    REALM_ASSERT(!has_search_index());
    m_search_index.reset(new StringIndex(this, get_alloc())); // Throws
    populate_search_index();
    return m_search_index.get();
}

void TimestampColumn::destroy_search_index() noexcept
{
    m_search_index.reset();
}

void TimestampColumn::set_search_index_ref(ref_type ref, ArrayParent* parent,
        size_t ndx_in_parent, bool allow_duplicate_values)
{
    REALM_ASSERT(!m_search_index);
    m_search_index.reset(new StringIndex(ref, parent, ndx_in_parent, this,
                !allow_duplicate_values, get_alloc())); // Throws
}


ref_type TimestampColumn::write(size_t /*slice_offset*/, size_t /*slice_size*/, size_t /*table_size*/,
    _impl::OutputStream&) const
{
    // FIXME: Dummy implementation
    return 0;
}

void TimestampColumn::set_ndx_in_parent(size_t ndx) noexcept
{
    m_array->set_ndx_in_parent(ndx);
    if (has_search_index()) {
        m_search_index->set_ndx_in_parent(ndx + 1);
    }
}

void TimestampColumn::update_from_parent(size_t old_baseline) noexcept
{
    m_array->update_from_parent(old_baseline);

    m_seconds->update_from_parent(old_baseline);
    m_nanoseconds->update_from_parent(old_baseline);
    if (has_search_index()) {
        m_search_index->update_from_parent(old_baseline);
    }
}

void TimestampColumn::refresh_accessor_tree(size_t new_col_ndx, const Spec& spec)
{
    m_array->init_from_parent();

    m_seconds->init_from_parent();
    m_nanoseconds->init_from_parent();

    if (has_search_index()) {
        m_search_index->refresh_accessor_tree(new_col_ndx, spec);
    }
}

#ifdef REALM_DEBUG

void TimestampColumn::verify() const
{
    // FIXME: Dummy implementation
}

void TimestampColumn::to_dot(std::ostream&, StringData /*title*/) const
{
    // FIXME: Dummy implementation
}

void TimestampColumn::do_dump_node_structure(std::ostream&, int /*level*/) const
{
    // FIXME: Dummy implementation
}

void TimestampColumn::leaf_to_dot(MemRef, ArrayParent*, size_t /*ndx_in_parent*/, std::ostream&) const
{
    // FIXME: Dummy implementation
}

#endif

void TimestampColumn::add(const Timestamp& ts)
{
    bool is_null = ts.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ts.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ts.m_nanoseconds;
    m_seconds->insert(npos, seconds);
    m_nanoseconds->insert(npos, nanoseconds);

    if (has_search_index()) {
        size_t ndx = size() - 1; // Slow
        m_search_index->insert(ndx, ts, 1, true);
    }
}

Timestamp TimestampColumn::get(size_t row_ndx) const noexcept
{
    util::Optional<int64_t> seconds = m_seconds->get(row_ndx);
    return seconds ? Timestamp(*seconds, int32_t(m_nanoseconds->get(row_ndx))) : Timestamp(null());
}

void TimestampColumn::set(size_t row_ndx, const Timestamp& ts)
{
    bool is_null = ts.is_null();
    util::Optional<int64_t> seconds = is_null ? util::none : util::make_optional(ts.m_seconds);
    int32_t nanoseconds = is_null ? 0 : ts.m_nanoseconds;

    if (has_search_index()) {
        m_search_index->set(row_ndx, ts);
    }

    m_seconds->set(row_ndx, seconds);
    m_nanoseconds->set(row_ndx, nanoseconds);
}

bool TimestampColumn::compare(const TimestampColumn& c) const noexcept
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
