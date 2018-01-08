/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/column_timestamp.hpp>
#include <realm/index_string.hpp>

namespace realm {


TimestampColumn::TimestampColumn(bool nullable, Allocator& alloc, ref_type ref, size_t col_ndx)
    : ColumnBaseSimple(col_ndx)
{
    std::unique_ptr<Array> top;
    std::unique_ptr<BpTree<util::Optional<int64_t>>> seconds;
    std::unique_ptr<BpTree<int64_t>> nanoseconds;

    top.reset(new Array(alloc)); // Throws
    top->init_from_ref(ref);

    ref_type seconds_ref = top->get_as_ref(0);
    ref_type nanoseconds_ref = top->get_as_ref(1);

    seconds.reset(new BpTree<util::Optional<int64_t>>(BpTreeBase::unattached_tag{})); // Throws
    seconds->init_from_ref(alloc, seconds_ref);
    seconds->set_parent(top.get(), 0);

    nanoseconds.reset(new BpTree<int64_t>(BpTreeBase::unattached_tag{})); // Throws
    nanoseconds->init_from_ref(alloc, nanoseconds_ref);
    nanoseconds->set_parent(top.get(), 1);

    m_array = std::move(top);
    m_seconds = std::move(seconds);
    m_nanoseconds = std::move(nanoseconds);
    m_nullable = nullable;
}


template <class BT>
class TimestampColumn::CreateHandler : public ColumnBase::CreateHandler {
public:
    CreateHandler(typename BT::value_type value, Allocator& alloc)
        : m_value(value)
        , m_alloc(alloc)
    {
    }

    ref_type create_leaf(size_t size) override
    {
        MemRef mem = BT::create_leaf(Array::type_Normal, size, m_value, m_alloc); // Throws
        return mem.get_ref();
    }

private:
    const typename BT::value_type m_value;
    Allocator& m_alloc;
};


ref_type TimestampColumn::create(Allocator& alloc, size_t size, bool nullable)
{
    Array top(alloc);
    top.create(Array::type_HasRefs, false /* context_flag */, 2);

    util::Optional<int64_t> default_value = nullable ? util::none : util::make_optional<int64_t>(0);
    CreateHandler<BpTree<util::Optional<int64_t>>> create_handler{default_value, alloc};
    ref_type seconds_ref = ColumnBase::create(alloc, size, create_handler);

    CreateHandler<BpTree<int64_t>> nano_create_handler{0, alloc};
    ref_type nanoseconds_ref = ColumnBase::create(alloc, size, nano_create_handler);

    top.set_as_ref(0, seconds_ref);
    top.set_as_ref(1, nanoseconds_ref);

    return top.get_ref();
}


size_t TimestampColumn::get_size_from_ref(ref_type root_ref, Allocator& alloc) noexcept
{
    const char* root_header = alloc.translate(root_ref);
    ref_type seconds_ref = to_ref(Array::get(root_header, 0));
    return IntNullColumn::get_size_from_ref(seconds_ref, alloc);
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
    return m_nullable;
}

/// Whether or not the value at \a row_ndx is NULL. If the column is not
/// nullable, always returns false.
bool TimestampColumn::is_null(size_t row_ndx) const noexcept
{
    // If this assert triggers, this column object was instantiated with bad nullability flag in the
    // constructor, compared to what it was created with by the static ::create() method
    REALM_ASSERT_DEBUG(!(!m_nullable && m_seconds->is_null(row_ndx)));

    return m_seconds->is_null(row_ndx);
}

/// Sets the value at \a row_ndx to be NULL.
/// \throw LogicError Thrown if this column is not nullable.
void TimestampColumn::set_null(size_t row_ndx)
{
    // FIXME: Consider not setting 0 on m_nanoseconds
    // The current setting of 0 forces an arguably unnecessary copy-on-write etc of that leaf node
    m_seconds->set_null(row_ndx);   // Throws
    m_nanoseconds->set(row_ndx, 0); // Throws
}

void TimestampColumn::insert_rows(size_t row_ndx, size_t num_rows_to_insert, size_t /*prior_num_rows*/, bool nullable)
{
    bool is_append = row_ndx == size();
    size_t row_ndx_or_npos = is_append ? realm::npos : row_ndx;

    util::Optional<int64_t> default_value = nullable ? util::none : util::make_optional<int64_t>(0);
    m_seconds->insert(row_ndx_or_npos, default_value, num_rows_to_insert); // Throws
    m_nanoseconds->insert(row_ndx_or_npos, 0, num_rows_to_insert);         // Throws
}

void TimestampColumn::erase(size_t row_ndx, bool is_last)
{
    m_seconds->erase(row_ndx, is_last);     // Throws
    m_nanoseconds->erase(row_ndx, is_last); // Throws
}

void TimestampColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t /*prior_num_rows*/,
                                 bool /*broken_reciprocal_backlinks*/)
{
    bool is_last = (row_ndx + num_rows_to_erase) == size();
    for (size_t i = 0; i < num_rows_to_erase; ++i) {
        m_seconds->erase(row_ndx + num_rows_to_erase - i - 1, is_last);     // Throws
        m_nanoseconds->erase(row_ndx + num_rows_to_erase - i - 1, is_last); // Throws
    }
}

void TimestampColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows, bool /*broken_reciprocal_backlinks*/)
{
    size_t last_row_ndx = prior_num_rows - 1;

    m_seconds->move_last_over(row_ndx, last_row_ndx);     // Throws
    m_nanoseconds->move_last_over(row_ndx, last_row_ndx); // Throws
}

void TimestampColumn::clear(size_t num_rows, bool /*broken_reciprocal_backlinks*/)
{
    REALM_ASSERT_EX(num_rows == m_seconds->size(), num_rows, m_seconds->size());
    static_cast<void>(num_rows);
    m_seconds->clear();     // Throws
    m_nanoseconds->clear(); // Throws
}

void TimestampColumn::swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    auto tmp1 = m_seconds->get(row_ndx_1);
    m_seconds->set(row_ndx_1, m_seconds->get(row_ndx_2)); // Throws
    m_seconds->set(row_ndx_2, tmp1);                      // Throws
    auto tmp2 = m_nanoseconds->get(row_ndx_1);
    m_nanoseconds->set(row_ndx_1, m_nanoseconds->get(row_ndx_2)); // Throws
    m_nanoseconds->set(row_ndx_2, tmp2);                          // Throws
}

void TimestampColumn::destroy() noexcept
{
    m_seconds->destroy();
    m_nanoseconds->destroy();
    if (m_array)
        m_array->destroy();

    if (m_search_index)
        m_search_index->destroy();
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
}

void TimestampColumn::update_from_parent(size_t old_baseline) noexcept
{
    m_array->update_from_parent(old_baseline);

    m_seconds->update_from_parent(old_baseline);
    m_nanoseconds->update_from_parent(old_baseline);
}

void TimestampColumn::refresh_accessor_tree(size_t new_col_ndx, const Spec& spec)
{
    ColumnBaseSimple::refresh_accessor_tree(new_col_ndx, spec);

    m_array->init_from_parent();

    m_seconds->init_from_parent();
    m_nanoseconds->init_from_parent();
}

// LCOV_EXCL_START ignore debug functions

void TimestampColumn::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT_3(m_seconds->size(), ==, m_nanoseconds->size());

    for (size_t t = 0; t < size(); t++) {
        REALM_ASSERT_3(m_nanoseconds->get(t), <, Timestamp::nanoseconds_per_second);
    }

    m_seconds->verify();
    m_nanoseconds->verify();
#endif
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

// LCOV_EXCL_STOP ignore debug functions

void TimestampColumn::add(const Timestamp& ts)
{
    bool ts_is_null = ts.is_null();
    util::Optional<int64_t> seconds = ts_is_null ? util::none : util::make_optional(ts.get_seconds());
    int32_t nanoseconds = ts_is_null ? 0 : ts.get_nanoseconds();
    m_seconds->insert(npos, seconds);         // Throws
    m_nanoseconds->insert(npos, nanoseconds); // Throws
}

Timestamp TimestampColumn::get(size_t row_ndx) const noexcept
{
    util::Optional<int64_t> seconds = m_seconds->get(row_ndx);
    return seconds ? Timestamp(*seconds, int32_t(m_nanoseconds->get(row_ndx))) : Timestamp{};
}

void TimestampColumn::set(size_t row_ndx, const Timestamp& ts)
{
    if (ts.is_null()) {
        return set_null(row_ndx); // Throws
    }

    util::Optional<int64_t> seconds = util::make_optional(ts.get_seconds());
    int32_t nanoseconds = ts.get_nanoseconds();

    m_seconds->set(row_ndx, seconds);         // Throws
    m_nanoseconds->set(row_ndx, nanoseconds); // Throws
}

bool TimestampColumn::compare(const TimestampColumn& c) const noexcept
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i < n; ++i) {
        Timestamp left = get(i);
        Timestamp right = c.get(i);
        if (!realm::Equal()(left, right, left.is_null(), right.is_null())) {
            return false;
        }
    }
    return true;
}

int TimestampColumn::compare_values(size_t row1, size_t row2) const noexcept
{
    return ColumnBase::compare_values(this, row1, row2);
}

Timestamp TimestampColumn::maximum(size_t* result_index) const
{
    return minmax<Greater>(result_index);
}

Timestamp TimestampColumn::minimum(size_t* result_index) const
{
    return minmax<Less>(result_index);
}
}
