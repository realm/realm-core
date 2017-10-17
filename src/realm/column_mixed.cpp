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

#include <realm/column_mixed.hpp>

#include <realm/column_binary.hpp>
#include <realm/column_timestamp.hpp>

#include <memory>
#include <ostream>

using namespace realm;
using namespace realm::util;

MixedColumn::RefsColumn::RefsColumn(Allocator& alloc, ref_type ref, Table* table, size_t column_ndx)
    : SubtableColumnBase(alloc, ref, table, column_ndx)
{
}

MixedColumn::RefsColumn::~RefsColumn() noexcept
{
}

MixedColumn::~MixedColumn() noexcept
{
}


void MixedColumn::update_from_parent(size_t old_baseline) noexcept
{
    if (!get_root_array()->update_from_parent(old_baseline))
        return;

    m_types->update_from_parent(old_baseline);
    m_data->update_from_parent(old_baseline);
    if (m_binary_data)
        m_binary_data->update_from_parent(old_baseline);
    if (m_timestamp_data)
        m_timestamp_data->update_from_parent(old_baseline);
}

void MixedColumn::create(Allocator& alloc, ref_type ref, Table* table, size_t column_ndx)
{
    std::unique_ptr<Array> top;
    std::unique_ptr<IntegerColumn> types;
    std::unique_ptr<RefsColumn> data;
    std::unique_ptr<BinaryColumn> binary_data;
    std::unique_ptr<TimestampColumn> timestamp_data;
    top.reset(new Array(alloc)); // Throws
    top->init_from_ref(ref);
    REALM_ASSERT(top->size() == 2 || top->size() == 3 || top->size() == 4);
    ref_type types_ref = top->get_as_ref(0);
    ref_type data_ref = top->get_as_ref(1);
    types.reset(new IntegerColumn(alloc, types_ref)); // Throws
    types->set_parent(top.get(), 0);
    data.reset(new RefsColumn(alloc, data_ref, table, column_ndx)); // Throws
    data->set_parent(top.get(), 1);
    REALM_ASSERT_3(types->size(), ==, data->size());

    // Binary data column with values that does not fit in data column is only
    // there if needed
    if (top->size() >= 3) {
        ref_type binary_data_ref = top->get_as_ref(2);
        binary_data.reset(new BinaryColumn(alloc, binary_data_ref)); // Throws
        binary_data->set_parent(top.get(), 2);
    }

    // TimestampColumn is only there if needed
    if (top->size() >= 4) {
        ref_type timestamp_ref = top->get_as_ref(3);
        // When adding/creating a Mixed column the user cannot specify nullability, so the "true" below
        // makes it implicitly nullable, which may not be wanted. But it's OK since Mixed columns are not
        // publicly supported
        timestamp_data.reset(new TimestampColumn(true /*FIXME*/, alloc, timestamp_ref)); // Throws
        timestamp_data->set_parent(top.get(), 3);
    }

    m_array = std::move(top);
    m_types = std::move(types);
    m_data = std::move(data);
    m_binary_data = std::move(binary_data);
    m_timestamp_data = std::move(timestamp_data);
}


void MixedColumn::ensure_binary_data_column()
{
    if (m_binary_data)
        return;

    ref_type ref = BinaryColumn::create(m_array->get_alloc(), 0, true); // Throws
    m_binary_data.reset(new BinaryColumn(m_array->get_alloc(), ref));   // Throws
    REALM_ASSERT_3(m_array->size(), ==, 2);
    m_array->add(ref); // Throws
    m_binary_data->set_parent(m_array.get(), 2);
}


void MixedColumn::ensure_timestamp_column()
{
    // binary data is expected at index 2
    ensure_binary_data_column();

    if (m_timestamp_data)
        return;

    constexpr bool nullable = true;
    ref_type ref = TimestampColumn::create(m_array->get_alloc(), 0, nullable); // Throws
    // When adding/creating a Mixed column the user cannot specify nullability, so the "true" below
    // makes it implicitly nullable, which may not be wanted. But it's OK since Mixed columns are not
    // publicly supported
    m_timestamp_data.reset(new TimestampColumn(true /*FIXME*/, m_array->get_alloc(), ref)); // Throws
    REALM_ASSERT_3(m_array->size(), ==, 3);
    m_array->add(ref); // Throws
    m_timestamp_data->set_parent(m_array.get(), 3);
}


MixedColumn::MixedColType MixedColumn::clear_value(size_t row_ndx, MixedColType new_type)
{
    REALM_ASSERT_3(row_ndx, <, m_types->size());

    MixedColType old_type = MixedColType(m_types->get(row_ndx));
    switch (old_type) {
        case mixcol_Int:
        case mixcol_IntNeg:
        case mixcol_Bool:
        case mixcol_OldDateTime:
        case mixcol_Float:
        case mixcol_Double:
        case mixcol_DoubleNeg:
            goto carry_on;
        case mixcol_String:
        case mixcol_Binary: {
            // If item is in middle of the column, we just clear it to avoid
            // having to adjust refs to following items
            //
            // FIXME: this is a leak. We should adjust (not important, Mixed is not officially supported)
            size_t data_ndx = size_t(uint64_t(m_data->get(row_ndx)) >> 1);
            const bool is_last = data_ndx == m_binary_data->size() - 1;
            if (is_last) {
                m_binary_data->erase(data_ndx, is_last);
            }
            else {
                // FIXME: But this will lead to unbounded in-file leaking in
                // for(;;) { insert_binary(i, ...); erase(i); }  (not important, Mixed is not officially supported)
                m_binary_data->set(data_ndx, BinaryData());
            }
            goto carry_on;
        }
        case mixcol_Timestamp: {
            size_t data_row_ndx = size_t(m_data->get(row_ndx) >> 1);
            const bool is_last = data_row_ndx == m_timestamp_data->size() - 1;
            if (is_last) {
                m_timestamp_data->erase(data_row_ndx, is_last);
            }
            else {
                // FIXME: But this will lead to unbounded in-file leaking in
                // for(;;) { insert_binary(i, ...); erase(i); }
                // (not important because Mixed is not officially supported)
                m_timestamp_data->set(data_row_ndx, Timestamp{});
            }
            goto carry_on;
        }

        case mixcol_Table: {
            // Delete entire table
            ref_type ref = m_data->get_as_ref(row_ndx);
            Array::destroy_deep(ref, m_data->get_alloc());
            goto carry_on;
        }
        case mixcol_Mixed:
            break;
    }
    REALM_ASSERT(false);

carry_on:
    if (old_type != new_type)
        m_types->set(row_ndx, new_type);
    m_data->set(row_ndx, 0);

    return old_type;
}


void MixedColumn::do_erase(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_ndx <= prior_num_rows - num_rows_to_erase);

    bool is_last = (row_ndx + num_rows_to_erase == prior_num_rows);
    for (size_t i = num_rows_to_erase; i > 0; --i) {
        size_t row_ndx_2 = row_ndx + i - 1;
        // Remove refs or binary data
        clear_value(row_ndx_2, mixcol_Int); // Throws
        m_types->erase(row_ndx, is_last);   // Throws
    }

    bool broken_reciprocal_backlinks = false;                                                    // Ignored
    m_data->erase_rows(row_ndx, num_rows_to_erase, prior_num_rows, broken_reciprocal_backlinks); // Throws
}


void MixedColumn::do_move_last_over(size_t row_ndx, size_t prior_num_rows)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx < prior_num_rows);

    // Remove refs or binary data
    clear_value(row_ndx, mixcol_Int); // Throws

    size_t last_row_ndx = prior_num_rows - 1;
    m_types->move_last_over(row_ndx, last_row_ndx); // Throws

    bool broken_reciprocal_backlinks = false;                                         // Ignored
    m_data->move_last_row_over(row_ndx, prior_num_rows, broken_reciprocal_backlinks); // Throws
}

void MixedColumn::do_swap_rows(size_t row_ndx_1, size_t row_ndx_2)
{
    REALM_ASSERT_3(row_ndx_1, <=, size());
    REALM_ASSERT_3(row_ndx_2, <=, size());

    m_types->swap_rows(row_ndx_1, row_ndx_2);
    m_data->swap_rows(row_ndx_1, row_ndx_2);
}


void MixedColumn::do_clear(size_t num_rows)
{
    discard_child_accessors();
    bool broken_reciprocal_backlinks = false;              // Value is immaterial for these column types
    m_types->clear(num_rows, broken_reciprocal_backlinks); // Throws
    m_data->clear(num_rows, broken_reciprocal_backlinks);  // Throws
    if (m_binary_data) {
        m_binary_data->clear(); // Throws
    }
}


DataType MixedColumn::get_type(size_t ndx) const noexcept
{
    REALM_ASSERT_3(ndx, <, m_types->size());
    MixedColType coltype = MixedColType(m_types->get(ndx));
    switch (coltype) {
        case mixcol_IntNeg:
            return type_Int;
        case mixcol_DoubleNeg:
            return type_Double;
        default:
            return DataType(coltype); // all others must be in sync with ColumnType
    }
}


void MixedColumn::set_string(size_t ndx, StringData value)
{
    REALM_ASSERT_3(ndx, <, m_types->size());
    ensure_binary_data_column();

    MixedColType type = MixedColType(m_types->get(ndx));

    // See if we can reuse data position
    if (type == mixcol_String) {
        size_t data_ndx = size_t(uint64_t(m_data->get(ndx)) >> 1);
        m_binary_data->set_string(data_ndx, value);
    }
    else if (type == mixcol_Binary) {
        size_t data_ndx = size_t(uint64_t(m_data->get(ndx)) >> 1);
        m_binary_data->set_string(data_ndx, value);
        m_types->set(ndx, mixcol_String);
    }
    else {
        // Remove refs or binary data
        clear_value_and_discard_subtab_acc(ndx, mixcol_String); // Throws

        // Add value to data column
        size_t data_ndx = m_binary_data->size();
        m_binary_data->add_string(value);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        int64_t v = int64_t((uint64_t(data_ndx) << 1) + 1);

        m_types->set(ndx, mixcol_String);
        m_data->set(ndx, v);
    }
}

void MixedColumn::set_binary(size_t ndx, BinaryData value)
{
    REALM_ASSERT_3(ndx, <, m_types->size());
    ensure_binary_data_column();

    MixedColType type = MixedColType(m_types->get(ndx));

    // See if we can reuse data position
    if (type == mixcol_String) {
        size_t data_ndx = size_t(uint64_t(m_data->get(ndx)) >> 1);
        m_binary_data->set(data_ndx, value);
        m_types->set(ndx, mixcol_Binary);
    }
    else if (type == mixcol_Binary) {
        size_t data_ndx = size_t(uint64_t(m_data->get(ndx)) >> 1);
        m_binary_data->set(data_ndx, value);
    }
    else {
        // Remove refs or binary data
        clear_value_and_discard_subtab_acc(ndx, mixcol_Binary); // Throws

        // Add value to data column
        size_t data_ndx = m_binary_data->size();
        m_binary_data->add(value);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        int64_t v = int64_t((uint64_t(data_ndx) << 1) + 1);

        m_types->set(ndx, mixcol_Binary);
        m_data->set(ndx, v);
    }
}

void MixedColumn::set_timestamp(size_t ndx, Timestamp value)
{
    REALM_ASSERT_3(ndx, <, m_types->size());
    ensure_timestamp_column();

    MixedColType type = MixedColType(m_types->get(ndx));

    // See if we can reuse data position
    if (type == mixcol_Timestamp) {
        size_t data_ndx = size_t(uint64_t(m_data->get(ndx)) >> 1);
        m_timestamp_data->set(data_ndx, value);
    }
    else {
        // Remove refs or string / binary data
        clear_value_and_discard_subtab_acc(ndx, type); // Throws

        // Add value to data column
        size_t data_ndx = m_timestamp_data->size();
        m_timestamp_data->add(value);

        // Shift value one bit and set lowest bit to indicate that this is not a ref
        int64_t v = int64_t((uint64_t(data_ndx) << 1) + 1);

        m_types->set(ndx, mixcol_Timestamp);
        m_data->set(ndx, v);
    }
}

bool MixedColumn::compare_mixed(const MixedColumn& c) const
{
    const size_t n = size();
    if (c.size() != n)
        return false;

    for (size_t i = 0; i < n; ++i) {
        DataType type = get_type(i);
        if (c.get_type(i) != type)
            return false;
        switch (type) {
            case type_Int:
                if (get_int(i) != c.get_int(i))
                    return false;
                break;
            case type_Bool:
                if (get_bool(i) != c.get_bool(i))
                    return false;
                break;
            case type_OldDateTime:
                if (get_olddatetime(i) != c.get_olddatetime(i))
                    return false;
                break;
            case type_Timestamp:
                if (get_timestamp(i) != c.get_timestamp(i))
                    return false;
                break;
            case type_Float:
                if (get_float(i) != c.get_float(i))
                    return false;
                break;
            case type_Double:
                if (get_double(i) != c.get_double(i))
                    return false;
                break;
            case type_String:
                if (get_string(i) != c.get_string(i))
                    return false;
                break;
            case type_Binary:
                if (get_binary(i) != c.get_binary(i))
                    return false;
                break;
            case type_Table: {
                ConstTableRef t1 = get_subtable_tableref(i);
                ConstTableRef t2 = c.get_subtable_tableref(i);
                if (*t1 != *t2)
                    return false;
                break;
            }
            case type_Mixed:
            case type_Link:
            case type_LinkList:
                REALM_ASSERT(false);
                break;
        }
    }
    return true;
}


int MixedColumn::compare_values(size_t, size_t) const noexcept
{
    REALM_ASSERT(false); // querying Mixed is not supported
    return 0;
}


void MixedColumn::do_discard_child_accessors() noexcept
{
    discard_child_accessors();
}


ref_type MixedColumn::create(Allocator& alloc, size_t size)
{
    Array top(alloc);
    top.create(Array::type_HasRefs); // Throws

    {
        int_fast64_t v = mixcol_Int;
        ref_type ref = IntegerColumn::create(alloc, Array::type_Normal, size, v); // Throws
        v = from_ref(ref);
        top.add(v); // Throws
    }
    {
        int_fast64_t v = 1;                                                        // 1 + 2*value where value is 0
        ref_type ref = IntegerColumn::create(alloc, Array::type_HasRefs, size, v); // Throws
        v = from_ref(ref);
        top.add(v); // Throws
    }

    return top.get_ref();
}


ref_type MixedColumn::write(size_t slice_offset, size_t slice_size, size_t table_size, _impl::OutputStream& out) const
{
    // FIXME:There is no reasonably efficient way to implement this. The
    // problem is that we have no guarantees about how the order of entries in
    // m_binary_data relate to the order of entries in the column.
    //
    // It seems that we would have to change m_binary_data to always contain one
    // entry for each entry in the column, and at the corresponding index.
    //
    // An even better solution will probably be to change a mixed column into an
    // ordinary column of mixed leafs. BinaryColumn can serve as a model of how
    // to place multiple subarrays into a single leaf.
    //
    // There are other options such as storing a ref to a ArrayBlob in m_data if
    // the type is 'string'.
    //
    // Unfortunately this will break the file format compatibility.
    //
    // The fact that the current design has other flaws (se FIXMEs in
    // MixedColumn::clear_value()) makes it even more urgent to change the
    // representation and implementation of MixedColumn. Note however, that
    // Mixed is not currently publicly supported.
    //
    // In fact, there is yet another problem with the current design, it relies
    // on the ability of a column to know its own size. While this is not an
    // urgent problem, it is in conflict with the desire to drop the `N_t` field
    // from the B+-tree inner node (a.k.a. `total_elems_in_subtree`).

    ref_type types_ref = m_types->write(slice_offset, slice_size, table_size, out); // Throws
    ref_type data_ref = m_data->write(slice_offset, slice_size, table_size, out);   // Throws

    // FIXME: This is far from good enough. See comments above.
    ref_type binary_data_ref = 0;
    if (m_binary_data) {
        bool deep = true;                                                                      // Deep
        bool only_if_modified = false;                                                         // Always
        binary_data_ref = m_binary_data->get_root_array()->write(out, deep, only_if_modified); // Throws
    }

    // build new top array
    Array top(Allocator::get_default());
    _impl::ShallowArrayDestroyGuard dg(&top);
    top.create(Array::type_HasRefs); // Throws
    {
        int_fast64_t v(from_ref(types_ref));
        top.add(v); // Throws
    }
    {
        int_fast64_t v(from_ref(data_ref));
        top.add(v); // Throws
    }
    if (binary_data_ref != 0) {
        int_fast64_t v(from_ref(binary_data_ref));
        top.add(v); // Throws
    }

    // Write new top array
    bool deep = false;                             // Shallow
    bool only_if_modified = false;                 // Always
    return top.write(out, deep, only_if_modified); // Throws
}

// LCOV_EXCL_START ignore debug functions

void MixedColumn::verify() const
{
#ifdef REALM_DEBUG
    do_verify(0, 0);
#endif
}

void MixedColumn::verify(const Table& table, size_t col_ndx) const
{
#ifdef REALM_DEBUG
    do_verify(&table, col_ndx);

    // Verify each sub-table
    size_t n = size();
    for (size_t i = 0; i < n; ++i) {
        int64_t v = m_data->get(i);
        if (v == 0 || v & 0x1)
            continue;
        ConstTableRef subtable = m_data->get_subtable_tableref(i);
        REALM_ASSERT_3(subtable->get_parent_row_index(), ==, i);
        subtable->verify();
    }
#else
    static_cast<void>(table);
    static_cast<void>(col_ndx);
#endif
}

void MixedColumn::do_dump_node_structure(std::ostream& out, int level) const
{
#ifdef REALM_DEBUG
    m_types->do_dump_node_structure(out, level); // FIXME: How to do this?
#else
    static_cast<void>(out);
    static_cast<void>(level);
#endif
}

void MixedColumn::leaf_to_dot(MemRef, ArrayParent*, size_t, std::ostream&) const
{
}

void MixedColumn::to_dot(std::ostream& out, StringData title) const
{
#ifdef REALM_DEBUG
    ref_type ref = get_ref();
    out << "subgraph cluster_mixed_column" << ref << " {" << std::endl;
    out << " label = \"Mixed column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    m_array->to_dot(out, "mixed_top");
    m_types->to_dot(out, "types");
    m_data->to_dot(out, "refs");
    if (m_array->size() > 2)
        m_binary_data->to_dot(out, "data");

    // Write sub-tables
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        MixedColType type = MixedColType(m_types->get(i));
        if (type != mixcol_Table)
            continue;
        ConstTableRef subtable = m_data->get_subtable_tableref(i);
        subtable->to_dot(out);
    }

    out << "}" << std::endl;
#else
    static_cast<void>(out);
    static_cast<void>(title);
#endif
}

#ifdef REALM_DEBUG

void MixedColumn::do_verify(const Table* table, size_t col_ndx) const
{
    m_array->verify();
    m_types->verify();
    if (table) {
        m_data->verify(*table, col_ndx);
    }
    else {
        m_data->verify();
    }
    if (m_binary_data)
        m_binary_data->verify();

    // types and refs should be in sync
    size_t types_len = m_types->size();
    size_t refs_len = m_data->size();
    REALM_ASSERT_3(types_len, ==, refs_len);
}

#endif // LCOV_EXCL_STOP ignore debug functions
