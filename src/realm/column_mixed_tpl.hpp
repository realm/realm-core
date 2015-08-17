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

namespace realm {

inline MixedColumn::MixedColumn(Allocator& alloc, ref_type ref,
                                Table* table, std::size_t column_index)
{
    create(alloc, ref, table, column_index);
}

inline void MixedColumn::adj_acc_insert_rows(std::size_t row_index,
                                             std::size_t num_rows) REALM_NOEXCEPT
{
    m_data->adj_acc_insert_rows(row_index, num_rows);
}

inline void MixedColumn::adj_acc_erase_row(std::size_t row_index) REALM_NOEXCEPT
{
    m_data->adj_acc_erase_row(row_index);
}

inline void MixedColumn::adj_acc_move_over(std::size_t from_row_index,
                                           std::size_t to_row_index) REALM_NOEXCEPT
{
    m_data->adj_acc_move_over(from_row_index, to_row_index);
}

inline void MixedColumn::adj_acc_clear_root_table() REALM_NOEXCEPT
{
    m_data->adj_acc_clear_root_table();
}

inline ref_type MixedColumn::get_subtable_ref(std::size_t row_index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(row_index, <, m_types->size());
    if (m_types->get(row_index) != type_Table)
        return 0;
    return m_data->get_as_ref(row_index);
}

inline std::size_t MixedColumn::get_subtable_size(std::size_t row_index) const REALM_NOEXCEPT
{
    ref_type top_ref = get_subtable_ref(row_index);
    if (top_ref == 0)
        return 0;
    return _impl::TableFriend::get_size_from_ref(top_ref, m_data->get_alloc());
}

inline Table* MixedColumn::get_subtable_accessor(std::size_t row_index) const REALM_NOEXCEPT
{
    return m_data->get_subtable_accessor(row_index);
}

inline void MixedColumn::discard_subtable_accessor(std::size_t row_index) REALM_NOEXCEPT
{
    m_data->discard_subtable_accessor(row_index);
}

inline Table* MixedColumn::get_subtable_ptr(std::size_t row_index)
{
    REALM_ASSERT_3(row_index, <, m_types->size());
    if (m_types->get(row_index) != type_Table)
        return 0;
    return m_data->get_subtable_ptr(row_index); // Throws
}

inline const Table* MixedColumn::get_subtable_ptr(std::size_t subtable_index) const
{
    return const_cast<MixedColumn*>(this)->get_subtable_ptr(subtable_index);
}

inline void MixedColumn::discard_child_accessors() REALM_NOEXCEPT
{
    m_data->discard_child_accessors();
}


//
// Getters
//

#define REALM_BIT63 0x8000000000000000ULL

inline int64_t MixedColumn::get_value(std::size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(index, <, m_types->size());

    // Shift the unsigned value right - ensuring 0 gets in from left.
    // Shifting signed integers right doesn't ensure 0's.
    uint64_t value = uint64_t(m_data->get(index)) >> 1;
    return int64_t(value);
}

inline int64_t MixedColumn::get_int(std::size_t index) const REALM_NOEXCEPT
{
    // Get first 63 bits of the integer value
    int64_t value = get_value(index);

    // restore 'sign'-bit from the column-type
    MixedColType column_type = MixedColType(m_types->get(index));
    if (column_type == mixcolumn_IntNeg) {
        // FIXME: Bad cast of result of '|' from unsigned to signed
        value |= REALM_BIT63; // set sign bit (63)
    }
    else {
        REALM_ASSERT_3(column_type, ==, mixcolumn_Int);
    }
    return value;
}

inline bool MixedColumn::get_bool(std::size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(m_types->get(index), ==, mixcolumn_Bool);

    return (get_value(index) != 0);
}

inline DateTime MixedColumn::get_datetime(std::size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(m_types->get(index), ==, mixcolumn_Date);

    return DateTime(get_value(index));
}

inline float MixedColumn::get_float(std::size_t index) const REALM_NOEXCEPT
{
    REALM_STATIC_ASSERT(std::numeric_limits<float>::is_iec559, "'float' is not IEEE");
    REALM_STATIC_ASSERT((sizeof (float) * CHAR_BIT == 32), "Assume 32 bit float.");
    REALM_ASSERT_3(m_types->get(index), ==, mixcolumn_Float);

    return type_punning<float>(get_value(index));
}

inline double MixedColumn::get_double(std::size_t index) const REALM_NOEXCEPT
{
    REALM_STATIC_ASSERT(std::numeric_limits<double>::is_iec559, "'double' is not IEEE");
    REALM_STATIC_ASSERT((sizeof (double) * CHAR_BIT == 64), "Assume 64 bit double.");

    int64_t int_val = get_value(index);

    // restore 'sign'-bit from the column-type
    MixedColType column_type = MixedColType(m_types->get(index));
    if (column_type == mixcolumn_DoubleNeg)
        int_val |= REALM_BIT63; // set sign bit (63)
    else {
        REALM_ASSERT_3(column_type, ==, mixcolumn_Double);
    }
    return type_punning<double>(int_val);
}

inline StringData MixedColumn::get_string(std::size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(index, <, m_types->size());
    REALM_ASSERT_3(m_types->get(index), ==, mixcolumn_String);
    REALM_ASSERT(m_binary_data);

    std::size_t data_index = std::size_t(int64_t(m_data->get(index)) >> 1);
    return m_binary_data->get_string(data_index);
}

inline BinaryData MixedColumn::get_binary(std::size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(index, <, m_types->size());
    REALM_ASSERT_3(m_types->get(index), ==, mixcolumn_Binary);
    REALM_ASSERT(m_binary_data);

    std::size_t data_index = std::size_t(uint64_t(m_data->get(index)) >> 1);
    return m_binary_data->get(data_index);
}

//
// Setters
//

// Set a int64 value.
// Store 63 bit of the value in m_data. Store sign bit in m_types.

inline void MixedColumn::set_int64(std::size_t index, int64_t value, MixedColType pos_type, MixedColType neg_type)
{
    REALM_ASSERT_3(index, <, m_types->size());

    // If sign-bit is set in value, 'store' it in the column-type
    MixedColType coltype = ((value & REALM_BIT63) == 0) ? pos_type : neg_type;

    // Remove refs or binary data (sets type to double)
    clear_value_and_discard_subtab_acc(index, coltype); // Throws

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    value = (value << 1) + 1;
    m_data->set(index, value);
}

inline void MixedColumn::set_int(std::size_t index, int64_t value)
{
    set_int64(index, value, mixcolumn_Int, mixcolumn_IntNeg); // Throws
}

inline void MixedColumn::set_double(std::size_t index, double value)
{
    int64_t val64 = type_punning<int64_t>(value);
    set_int64(index, val64, mixcolumn_Double, mixcolumn_DoubleNeg); // Throws
}

inline void MixedColumn::set_value(std::size_t index, int64_t value, MixedColType coltype)
{
    REALM_ASSERT_3(index, <, m_types->size());

    // Remove refs or binary data (sets type to float)
    clear_value_and_discard_subtab_acc(index, coltype); // Throws

    // Shift value one bit and set lowest bit to indicate that this is not a ref
    int64_t v = (value << 1) + 1;
    m_data->set(index, v); // Throws
}

inline void MixedColumn::set_float(std::size_t index, float value)
{
    int64_t val64 = type_punning<int64_t>(value);
    set_value(index, val64, mixcolumn_Float); // Throws
}

inline void MixedColumn::set_bool(std::size_t index, bool value)
{
    set_value(index, (value ? 1 : 0), mixcolumn_Bool); // Throws
}

inline void MixedColumn::set_datetime(std::size_t index, DateTime value)
{
    set_value(index, int64_t(value.get_datetime()), mixcolumn_Date); // Throws
}

inline void MixedColumn::set_subtable(std::size_t index, const Table* t)
{
    REALM_ASSERT_3(index, <, m_types->size());
    typedef _impl::TableFriend tf;
    ref_type ref;
    if (t) {
        ref = tf::clone(*t, get_alloc()); // Throws
    }
    else {
        ref = tf::create_empty_table(get_alloc()); // Throws
    }
    // Remove any previous refs or binary data
    clear_value_and_discard_subtab_acc(index, mixcolumn_Table); // Throws
    m_data->set(index, ref); // Throws
}

//
// Inserts
//

inline void MixedColumn::insert_value(std::size_t row_index, int_fast64_t types_value,
                                      int_fast64_t data_value)
{
    std::size_t size = m_types->size(); // Slow
    bool is_append = row_index == size;
    std::size_t row_index_2 = is_append ? realm::npos : row_index;
    std::size_t num_rows = 1;
    m_types->insert_without_updating_index(row_index_2, types_value, num_rows); // Throws
    m_data->do_insert(row_index_2, data_value, num_rows); // Throws
}

// Insert a int64 value.
// Store 63 bit of the value in m_data. Store sign bit in m_types.

inline void MixedColumn::insert_int(std::size_t index, int_fast64_t value, MixedColType type)
{
    int_fast64_t types_value = type;
    // Shift value one bit and set lowest bit to indicate that this is not a ref
    int_fast64_t data_value =  1 + (value << 1);
    insert_value(index, types_value, data_value); // Throws
}

inline void MixedColumn::insert_pos_neg(std::size_t index, int_fast64_t value, MixedColType pos_type,
                                        MixedColType neg_type)
{
    // 'store' the sign-bit in the integer-type
    MixedColType type = (value & REALM_BIT63) == 0 ? pos_type : neg_type;
    int_fast64_t types_value = type;
    // Shift value one bit and set lowest bit to indicate that this is not a ref
    int_fast64_t data_value =  1 + (value << 1);
    insert_value(index, types_value, data_value); // Throws
}

inline void MixedColumn::insert_int(std::size_t index, int_fast64_t value)
{
    insert_pos_neg(index, value, mixcolumn_Int, mixcolumn_IntNeg); // Throws
}

inline void MixedColumn::insert_double(std::size_t index, double value)
{
    int_fast64_t value_2 = type_punning<int64_t>(value);
    insert_pos_neg(index, value_2, mixcolumn_Double, mixcolumn_DoubleNeg); // Throws
}

inline void MixedColumn::insert_float(std::size_t index, float value)
{
    int_fast64_t value_2 = type_punning<int32_t>(value);
    insert_int(index, value_2, mixcolumn_Float); // Throws
}

inline void MixedColumn::insert_bool(std::size_t index, bool value)
{
    int_fast64_t value_2 = int_fast64_t(value);
    insert_int(index, value_2, mixcolumn_Bool); // Throws
}

inline void MixedColumn::insert_datetime(std::size_t index, DateTime value)
{
    int_fast64_t value_2 = int_fast64_t(value.get_datetime());
    insert_int(index, value_2, mixcolumn_Date); // Throws
}

inline void MixedColumn::insert_string(std::size_t index, StringData value)
{
    ensure_binary_data_column();
    std::size_t blob_index = m_binary_data->size();
    m_binary_data->add_string(value); // Throws

    int_fast64_t value_2 = int_fast64_t(blob_index);
    insert_int(index, value_2, mixcolumn_String); // Throws
}

inline void MixedColumn::insert_binary(std::size_t index, BinaryData value)
{
    ensure_binary_data_column();
    std::size_t blob_index = m_binary_data->size();
    m_binary_data->add(value); // Throws

    int_fast64_t value_2 = int_fast64_t(blob_index);
    insert_int(index, value_2, mixcolumn_Binary); // Throws
}

inline void MixedColumn::insert_subtable(std::size_t index, const Table* t)
{
    typedef _impl::TableFriend tf;
    ref_type ref;
    if (t) {
        ref = tf::clone(*t, get_alloc()); // Throws
    }
    else {
        ref = tf::create_empty_table(get_alloc()); // Throws
    }
    int_fast64_t types_value = mixcolumn_Table;
    int_fast64_t data_value = int_fast64_t(ref);
    insert_value(index, types_value, data_value); // Throws
}

inline void MixedColumn::erase(std::size_t row_index)
{
    size_t num_rows_to_erase = 1;
    size_t prior_num_rows = size(); // Note that size() is slow
    do_erase(row_index, num_rows_to_erase, prior_num_rows); // Throws
}

inline void MixedColumn::move_last_over(std::size_t row_index)
{
    size_t prior_num_rows = size(); // Note that size() is slow
    do_move_last_over(row_index, prior_num_rows); // Throws
}

inline void MixedColumn::clear()
{
    std::size_t num_rows = size(); // Note that size() is slow
    do_clear(num_rows); // Throws
}

inline std::size_t MixedColumn::get_size_from_ref(ref_type root_ref,
                                                  Allocator& alloc) REALM_NOEXCEPT
{
    const char* root_header = alloc.translate(root_ref);
    ref_type types_ref = to_ref(Array::get(root_header, 0));
    return IntegerColumn::get_size_from_ref(types_ref, alloc);
}

inline void MixedColumn::clear_value_and_discard_subtab_acc(std::size_t row_index,
                                                            MixedColType new_type)
{
    MixedColType old_type = clear_value(row_index, new_type);
    if (old_type == mixcolumn_Table)
        m_data->discard_subtable_accessor(row_index);
}

// Implementing pure virtual method of ColumnBase.
inline void MixedColumn::insert_rows(size_t row_index, size_t num_rows_to_insert,
                                     size_t prior_num_rows)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_index <= prior_num_rows);

    size_t row_index_2 = (row_index == prior_num_rows ? realm::npos : row_index);

    int_fast64_t type_value = mixcolumn_Int;
    m_types->insert_without_updating_index(row_index_2, type_value, num_rows_to_insert); // Throws

    // The least significant bit indicates that the rest of the bits form an
    // integer value, so 1 is actually zero.
    int_fast64_t data_value = 1;
    m_data->do_insert(row_index_2, data_value, num_rows_to_insert); // Throws
}

// Implementing pure virtual method of ColumnBase.
inline void MixedColumn::erase_rows(size_t row_index, size_t num_rows_to_erase,
                                    size_t prior_num_rows, bool)
{
    do_erase(row_index, num_rows_to_erase, prior_num_rows); // Throws
}

// Implementing pure virtual method of ColumnBase.
inline void MixedColumn::move_last_row_over(size_t row_index, size_t prior_num_rows, bool)
{
    do_move_last_over(row_index, prior_num_rows); // Throws
}

// Implementing pure virtual method of ColumnBase.
inline void MixedColumn::clear(std::size_t num_rows, bool)
{
    do_clear(num_rows); // Throws
}

inline void MixedColumn::mark(int type) REALM_NOEXCEPT
{
    m_data->mark(type);
}

inline void MixedColumn::refresh_accessor_tree(std::size_t column_index, const Spec& spec)
{
    get_root_array()->init_from_parent();
    m_types->refresh_accessor_tree(column_index, spec); // Throws
    m_data->refresh_accessor_tree(column_index, spec); // Throws
    if (m_binary_data) {
        REALM_ASSERT_3(get_root_array()->size(), ==, 3);
        m_binary_data->refresh_accessor_tree(column_index, spec); // Throws
        return;
    }
    // See if m_binary_data needs to be created.
    if (get_root_array()->size() == 3) {
        ref_type ref = get_root_array()->get_as_ref(2);
        m_binary_data.reset(new BinaryColumn(get_alloc(), ref)); // Throws
        m_binary_data->set_parent(get_root_array(), 2);
    }
}

inline void MixedColumn::RefsColumn::refresh_accessor_tree(std::size_t column_index, const Spec& spec)
{
    SubtableColumnParent::refresh_accessor_tree(column_index, spec); // Throws
    std::size_t spec_index_in_parent = 0; // Ignored because these are root tables
    m_subtable_map.refresh_accessor_tree(spec_index_in_parent); // Throws
}

} // namespace realm
