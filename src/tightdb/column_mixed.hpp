/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COLUMN_MIXED_HPP
#define TIGHTDB_COLUMN_MIXED_HPP

#include <limits>

#include <tightdb/column.hpp>
#include <tightdb/column_type.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/column_binary.hpp>
#include <tightdb/table.hpp>
#include <tightdb/index.hpp>
#include <tightdb/utilities.hpp>


namespace tightdb {


// Pre-declarations
class ColumnBinary;

class ColumnMixed : public ColumnBase {
public:
    /// Create a free-standing mixed column.
    ColumnMixed();

    /// Create a mixed column wrapper and have it instantiate a new
    /// underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you
    /// must pass a pointer to that table. Otherwise you must pass
    /// null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnMixed(Allocator& alloc, const Table* table, std::size_t column_ndx);

    /// Create a mixed column wrapper and attach it to a preexisting
    /// underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you
    /// must pass a pointer to that table. Otherwise you must pass
    /// null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnMixed(Allocator& alloc, const Table* table, std::size_t column_ndx,
                ArrayParent* parent, std::size_t ndx_in_parent, std::size_t ref);

    ~ColumnMixed();
    void Destroy();

    void SetParent(ArrayParent* parent, size_t pndx);
    void UpdateFromParent();

    DataType get_type(size_t ndx) const TIGHTDB_NOEXCEPT;
    size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE { return m_types->Size(); }
    bool is_empty() const TIGHTDB_NOEXCEPT { return m_types->is_empty(); }

    int64_t get_int(size_t ndx) const;
    bool get_bool(size_t ndx) const;
    Date get_date(size_t ndx) const;
    float get_float(size_t ndx) const;
    double get_double(size_t ndx) const;
    StringData get_string(size_t ndx) const;
    BinaryData get_binary(size_t ndx) const;

    /// The returned array ref is zero if the specified row does not
    /// contain a subtable.
    size_t get_subtable_ref(std::size_t row_idx) const TIGHTDB_NOEXCEPT;

    /// The returned size is zero if the specified row does not
    /// contain a subtable.
    size_t get_subtable_size(std::size_t row_idx) const TIGHTDB_NOEXCEPT;

    /// Returns null if the specified row does not contain a subtable,
    /// otherwise the returned table pointer must end up being wrapped
    /// by an instance of BasicTableRef.
    Table* get_subtable_ptr(std::size_t row_idx) const;

    void set_int(size_t ndx, int64_t value);
    void set_bool(size_t ndx, bool value);
    void set_date(size_t ndx, Date value);
    void set_float(size_t ndx, float value);
    void set_double(size_t ndx, double value);
    void set_string(size_t ndx, StringData value);
    void set_binary(size_t ndx, BinaryData value);
    void set_subtable(size_t ndx, const Table*);

    void insert_int(size_t ndx, int64_t value);
    void insert_bool(size_t ndx, bool value);
    void insert_date(size_t ndx, Date value);
    void insert_float(size_t ndx, float value);
    void insert_double(size_t ndx, double value);
    void insert_string(size_t ndx, StringData value);
    void insert_binary(size_t ndx, BinaryData value);
    void insert_subtable(size_t ndx, const Table*);

    void add() TIGHTDB_OVERRIDE { insert_int(Size(), 0); }
    void insert(size_t ndx) TIGHTDB_OVERRIDE { insert_int(ndx, 0); invalidate_subtables(); }
    void Clear() TIGHTDB_OVERRIDE;
    void erase(size_t ndx) TIGHTDB_OVERRIDE;
    void move_last_over(size_t ndx) TIGHTDB_OVERRIDE;
    void fill(size_t count);

    // Indexing
    bool HasIndex() const {return false;}
    void BuildIndex(Index& index) { static_cast<void>(index); }
    void ClearIndex() {}

    size_t get_ref() const {return m_array->get_ref();}

    /// Compare two mixed columns for equality.
    bool compare(const ColumnMixed&) const;

    void invalidate_subtables();

    // Overriding virtual method.
    void invalidate_subtables_virtual();

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void ToDot(std::ostream& out, StringData title) const;
#endif // TIGHTDB_DEBUG

private:
    void Create(Allocator& alloc, const Table* table, size_t column_ndx);
    void Create(Allocator& alloc, const Table* table, size_t column_ndx,
                ArrayParent* parent, size_t ndx_in_parent, size_t ref);
    void InitDataColumn();

    enum MixedColType {
        // NOTE: below numbers must be kept in sync with ColumnType
        // Column types used in Mixed
        mixcol_Int         =  0,
        mixcol_Bool        =  1,
        mixcol_String      =  2,
                           // 3, used for STRING_ENUM in ColumnType
        mixcol_Binary      =  4,
        mixcol_Table       =  5,
        mixcol_Mixed       =  6,
        mixcol_Date        =  7,
                           // 8, used for RESERVED1 in ColumnType
        mixcol_Float       =  9,
        mixcol_Double      = 10, // Positive Double
        mixcol_DoubleNeg   = 11, // Negative Double
        mixcol_IntNeg      = 12  // Negative Integers
    };

    void clear_value(size_t ndx, MixedColType newtype);

    // Get/set/insert 64-bit values in m_refs/m_types
    int64_t get_value(size_t ndx) const;
    void set_value(size_t ndx, int64_t value, MixedColType coltype);
    void insert_int64(size_t ndx, int64_t value, MixedColType pos_type, MixedColType neg_type);
    void set_int64(size_t ndx, int64_t value, MixedColType pos_type, MixedColType neg_type);

    class RefsColumn;

    // Member variables:

    // 'm_types' stores the ColumnType of each value at the given index.
    // For values that uses all 64 bit's the datatype also stores this bit.
    // (By having a type for both positive numbers, and another type for negative numbers)
    Column*       m_types;

    // Bit 0 is used to indicate if it's a reference.
    // If not, the data value is stored (shifted 1 bit left). And the sign bit is stored in m_types.
    RefsColumn*   m_refs;

    // m_data holds any Binary/String data - if needed.
    ColumnBinary* m_data;
};


class ColumnMixed::RefsColumn: public ColumnSubtableParent {
public:
    RefsColumn(Allocator& alloc, const Table* table, std::size_t column_ndx):
        ColumnSubtableParent(alloc, table, column_ndx) {}
    RefsColumn(Allocator& alloc, const Table* table, std::size_t column_ndx,
               ArrayParent* parent, std::size_t ndx_in_parent, std::size_t ref):
        ColumnSubtableParent(alloc, table, column_ndx, parent, ndx_in_parent, ref) {}
    using ColumnSubtableParent::get_subtable_ptr;
    using ColumnSubtableParent::get_subtable;
};


} // namespace tightdb


// Implementation
#include <tightdb/column_mixed_tpl.hpp>


#endif // TIGHTDB_COLUMN_MIXED_HPP
