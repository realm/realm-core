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

#include <tightdb/column.hpp>
#include <tightdb/column_type.hpp>
#include <tightdb/column_table.hpp>
#include <tightdb/table.hpp>
#include <tightdb/index.hpp>

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

    ColumnType GetType(size_t ndx) const;
    size_t Size() const {return m_types->Size();}
    bool is_empty() const {return m_types->is_empty();}

    int64_t GetInt(size_t ndx) const;
    bool get_bool(size_t ndx) const;
    time_t get_date(size_t ndx) const;
    const char* get_string(size_t ndx) const;
    BinaryData get_binary(size_t ndx) const;

    /// The returned size is zero if the specified row does not
    /// contain a subtable.
    size_t get_subtable_size(std::size_t row_idx) const;

    /// Returns null if the specified row does not contain a subtable,
    /// otherwise the returned table pointer must end up being wrapped
    /// by an instance of BasicTableRef.
    Table* get_subtable_ptr(std::size_t row_idx) const;

    void SetInt(size_t ndx, int64_t value);
    void set_bool(size_t ndx, bool value);
    void set_date(size_t ndx, time_t value);
    void set_string(size_t ndx, const char* value);
    void set_binary(size_t ndx, const char* value, size_t len);
    bool set_subtable(size_t ndx);

    void insert_int(size_t ndx, int64_t value);
    void insert_bool(size_t ndx, bool value);
    void insert_date(size_t ndx, time_t value);
    void insert_string(size_t ndx, const char* value);
    void insert_binary(size_t ndx, const char* value, size_t len);
    bool insert_subtable(size_t ndx);

    bool add() { insert_int(Size(), 0); return true; }
    void insert(size_t ndx) { insert_int(ndx, 0); invalidate_subtables(); }
    void Clear();
    void Delete(size_t ndx);

    // Indexing
    bool HasIndex() const {return false;}
    void BuildIndex(Index& index) {(void)index;}
    void ClearIndex() {}

    size_t GetRef() const {return m_array->GetRef();}

    void invalidate_subtables();

    /// Compare two mixed columns for equality.
    bool Compare(const ColumnMixed&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void ToDot(std::ostream& out, const char* title) const;
#endif // TIGHTDB_DEBUG

private:
    void Create(Allocator& alloc, const Table* table, size_t column_ndx);
    void Create(Allocator& alloc, const Table* table, size_t column_ndx,
                ArrayParent* parent, size_t ndx_in_parent, size_t ref);
    void InitDataColumn();

    void ClearValue(size_t ndx, ColumnType newtype);

    class RefsColumn;

    // Member variables
    Column*       m_types;
    RefsColumn*   m_refs;
    ColumnBinary* m_data;
};


class ColumnMixed::RefsColumn: public ColumnSubtableParent
{
public:
    RefsColumn(Allocator& alloc, const Table* table, std::size_t column_ndx):
        ColumnSubtableParent(alloc, table, column_ndx) {}
    RefsColumn(Allocator& alloc, const Table* table, std::size_t column_ndx,
               ArrayParent* parent, std::size_t ndx_in_parent, std::size_t ref):
        ColumnSubtableParent(alloc, table, column_ndx, parent, ndx_in_parent, ref) {}
    using ColumnSubtableParent::get_subtable_ptr;
    using ColumnSubtableParent::get_subtable;

#ifdef TIGHTDB_ENABLE_REPLICATION
    // Overriding method in ColumnSubtableParent
    virtual size_t* record_subtable_path(size_t* begin, size_t* end)
    {
        if (end == begin) return 0; // Error, not enough space in buffer
        ArrayParent* const parent = m_array->GetParent();
        TIGHTDB_ASSERT(dynamic_cast<Array*>(parent));
        const size_t column_index = static_cast<Array*>(parent)->GetParentNdx();
        *begin++ = column_index;
        if (end == begin) return 0; // Error, not enough space in buffer
        return m_table->record_subtable_path(begin, end);
    }
#endif // TIGHTDB_ENABLE_REPLICATION
};


inline ColumnMixed::ColumnMixed(): m_data(0)
{
    Create(GetDefaultAllocator(), 0, 0);
}

inline ColumnMixed::ColumnMixed(Allocator& alloc, const Table* table, std::size_t column_ndx):
    m_data(0)
{
    Create(alloc, table, column_ndx);
}

inline ColumnMixed::ColumnMixed(Allocator& alloc, const Table* table, std::size_t column_ndx,
                                ArrayParent* parent, std::size_t ndx_in_parent, std::size_t ref):
    m_data(0)
{
    Create(alloc, table, column_ndx, parent, ndx_in_parent, ref);
}

inline size_t ColumnMixed::get_subtable_size(size_t row_idx) const
{
    // FIXME: If the table object is cached, it is possible to get the
    // size from it. Maybe it is faster in general to check for the
    // the presence of the cached object and use it when available.
    TIGHTDB_ASSERT(row_idx < m_types->Size());
    if (m_types->Get(row_idx) != COLUMN_TYPE_TABLE) return 0;
    const size_t top_ref = m_refs->GetAsRef(row_idx);
    const size_t columns_ref = Array(top_ref, NULL, 0, m_refs->GetAllocator()).GetAsRef(1);
    const Array columns(columns_ref, NULL, 0, m_refs->GetAllocator());
    if (columns.is_empty()) return 0;
    const size_t first_col_ref = columns.GetAsRef(0);
    return get_size_from_ref(first_col_ref, m_refs->GetAllocator());
}

inline Table* ColumnMixed::get_subtable_ptr(size_t row_idx) const
{
    TIGHTDB_ASSERT(row_idx < m_types->Size());
    if (m_types->Get(row_idx) != COLUMN_TYPE_TABLE) return 0;
    return m_refs->get_subtable_ptr(row_idx);
}

inline void ColumnMixed::invalidate_subtables()
{
    m_refs->invalidate_subtables();
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_MIXED_HPP
