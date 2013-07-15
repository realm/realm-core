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
#ifndef TIGHTDB_COLUMN_TABLE_HPP
#define TIGHTDB_COLUMN_TABLE_HPP

#include <tightdb/column.hpp>
#include <tightdb/table.hpp>

namespace tightdb {


/// Base class for any type of column that can contain subtables.
class ColumnSubtableParent: public Column, public Table::Parent {
public:
    void UpdateFromParent();

    void invalidate_subtables();

    void Clear() TIGHTDB_OVERRIDE
    {
        m_array->Clear();
        if (m_array->IsNode()) m_array->set_type(Array::coldef_HasRefs);
        invalidate_subtables();
    }

    void move_last_over(size_t ndx) TIGHTDB_OVERRIDE;

protected:
    /// A pointer to the table that this column is part of. For a
    /// free-standing column, this pointer is null.
    const Table* const m_table;

    /// The index of this column within the table that this column is
    /// part of. For a free-standing column, this index is zero.
    ///
    /// This index specifies the position of the column within the
    /// Table::m_cols array. Note that this corresponds to the logical
    /// index of the column, which is not always the same as the index
    /// of this column within Table::m_columns. This is because
    /// Table::m_columns contains a varying number of entries for each
    /// column depending on the type of column.
    std::size_t m_index;

    ColumnSubtableParent(Allocator&, const Table*, std::size_t column_ndx);

    ColumnSubtableParent(Allocator&, const Table*, std::size_t column_ndx,
                         ArrayParent* parent, std::size_t ndx_in_parent, std::size_t ref);

    /// Get the subtable at the specified index.
    ///
    /// This method must be used only for subtables with shared spec,
    /// i.e. for elements of a ColumnTable.
    ///
    /// The returned table pointer must always end up being wrapped in
    /// a TableRef.
    Table* get_subtable_ptr(std::size_t subtable_ndx, std::size_t spec_ref) const;

    /// Get the subtable at the specified index.
    ///
    /// This method must be used only for subtables with independent
    /// specs, i.e. for elements of a ColumnMixed.
    ///
    /// The returned table pointer must always end up being wrapped in
    /// a TableRef.
    Table* get_subtable_ptr(std::size_t subtable_ndx) const;

    /// This method must be used only for subtables with shared spec,
    /// i.e. for elements of a ColumnTable.
    TableRef get_subtable(std::size_t subtable_ndx, std::size_t spec_ref) const
    {
        return TableRef(get_subtable_ptr(subtable_ndx, spec_ref));
    }

    /// This method must be used only for subtables with independent
    /// specs, i.e. for elements of a ColumnMixed.
    TableRef get_subtable(std::size_t subtable_ndx) const
    {
        return TableRef(get_subtable_ptr(subtable_ndx));
    }

    void update_child_ref(std::size_t subtable_ndx, std::size_t new_ref) TIGHTDB_OVERRIDE;
    std::size_t get_child_ref(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void child_destroyed(std::size_t subtable_ndx) TIGHTDB_OVERRIDE;

    /// Assumes that the two tables have the same spec.
    static bool compare_subtable_rows(const Table&, const Table&);

    /// Construct a copy of the columns array of the specified table
    /// and return just the ref to that array.
    ///
    /// In the clone, no string column will be of the enumeration
    /// type.
    std::size_t clone_table_columns(const Table*);

#ifdef TIGHTDB_ENABLE_REPLICATION
    size_t* record_subtable_path(size_t* begin, size_t* end) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        if (end == begin) return 0; // Error, not enough space in buffer
        *begin++ = m_index;
        if (end == begin) return 0; // Error, not enough space in buffer
        return m_table->record_subtable_path(begin, end);
    }
#endif // TIGHTDB_ENABLE_REPLICATION

private:
    struct SubtableMap {
        SubtableMap(Allocator& alloc): m_indices(alloc), m_wrappers(alloc) {}
        ~SubtableMap();
        bool empty() const TIGHTDB_NOEXCEPT { return !m_indices.IsValid() || m_indices.is_empty(); }
        Table* find(std::size_t subtable_ndx) const;
        void insert(std::size_t subtable_ndx, Table* wrapper);
        void remove(std::size_t subtable_ndx);
        void update_from_parents();
        void invalidate_subtables();
    private:
        Array m_indices;
        Array m_wrappers;
    };

    mutable SubtableMap m_subtable_map;
};



class ColumnTable: public ColumnSubtableParent {
public:
    /// Create a subtable column wrapper and have it instantiate a new
    /// underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you must
    /// pass a pointer to that table. Otherwise you must pass null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                std::size_t spec_ref);

    /// Create a subtable column wrapper and attach it to a
    /// preexisting underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you must
    /// pass a pointer to that table. Otherwise you must pass null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                ArrayParent* parent, std::size_t idx_in_parent,
                std::size_t spec_ref, std::size_t column_ref);

    bool   has_subtable(size_t ndx) const;
    size_t get_subtable_size(size_t ndx) const TIGHTDB_NOEXCEPT;

    /// The returned table pointer must always end up being wrapped in
    /// an instance of BasicTableRef.
    Table* get_subtable_ptr(std::size_t subtable_ndx) const
    {
        return ColumnSubtableParent::get_subtable_ptr(subtable_ndx, m_ref_specSet);
    }

    // When passing a table to add() or insert() it is assumed that
    // the table spec is compatible with this column. The number of
    // columns must be the same, and the corresponding columns must
    // have the same data type (as returned by
    // Table::get_column_type()).

    void add() TIGHTDB_OVERRIDE;
    void add(const Table*);
    void insert(std::size_t ndx) TIGHTDB_OVERRIDE;
    void insert(std::size_t ndx, const Table*);
    void set(std::size_t ndx, const Table*);
    void erase(size_t ndx) TIGHTDB_OVERRIDE;
    void ClearTable(size_t ndx);
    void fill(size_t count);

    /// Compare two subtable columns for equality.
    bool compare(const ColumnTable&) const;

    void invalidate_subtables_virtual() TIGHTDB_OVERRIDE;

    void set_specref(size_t ref) {m_ref_specSet = ref;}

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

protected:
#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG

    // Member variables
    size_t m_ref_specSet;

    bool subtables_have_shared_spec() TIGHTDB_OVERRIDE { return true; }
};





// Implementation

inline void ColumnSubtableParent::UpdateFromParent()
{
    if (!m_array->UpdateFromParent()) return;
    m_subtable_map.update_from_parents();
}

inline Table* ColumnSubtableParent::get_subtable_ptr(std::size_t subtable_ndx) const
{
    TIGHTDB_ASSERT(subtable_ndx < Size());

    Table *subtable = m_subtable_map.find(subtable_ndx);
    if (!subtable) {
        const std::size_t top_ref = get_as_ref(subtable_ndx);
        Allocator& alloc = get_alloc();
        subtable = new Table(Table::RefCountTag(), alloc, top_ref,
                             const_cast<ColumnSubtableParent*>(this), subtable_ndx);
        const bool was_empty = m_subtable_map.empty();
        m_subtable_map.insert(subtable_ndx, subtable);
        if (was_empty && m_table) m_table->bind_ref();
    }
    return subtable;
}

inline Table* ColumnSubtableParent::get_subtable_ptr(std::size_t subtable_ndx,
                                                     std::size_t spec_ref) const
{
    TIGHTDB_ASSERT(subtable_ndx < Size());

    Table *subtable = m_subtable_map.find(subtable_ndx);
    if (!subtable) {
        const std::size_t columns_ref = get_as_ref(subtable_ndx);
        Allocator& alloc = get_alloc();
        subtable = new Table(Table::RefCountTag(), alloc, spec_ref, columns_ref,
                             const_cast<ColumnSubtableParent*>(this), subtable_ndx);
        const bool was_empty = m_subtable_map.empty();
        m_subtable_map.insert(subtable_ndx, subtable);
        if (was_empty && m_table) m_table->bind_ref();
    }
    return subtable;
}

inline ColumnSubtableParent::SubtableMap::~SubtableMap()
{
    if (m_indices.IsValid()) {
        TIGHTDB_ASSERT(m_indices.is_empty());
        m_indices.Destroy();
        m_wrappers.Destroy();
    }
}

inline Table* ColumnSubtableParent::SubtableMap::find(size_t subtable_ndx) const
{
    if (!m_indices.IsValid()) return 0;
    size_t const pos = m_indices.find_first(subtable_ndx);
    return pos != size_t(-1) ? reinterpret_cast<Table *>(m_wrappers.get(pos)) : 0;
}

inline void ColumnSubtableParent::SubtableMap::insert(size_t subtable_ndx, Table* wrapper)
{
    if (!m_indices.IsValid()) {
        m_indices.set_type(Array::coldef_Normal);
        m_wrappers.set_type(Array::coldef_Normal);
    }
    m_indices.add(subtable_ndx);
    m_wrappers.add(reinterpret_cast<unsigned long>(wrapper));
}

inline void ColumnSubtableParent::SubtableMap::remove(size_t subtable_ndx)
{
    TIGHTDB_ASSERT(m_indices.IsValid());
    const size_t pos = m_indices.find_first(subtable_ndx);
    TIGHTDB_ASSERT(pos != size_t(-1));
    // FIXME: It is a problem that Array as our most low-level array
    // construct has too many features to deliver a Delete() method
    // that cannot be guaranteed to never throw.
    m_indices.Delete(pos);
    m_wrappers.Delete(pos);
}

inline void ColumnSubtableParent::SubtableMap::update_from_parents()
{
    if (!m_indices.IsValid()) return;

    const size_t count = m_wrappers.size();
    for (size_t i = 0; i < count; ++i) {
        Table* const t = reinterpret_cast<Table*>(m_wrappers.get(i));
        t->UpdateFromParent();
    }
}

inline void ColumnSubtableParent::SubtableMap::invalidate_subtables()
{
    if (!m_indices.IsValid()) return;

    const size_t n = m_wrappers.size();
    for (size_t i=0; i<n; ++i) {
        Table* const t = reinterpret_cast<Table*>(m_wrappers.get(i));
        t->invalidate();
    }

    m_indices.Clear(); // FIXME: Can we rely on Array::Clear() never failing????
    m_wrappers.Clear();
}

inline ColumnSubtableParent::ColumnSubtableParent(Allocator& alloc,
                                                  const Table* table, std::size_t column_ndx):
    Column(Array::coldef_HasRefs, alloc),
    m_table(table), m_index(column_ndx),
    m_subtable_map(Allocator::get_default()) {}

inline ColumnSubtableParent::ColumnSubtableParent(Allocator& alloc,
                                                  const Table* table, std::size_t column_ndx,
                                                  ArrayParent* parent, std::size_t ndx_in_parent,
                                                  std::size_t ref):
    Column(ref, parent, ndx_in_parent, alloc),
    m_table(table), m_index(column_ndx),
    m_subtable_map(Allocator::get_default()) {}

inline void ColumnSubtableParent::update_child_ref(size_t subtable_ndx, size_t new_ref)
{
    set(subtable_ndx, new_ref);
}

inline size_t ColumnSubtableParent::get_child_ref(size_t subtable_ndx) const TIGHTDB_NOEXCEPT
{
    return get_as_ref(subtable_ndx);
}

inline void ColumnSubtableParent::invalidate_subtables()
{
    const bool was_empty = m_subtable_map.empty();
    m_subtable_map.invalidate_subtables();
    if (!was_empty && m_table) m_table->unbind_ref();
}

inline bool ColumnSubtableParent::compare_subtable_rows(const Table& a, const Table& b)
{
    return a.compare_rows(b);
}

inline std::size_t ColumnSubtableParent::clone_table_columns(const Table* t)
{
    return t->clone_columns(m_array->get_alloc());
}


inline ColumnTable::ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                                std::size_t spec_ref):
    ColumnSubtableParent(alloc, table, column_ndx), m_ref_specSet(spec_ref) {}

inline ColumnTable::ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                                ArrayParent* parent, std::size_t ndx_in_parent,
                                std::size_t spec_ref, std::size_t column_ref):
    ColumnSubtableParent(alloc, table, column_ndx, parent, ndx_in_parent, column_ref),
    m_ref_specSet(spec_ref) {}

inline void ColumnTable::add(const Table* subtable)
{
    insert(Size(), subtable);
}

inline void ColumnTable::invalidate_subtables_virtual()
{
    invalidate_subtables();
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TABLE_HPP
