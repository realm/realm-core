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

#include <vector>

#include <tightdb/util/features.h>
#include <tightdb/column.hpp>
#include <tightdb/table.hpp>

namespace tightdb {


/// Base class for any type of column that can contain subtables.
class ColumnSubtableParent: public Column, public Table::Parent {
public:
    void update_from_parent(std::size_t old_baseline) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    void detach_subtable_accessors() TIGHTDB_NOEXCEPT;

    ~ColumnSubtableParent() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

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
                         ArrayParent*, std::size_t ndx_in_parent, ref_type);

    /// Get the subtable at the specified index.
    ///
    /// This method must be used only for subtables with shared spec,
    /// i.e. for elements of a ColumnTable.
    ///
    /// The returned table pointer must always end up being wrapped in
    /// a TableRef.
    Table* get_subtable_ptr(std::size_t subtable_ndx, ref_type spec_ref) const;

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
    TableRef get_subtable(std::size_t subtable_ndx, ref_type spec_ref) const;

    /// This method must be used only for subtables with independent
    /// specs, i.e. for elements of a ColumnMixed.
    TableRef get_subtable(std::size_t subtable_ndx) const;

    void update_child_ref(std::size_t subtable_ndx, ref_type new_ref) TIGHTDB_OVERRIDE;
    ref_type get_child_ref(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void child_accessor_destroyed(std::size_t subtable_ndx) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    /// Assumes that the two tables have the same spec.
    static bool compare_subtable_rows(const Table&, const Table&);

    /// Construct a copy of the columns array of the specified table
    /// and return just the ref to that array.
    ///
    /// In the clone, no string column will be of the enumeration
    /// type.
    ref_type clone_table_columns(const Table*);

    static ref_type create(std::size_t size, Allocator&);

#ifdef TIGHTDB_DEBUG
    std::pair<ref_type, std::size_t>
    get_to_dot_parent(std::size_t ndx_in_parent) const TIGHTDB_OVERRIDE;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    std::size_t* record_subtable_path(std::size_t* begin,
                                      std::size_t* end) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
#endif

private:
    struct SubtableMap {
        ~SubtableMap() TIGHTDB_NOEXCEPT {}
        bool empty() const TIGHTDB_NOEXCEPT { return m_entries.empty(); }
        Table* find(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT;
        void add(std::size_t subtable_ndx, Table*);
        void remove(std::size_t subtable_ndx) TIGHTDB_NOEXCEPT;
        void update_from_parent(std::size_t old_baseline) const TIGHTDB_NOEXCEPT;
        void detach_accessors() TIGHTDB_NOEXCEPT;
    private:
        struct entry {
            std::size_t m_subtable_ndx;
            Table* m_table;
        };
        typedef std::vector<entry> entries;
        entries m_entries;
    };

    mutable SubtableMap m_subtable_map;
};



class ColumnTable: public ColumnSubtableParent {
public:
    /// Create a subtable column accessor and have it instantiate a
    /// new underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you must
    /// pass a pointer to that table. Otherwise you must pass null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnTable(Allocator&, const Table* table, std::size_t column_ndx, ref_type spec_ref);

    /// Create a subtable column accessor and attach it to a
    /// preexisting underlying structure of arrays.
    ///
    /// \param table If this column is used as part of a table you must
    /// pass a pointer to that table. Otherwise you must pass null.
    ///
    /// \param column_ndx If this column is used as part of a table
    /// you must pass the logical index of the column within that
    /// table. Otherwise you should pass zero.
    ColumnTable(Allocator&, const Table* table, std::size_t column_ndx,
                ArrayParent*, std::size_t ndx_in_parent,
                ref_type spec_ref, ref_type column_ref);

    ~ColumnTable() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

    std::size_t get_subtable_size(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    /// The returned table pointer must always end up being wrapped in
    /// an instance of BasicTableRef.
    Table* get_subtable_ptr(std::size_t subtable_ndx) const
    {
        return ColumnSubtableParent::get_subtable_ptr(subtable_ndx, m_spec_ref);
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
    void erase(std::size_t ndx, bool is_last) TIGHTDB_OVERRIDE;
    void clear_table(std::size_t ndx);
    void fill(std::size_t count);

    void clear() TIGHTDB_OVERRIDE;

    void move_last_over(std::size_t ndx) TIGHTDB_OVERRIDE;

    /// Compare two subtable columns for equality.
    bool compare_table(const ColumnTable&) const;

    static ref_type create(std::size_t size, Allocator&);

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE; // Must be upper case to avoid conflict with macro in ObjC
    void dump_node_structure(std::ostream&, int level) const TIGHTDB_OVERRIDE;
    using ColumnSubtableParent::dump_node_structure;
    void to_dot(std::ostream&, StringData title) const TIGHTDB_OVERRIDE;
#endif

private:
    const ref_type m_spec_ref;

    void destroy_subtable(std::size_t ndx);

    void do_detach_subtable_accessors() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
};





// Implementation

inline Table* ColumnSubtableParent::get_subtable_ptr(std::size_t subtable_ndx) const
{
    TIGHTDB_ASSERT(subtable_ndx < size());

    Table* subtable = m_subtable_map.find(subtable_ndx);
    if (!subtable) {
        ref_type top_ref = get_as_ref(subtable_ndx);
        Allocator& alloc = get_alloc();
        subtable = new Table(Table::ref_count_tag(), alloc, top_ref,
                             const_cast<ColumnSubtableParent*>(this), subtable_ndx);
        bool was_empty = m_subtable_map.empty();
        m_subtable_map.add(subtable_ndx, subtable);
        if (was_empty && m_table)
            m_table->bind_ref();
    }
    return subtable;
}

inline Table* ColumnSubtableParent::get_subtable_ptr(std::size_t subtable_ndx,
                                                     ref_type spec_ref) const
{
    TIGHTDB_ASSERT(subtable_ndx < size());

    Table* subtable = m_subtable_map.find(subtable_ndx);
    if (!subtable) {
        ref_type columns_ref = get_as_ref(subtable_ndx);
        Allocator& alloc = get_alloc();
        subtable = new Table(Table::ref_count_tag(), alloc, spec_ref, columns_ref,
                             const_cast<ColumnSubtableParent*>(this), subtable_ndx);
        bool was_empty = m_subtable_map.empty();
        m_subtable_map.add(subtable_ndx, subtable);
        if (was_empty && m_table)
            m_table->bind_ref();
    }
    return subtable;
}

inline TableRef ColumnSubtableParent::get_subtable(std::size_t subtable_ndx,
                                                   ref_type spec_ref) const
{
    return TableRef(get_subtable_ptr(subtable_ndx, spec_ref));
}

inline TableRef ColumnSubtableParent::get_subtable(std::size_t subtable_ndx) const
{
    return TableRef(get_subtable_ptr(subtable_ndx));
}

inline Table* ColumnSubtableParent::SubtableMap::find(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        if (i->m_subtable_ndx == subtable_ndx)
            return i->m_table;
    return 0;
}

inline void ColumnSubtableParent::SubtableMap::add(std::size_t subtable_ndx, Table* table)
{
    entry e;
    e.m_subtable_ndx = subtable_ndx;
    e.m_table        = table;
    m_entries.push_back(e);
}

inline void ColumnSubtableParent::SubtableMap::remove(std::size_t subtable_ndx) TIGHTDB_NOEXCEPT
{
    typedef entries::iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        if (i->m_subtable_ndx == subtable_ndx) {
            m_entries.erase(i);
            return;
        }
    }
    TIGHTDB_ASSERT(false);
}

inline void ColumnSubtableParent::SubtableMap::
update_from_parent(std::size_t old_baseline) const TIGHTDB_NOEXCEPT
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        i->m_table->update_from_parent(old_baseline);
}

inline void ColumnSubtableParent::SubtableMap::detach_accessors() TIGHTDB_NOEXCEPT
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        i->m_table->detach();
    m_entries.clear();
}

inline ColumnSubtableParent::ColumnSubtableParent(Allocator& alloc,
                                                  const Table* table, std::size_t column_ndx):
    Column(Array::type_HasRefs, alloc),
    m_table(table), m_index(column_ndx)
{
}

inline ColumnSubtableParent::ColumnSubtableParent(Allocator& alloc,
                                                  const Table* table, std::size_t column_ndx,
                                                  ArrayParent* parent, std::size_t ndx_in_parent,
                                                  ref_type ref):
    Column(ref, parent, ndx_in_parent, alloc),
    m_table(table), m_index(column_ndx)
{
}

inline void ColumnSubtableParent::update_child_ref(std::size_t subtable_ndx, ref_type new_ref)
{
    set(subtable_ndx, new_ref);
}

inline ref_type ColumnSubtableParent::get_child_ref(std::size_t subtable_ndx) const TIGHTDB_NOEXCEPT
{
    return get_as_ref(subtable_ndx);
}

inline void ColumnSubtableParent::detach_subtable_accessors() TIGHTDB_NOEXCEPT
{
    bool was_empty = m_subtable_map.empty();
    m_subtable_map.detach_accessors();
    if (!was_empty && m_table)
        m_table->unbind_ref();
}

inline bool ColumnSubtableParent::compare_subtable_rows(const Table& a, const Table& b)
{
    return a.compare_rows(b);
}

inline ref_type ColumnSubtableParent::clone_table_columns(const Table* t)
{
    return t->clone_columns(m_array->get_alloc());
}

inline ref_type ColumnSubtableParent::create(std::size_t size, Allocator& alloc)
{
    Column c(Array::type_HasRefs, alloc);
    c.fill(size);
    return c.get_ref();
}

#ifdef TIGHTDB_ENABLE_REPLICATION
inline std::size_t* ColumnSubtableParent::record_subtable_path(std::size_t* begin,
                                                               std::size_t* end) TIGHTDB_NOEXCEPT
{
    if (end == begin)
        return 0; // Error, not enough space in buffer
    *begin++ = m_index;
    if (end == begin)
        return 0; // Error, not enough space in buffer
    return m_table->record_subtable_path(begin, end);
}
#endif // TIGHTDB_ENABLE_REPLICATION


inline ColumnTable::ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                                ref_type spec_ref):
    ColumnSubtableParent(alloc, table, column_ndx), m_spec_ref(spec_ref)
{
}

inline ColumnTable::ColumnTable(Allocator& alloc, const Table* table, std::size_t column_ndx,
                                ArrayParent* parent, std::size_t ndx_in_parent,
                                ref_type spec_ref, ref_type column_ref):
    ColumnSubtableParent(alloc, table, column_ndx, parent, ndx_in_parent, column_ref),
    m_spec_ref(spec_ref)
{
}

inline void ColumnTable::add(const Table* subtable)
{
    insert(size(), subtable);
}

inline ref_type ColumnTable::create(std::size_t size, Allocator& alloc)
{
    return ColumnSubtableParent::create(size, alloc);
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TABLE_HPP
