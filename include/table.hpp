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
#ifndef __TIGHTDB_TABLE_H
#define __TIGHTDB_TABLE_H

#include "../src/column_fwd.hpp"
#include "../src/table_ref.hpp"
#include "../src/spec.hpp"
#include "../src/mixed.hpp"
#include "../src/table_view.hpp"

namespace tightdb {
using std::size_t;
using std::time_t;

/**
 * The Table class is non-polymorphic, that is, it has no virtual
 * functions. This is important because it ensures that there is no
 * run-time distinction between a Table instance and an instance of
 * any variation of basic_table<T>, and this, in turn, makes it valid
 * to cast a pointer from Table to basic_table<T> even when the
 * instance is constructed as a Table. Of couse, this also assumes
 * that basic_table<> is non-polymorphic, has no destructor, and adds
 * no extra data members.
 */
class Table {
public:
    // Construct a new top-level table with an independent schema.
    Table(Allocator& alloc = GetDefaultAllocator());
    ~Table();

    // Schema handling (see also Spec.hpp)
    Spec&       get_spec();          
    const Spec& get_spec() const;
    void        update_from_spec(); // Must not be called for a table with shared schema
                // Add a column dynamically
    size_t      add_column(ColumnType type, const char* name);
    
    // Table size and deletion
    bool        is_empty() const {return m_size == 0;}
    size_t      size() const {return m_size;}
    void        clear();

    // Column information
    size_t      get_column_count() const;
    const char* get_column_name(size_t column_ndx) const;
    size_t      get_column_index(const char* name) const;
    ColumnType  get_column_type(size_t column_ndx) const;

    // Row handling
    size_t      add_empty_row();
    void        remove(size_t row_ndx);
    void        remove_last() {if (!is_empty()) remove(m_size-1);}

    // Insert row
    // NOTE: You have to insert values in ALL columns followed by insert_done().
    void insert_int(size_t column_ndx, size_t row_ndx, int64_t value);
    void insert_bool(size_t column_ndx, size_t row_ndx, bool value);
    void insert_date(size_t column_ndx, size_t row_ndx, time_t value);
    template<class T> void insert_enum(size_t column_ndx, size_t row_ndx, T value);
    void insert_string(size_t column_ndx, size_t row_ndx, const char* value);
    void insert_mixed(size_t column_ndx, size_t row_ndx, Mixed value);
    void insert_binary(size_t column_ndx, size_t row_ndx, const char* value, size_t len);
    void insert_table(size_t column_ndx, size_t row_ndx);
    void insert_done();

    // Get cell values
    int64_t     get_int(size_t column_ndx, size_t row_ndx) const;
    bool        get_bool(size_t column_ndx, size_t row_ndx) const;
    time_t      get_date(size_t column_ndx, size_t row_ndx) const;
    const char* get_string(size_t column_ndx, size_t row_ndx) const;
    BinaryData  get_binary(size_t column_ndx, size_t row_ndx) const;
    Mixed       get_mixed(size_t column_ndx, size_t row_ndx) const;
    ColumnType  get_mixed_type(size_t column_ndx, size_t row_ndx) const;

    // Set cell values
    void set_int(size_t column_ndx, size_t row_ndx, int64_t value);
    void set_bool(size_t column_ndx, size_t row_ndx, bool value);
    void set_date(size_t column_ndx, size_t row_ndx, time_t value);
    void set_string(size_t column_ndx, size_t row_ndx, const char* value);
    void set_binary(size_t column_ndx, size_t row_ndx, const char* value, size_t len);
    void set_mixed(size_t column_ndx, size_t row_ndx, Mixed value);

    // Sub-tables (works both on table- and mixed columns)
    TableRef        get_subtable(size_t column_ndx, size_t row_ndx);
    ConstTableRef   get_subtable(size_t column_ndx, size_t row_ndx) const;
    size_t          get_subtable_size(size_t column_ndx, size_t row_ndx) const;
    void            clear_subtable(size_t column_ndx, size_t row_ndx);

    // Indexing
    bool has_index(size_t column_ndx) const;
    void set_index(size_t column_ndx);

    // Aggregate functions
    int64_t sum(size_t column_ndx) const;
    int64_t maximum(size_t column_ndx) const;
    int64_t minimum(size_t column_ndx) const;

    // Searching
    size_t  find_first_int(size_t column_ndx, int64_t value) const;
    size_t  find_first_bool(size_t column_ndx, bool value) const;
    size_t  find_first_date(size_t column_ndx, time_t value) const;
    size_t  find_first_string(size_t column_ndx, const char* value) const;
    void    find_all_int(TableView& tv, size_t column_ndx, int64_t value);
    void    find_all_bool(TableView& tv, size_t column_ndx, bool value);
    void    find_all_date(TableView& tv, size_t column_ndx, time_t value);
    void    find_all_string(TableView& tv, size_t column_ndx, const char *value);
    
    // Optimizing
    void optimize();

    // Conversion
    void to_json(std::ostream& out);

    // Get a reference to this table
    TableRef get_table_ref() { return TableRef(this); }
    ConstTableRef get_table_ref() const { return ConstTableRef(this); } 

    // Debug
#ifdef _DEBUG
    bool Compare(const Table& c) const;
    void verify() const;
    void ToDot(std::ostream& out, const char* title=NULL) const;
    void Print() const;
    MemStats Stats() const;
#endif //_DEBUG

    // todo, note, these three functions have been protected
    const ColumnBase& GetColumnBase(size_t column_ndx) const;
    ColumnType GetRealColumnType(size_t column_ndx) const;

    class Parent;

protected:
     // Direct Column access
    Column& GetColumn(size_t column_ndx);
    const Column& GetColumn(size_t column_ndx) const;
    AdaptiveStringColumn& GetColumnString(size_t column_ndx);
    const AdaptiveStringColumn& GetColumnString(size_t column_ndx) const;
    ColumnBinary& GetColumnBinary(size_t column_ndx);
    const ColumnBinary& GetColumnBinary(size_t column_ndx) const;
    ColumnStringEnum& GetColumnStringEnum(size_t column_ndx);
    const ColumnStringEnum& GetColumnStringEnum(size_t column_ndx) const;
    ColumnTable& GetColumnTable(size_t column_ndx);
    const ColumnTable& GetColumnTable(size_t column_ndx) const;
    ColumnMixed& GetColumnMixed(size_t column_ndx);
    const ColumnMixed& GetColumnMixed(size_t column_ndx) const;


    friend class Group;
    friend class Query;
    friend class ColumnMixed;
    friend Table* TableHelper_get_subtable_ptr(Table* t, size_t col_idx, size_t row_idx);
    friend const Table* TableHelper_get_const_subtable_ptr(const Table* t, size_t col_idx, size_t row_idx);

    /**
     * Construct a top-level table with independent schema from ref.
     */
    Table(Allocator& alloc, size_t top_ref, Parent* parent, size_t ndx_in_parent);

    /**
     * Used when constructing subtables, that is, tables whose
     * lifetime is managed by reference counting, and not by the
     * application.
     */
    class SubtableTag {};

    /**
     * Construct a subtable with independent schema from ref.
     */
    Table(SubtableTag, Allocator& alloc, size_t top_ref,
          Parent* parent, size_t ndx_in_parent);

    /**
     * Construct a subtable with shared schema from ref.
     *
     * It is possible to construct a 'null' table by passing zero for
     * columns_ref, in this case the columns will be created on
     * demand.
     */
    Table(SubtableTag, Allocator& alloc, size_t schema_ref, size_t columns_ref,
          Parent* parent, size_t ndx_in_parent);

    void Create(size_t ref_specSet, size_t ref_columns,
                ArrayParent* parent, size_t ndx_in_parent);
    void CreateColumns();
    void CacheColumns();
    void ClearCachedColumns();

    // Specification
    size_t GetColumnRefPos(size_t column_ndx) const;
    void UpdateColumnRefs(size_t column_ndx, int diff);
    void UpdateFromParent();


#ifdef _DEBUG
    void ToDotInternal(std::ostream& out) const;
#endif //_DEBUG

    // Member variables
    size_t m_size;

    // On-disk format
    Array m_top;
    Array m_columns;
    Spec m_spec_set;

    // Cached columns
    Array m_cols;

    /**
     * Get the subtable at the specified column and row index.
     *
     * The returned table pointer must always end up being wrapped in
     * a TableRef.
     */
    Table *get_subtable_ptr(size_t col_idx, size_t row_idx);

    /**
     * Get the subtable at the specified column and row index.
     *
     * The returned table pointer must always end up being wrapped in
     * a ConstTableRef.
     */
    const Table *get_subtable_ptr(size_t col_idx, size_t row_idx) const;

    template<class T> static BasicTableRef<T> make_ref(T* p) { return BasicTableRef<T>(p); }

private:
    Table(Table const &); // Disable copy construction
    Table &operator=(Table const &); // Disable copying assignment

    template<class> struct Accessors { typedef void Row; }; // FIXME: Here to support BasicTableRef::operator[], but should be eliminated.
    template<class> friend class BasicTableRef;
    friend class ColumnSubtableParent;
    friend void TableHelper_unbind(Table* t);
    friend void TableHelper_bind(Table* t);

    mutable size_t m_ref_count;
    void bind_ref() const { ++m_ref_count; }
    void unbind_ref() const { if (--m_ref_count == 0) delete this; }

    ColumnBase& GetColumnBase(size_t column_ndx);
    void InstantiateBeforeChange();

    /**
     * Construct a table with independent schema and return just the
     * reference to the underlying memory.
     */
    static size_t create_table(Allocator&);

    // Experimental
    void    find_all_hamming(TableView& tv, size_t column_ndx, uint64_t value, size_t max);
};



class Table::Parent: public ArrayParent {
protected:
    friend class Table;

    /**
     * Must be called whenever a child Table is destroyed.
     */
    virtual void child_destroyed(size_t child_ndx) = 0;
};




// Implementation:

inline void Table::insert_bool(size_t column_ndx, size_t row_ndx, bool value)
{
    insert_int(column_ndx, row_ndx, value);
}

inline void Table::insert_date(size_t column_ndx, size_t row_ndx, time_t value)
{
    insert_int(column_ndx, row_ndx, value);
}

template<class T> inline void Table::insert_enum(size_t column_ndx, size_t row_ndx, T value)
{
    insert_int(column_ndx, row_ndx, value);
}

inline TableRef Table::get_subtable(size_t column_ndx, size_t row_ndx)
{
    return TableRef(get_subtable_ptr(column_ndx, row_ndx));
}

inline ConstTableRef Table::get_subtable(size_t column_ndx, size_t row_ndx) const
{
    return ConstTableRef(get_subtable_ptr(column_ndx, row_ndx));
}


} // namespace tightdb

#endif // TIGHTDB_TABLE_H
