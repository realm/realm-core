#ifndef TIGHTDB_TABLE_H
#define TIGHTDB_TABLE_H

#include "column_fwd.hpp"
#include "table_ref.hpp"
#include "spec.hpp"
#include "mixed.hpp"
#include "table_view.hpp"

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
    Spec&       GetSpec();          
    const Spec& GetSpec() const;
    void        UpdateFromSpec(); // Must not be called for a table with shared schema
                // Add a column dynamically
    size_t      register_column(ColumnType type, const char* name);
    
    // Table size and deletion
    bool        IsEmpty() const {return m_size == 0;}
    size_t      GetSize() const {return m_size;}
    void        clear();

    // Column information
    size_t      GetColumnCount() const;
    const char* GetColumnName(size_t column_ndx) const;
    size_t      GetColumnIndex(const char* name) const;
    ColumnType  GetColumnType(size_t column_ndx) const;

    // Row handling
    size_t      AddRow();
    void        erase(size_t row_ndx);
    void        pop_back() {if (!IsEmpty()) erase(m_size-1);}

    // Insert row
    // NOTE: You have to insert values in ALL columns followed by InsertDone().
    void InsertInt(size_t column_ndx, size_t row_ndx, int64_t value);
    void InsertBool(size_t column_ndx, size_t row_ndx, bool value);
    void InsertDate(size_t column_ndx, size_t row_ndx, time_t value);
    template<class T> void InsertEnum(size_t column_ndx, size_t row_ndx, T value);
    void InsertString(size_t column_ndx, size_t row_ndx, const char* value);
    void InsertMixed(size_t column_ndx, size_t row_ndx, Mixed value);
    void InsertBinary(size_t column_ndx, size_t row_ndx, const char* value, size_t len);
    void InsertTable(size_t column_ndx, size_t row_ndx);
    void InsertDone();

    // Get cell values
    int64_t     Get(size_t column_ndx, size_t row_ndx) const;
    bool        GetBool(size_t column_ndx, size_t row_ndx) const;
    time_t      GetDate(size_t column_ndx, size_t row_ndx) const;
    const char* GetString(size_t column_ndx, size_t row_ndx) const;
    BinaryData  GetBinary(size_t column_ndx, size_t row_ndx) const;
    Mixed       GetMixed(size_t column_ndx, size_t row_ndx) const;
    ColumnType  GetMixedType(size_t column_ndx, size_t row_ndx) const;

    // Set cell values
    void Set(size_t column_ndx, size_t row_ndx, int64_t value);
    void SetBool(size_t column_ndx, size_t row_ndx, bool value);
    void SetDate(size_t column_ndx, size_t row_ndx, time_t value);
    void SetString(size_t column_ndx, size_t row_ndx, const char* value);
    void SetBinary(size_t column_ndx, size_t row_ndx, const char* value, size_t len);
    void SetMixed(size_t column_ndx, size_t row_ndx, Mixed value);

    // Sub-tables (works both on table- and mixed columns)
    TableRef        GetTable(size_t column_ndx, size_t row_ndx);
    ConstTableRef   GetTable(size_t column_ndx, size_t row_ndx) const;
    size_t          GetTableSize(size_t column_ndx, size_t row_ndx) const;
    void            ClearTable(size_t column_ndx, size_t row_ndx);

    // Indexing
    bool HasIndex(size_t column_ndx) const;
    void SetIndex(size_t column_ndx);

    // Aggregate functions
    int64_t Sum(size_t column_ndx) const;
    int64_t Max(size_t column_ndx) const;
    int64_t Min(size_t column_ndx) const;

    // Searching
    size_t  Find(size_t column_ndx, int64_t value) const;
    size_t  FindBool(size_t column_ndx, bool value) const;
    size_t  FindString(size_t column_ndx, const char* value) const;
    size_t  FindDate(size_t column_ndx, time_t value) const;
    void    FindAll(TableView& tv, size_t column_ndx, int64_t value);
    void    FindAllBool(TableView& tv, size_t column_ndx, bool value);
    void    FindAllString(TableView& tv, size_t column_ndx, const char *value);
    void    FindAllHamming(TableView& tv, size_t column_ndx, uint64_t value, size_t max);

    // Optimizing
    void Optimize();

    // Conversion
    void to_json(std::ostream& out);

    // Get a reference to this table
    TableRef GetTableRef() { return TableRef(this); }
    ConstTableRef GetTableRef() const { return ConstTableRef(this); } 

// Internal / deprecate ------------------------------------

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
    friend class Group;
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

inline void Table::InsertBool(size_t column_ndx, size_t row_ndx, bool value)
{
    InsertInt(column_ndx, row_ndx, value);
}

inline void Table::InsertDate(size_t column_ndx, size_t row_ndx, time_t value)
{
    InsertInt(column_ndx, row_ndx, value);
}

template<class T> inline void Table::InsertEnum(size_t column_ndx, size_t row_ndx, T value)
{
    InsertInt(column_ndx, row_ndx, value);
}

inline TableRef Table::GetTable(size_t column_ndx, size_t row_ndx)
{
    return TableRef(get_subtable_ptr(column_ndx, row_ndx));
}

inline ConstTableRef Table::GetTable(size_t column_ndx, size_t row_ndx) const
{
    return ConstTableRef(get_subtable_ptr(column_ndx, row_ndx));
}


} // namespace tightdb

#endif // TIGHTDB_TABLE_H
