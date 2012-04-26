#ifndef __TDB_TABLE__
#define __TDB_TABLE__

#include <cstring> // strcmp()
#include <time.h>
#include "Column.hpp"
#include "ColumnString.hpp"
#include "ColumnStringEnum.hpp"
#include "ColumnBinary.hpp"
#include "alloc.hpp"
#include "ColumnType.hpp"
#include "TableRef.hpp"
#include "spec.hpp"

namespace tightdb {

class TableView;
class Group;
class ColumnTable;
class ColumnMixed;
class Table;


class Date {
public:
    Date(time_t d) : m_date(d) {}
    time_t GetDate() const {return m_date;}
private:
    time_t m_date;
};



class Mixed {
public:
    explicit Mixed(ColumnType v)  {assert(v == COLUMN_TYPE_TABLE); (void)v; m_type = COLUMN_TYPE_TABLE;}
    Mixed(bool v)        {m_type = COLUMN_TYPE_BOOL;   m_bool = v;}
    Mixed(Date v)        {m_type = COLUMN_TYPE_DATE;   m_date = v.GetDate();}
    Mixed(int64_t v)     {m_type = COLUMN_TYPE_INT;    m_int  = v;}
    Mixed(const char* v) {m_type = COLUMN_TYPE_STRING; m_str  = v;}
    Mixed(BinaryData v)  {m_type = COLUMN_TYPE_BINARY; m_str = (const char*)v.pointer; m_len = v.len;}
    Mixed(const char* v, size_t len) {m_type = COLUMN_TYPE_BINARY; m_str = v; m_len = len;}

    ColumnType GetType() const {return m_type;}

    int64_t     GetInt()    const {assert(m_type == COLUMN_TYPE_INT);    return m_int;}
    bool        GetBool()   const {assert(m_type == COLUMN_TYPE_BOOL);   return m_bool;}
    time_t      GetDate()   const {assert(m_type == COLUMN_TYPE_DATE);   return m_date;}
    const char* GetString() const {assert(m_type == COLUMN_TYPE_STRING); return m_str;}
    BinaryData  GetBinary() const {assert(m_type == COLUMN_TYPE_BINARY); BinaryData b = {m_str, m_len}; return b;}

private:
    ColumnType m_type;
    union {
        int64_t m_int;
        bool    m_bool;
        time_t  m_date;
        const char* m_str;
    };
    size_t m_len;
};




typedef BasicTableRef<Table> TableRef;
typedef BasicTableRef<const Table> ConstTableRef;


class Table {
public:
    /**
     * Construct a new top-level table with an independent schema.
     */
    Table(Allocator& alloc = GetDefaultAllocator());

    virtual ~Table();

    TableRef GetTableRef() { return TableRef(this); }
    ConstTableRef GetTableRef() const { return ConstTableRef(this); }

    // Column meta info
    size_t GetColumnCount() const;
    const char* GetColumnName(size_t ndx) const;
    size_t GetColumnIndex(const char* name) const;
    ColumnType GetColumnType(size_t ndx) const;
    Spec GetSpec();
    const Spec GetSpec() const;
    void UpdateFromSpec(size_t ref_specSet); ///< Must not be called for a table with shared schema

    bool IsEmpty() const {return m_size == 0;}
    size_t GetSize() const {return m_size;}

    size_t AddRow();
    void Clear();
    void DeleteRow(size_t ndx);
    void PopBack() {if (!IsEmpty()) DeleteRow(m_size-1);}

    // Adaptive ints
    int64_t Get(size_t column_id, size_t ndx) const;
    void Set(size_t column_id, size_t ndx, int64_t value);
    bool GetBool(size_t column_id, size_t ndx) const;
    void SetBool(size_t column_id, size_t ndx, bool value);
    time_t GetDate(size_t column_id, size_t ndx) const;
    void SetDate(size_t column_id, size_t ndx, time_t value);

    // NOTE: Low-level insert functions. Always insert in all columns at once
    // and call InsertDone after to avoid table getting un-balanced.
    void InsertInt(size_t column_id, size_t ndx, int64_t value);
    void InsertBool(size_t column_id, size_t ndx, bool value) {InsertInt(column_id, ndx, value);}
    void InsertDate(size_t column_id, size_t ndx, time_t value) {InsertInt(column_id, ndx, value);}
    template<class T> void InsertEnum(size_t column_id, size_t ndx, T value)
    {
        InsertInt(column_id, ndx, value);
    }
    void InsertString(size_t column_id, size_t ndx, const char* value);
    void InsertBinary(size_t column_id, size_t ndx, const void* value, size_t len);
    void InsertDone();

    // Strings
    const char* GetString(size_t column_id, size_t ndx) const;
    void SetString(size_t column_id, size_t ndx, const char* value);

    // Binary
    BinaryData GetBinary(size_t column_id, size_t ndx) const;
    void SetBinary(size_t column_id, size_t ndx, const void* value, size_t len);

    // Sub-tables
    TableRef GetTable(size_t column_id, size_t ndx);
    ConstTableRef GetTable(size_t column_id, size_t ndx) const;
    size_t GetTableSize(size_t column_id, size_t ndx) const;
    void   InsertTable(size_t column_id, size_t ndx);
    void   ClearTable(size_t column_id, size_t ndx);

    // Mixed
    Mixed GetMixed(size_t column_id, size_t ndx) const;
    ColumnType GetMixedType(size_t column_id, size_t ndx) const;
    void InsertMixed(size_t column_id, size_t ndx, Mixed value);
    void SetMixed(size_t column_id, size_t ndx, Mixed value);

    size_t register_column(ColumnType type, const char* name);

    Column& GetColumn(size_t ndx);
    const Column& GetColumn(size_t ndx) const;
    AdaptiveStringColumn& GetColumnString(size_t ndx);
    const AdaptiveStringColumn& GetColumnString(size_t ndx) const;
    ColumnBinary& GetColumnBinary(size_t ndx);
    const ColumnBinary& GetColumnBinary(size_t ndx) const;
    ColumnStringEnum& GetColumnStringEnum(size_t ndx);
    const ColumnStringEnum& GetColumnStringEnum(size_t ndx) const;
    ColumnTable& GetColumnTable(size_t ndx);
    const ColumnTable& GetColumnTable(size_t ndx) const;
    ColumnMixed& GetColumnMixed(size_t ndx);
    const ColumnMixed& GetColumnMixed(size_t ndx) const;

    // Searching
    size_t Find(size_t column_id, int64_t value) const;
    size_t FindBool(size_t column_id, bool value) const;
    size_t FindString(size_t column_id, const char* value) const;
    size_t FindDate(size_t column_id, time_t value) const;
    void FindAll(TableView& tv, size_t column_id, int64_t value);
    void FindAllBool(TableView& tv, size_t column_id, bool value);
    void FindAllString(TableView& tv, size_t column_id, const char *value);
    void FindAllHamming(TableView& tv, size_t column_id, uint64_t value, size_t max);

    // Indexing
    bool HasIndex(size_t column_id) const;
    void SetIndex(size_t column_id);

    // Optimizing
    void Optimize();

    // Conversion
    void to_json(std::ostream& out);

    // Debug
#ifdef _DEBUG
    bool Compare(const Table& c) const;
    void verify() const;
    void ToDot(std::ostream& out, const char* title=NULL) const;
    void Print() const;
    MemStats Stats() const;
#endif //_DEBUG

    // todo, note, these three functions have been protected
    const ColumnBase& GetColumnBase(size_t ndx) const;
    ColumnType GetRealColumnType(size_t ndx) const;

    class Parent;

protected:
    friend class Group;
    friend class ColumnMixed;

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
    Table(SubtableTag, Allocator& alloc, size_t top_ref, Parent* parent, size_t ndx_in_parent);

    /**
     * Construct a subtable with shared schema from ref.
     *
     * It is possible to construct a 'null' table by passing zero for
     * columns_ref, in this case the columns will be created on
     * demand.
     */
    Table(SubtableTag, Allocator& alloc, size_t schema_ref, size_t columns_ref,
          Parent* parent, size_t ndx_in_parent);

    void Create(size_t ref_specSet, size_t ref_columns, ArrayParent* parent, size_t ndx_in_parent);
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
    Array m_specSet;
    Array m_spec;
    ArrayString m_columnNames;
    Array m_subSpecs;
    Array m_columns;

    // Cached columns
    Array m_cols;

    /**
     * Get the subtable at the specified column and row index.
     *
     * The returned table pointer must always end up being wrapped in
     * a TableRef.
     */
    Table *get_subtable_ptr(std::size_t col_idx, std::size_t row_idx);

    /**
     * Get the subtable at the specified column and row index.
     *
     * The returned table pointer must always end up being wrapped in
     * a ConstTableRef.
     */
    const Table *get_subtable_ptr(std::size_t col_idx, std::size_t row_idx) const;

    template<class T> static BasicTableRef<T> make_ref(T* p) { return BasicTableRef<T>(p); }

private:
    Table(Table const &); // Disable copy construction
    Table &operator=(Table const &); // Disable copying assignment

    template<class> friend class BasicTableRef;
    friend class ColumnSubtableParent;

    mutable std::size_t m_ref_count;
    void bind_ref() const { ++m_ref_count; }
    void unbind_ref() const { if (--m_ref_count == 0) delete this; }

    ColumnBase& GetColumnBase(size_t ndx);
    void InstantiateBeforeChange();

    /**
     * Construct a table with independent schema and return just the
     * reference to the underlying memory.
     */
    static std::size_t create_table(Allocator&);
};



class Table::Parent: public ArrayParent {
protected:
    friend class Table;

    /**
     * Must be called whenever a child Table is destroyed.
     */
    virtual void child_destroyed(std::size_t child_ndx) = 0;
};



class TableView {
public:
    TableView(Table& source);
    TableView(const TableView& v);
    ~TableView();

    Table& GetParent() {return m_table;}
    Array& GetRefColumn() {return m_refs;}
    size_t GetRef(size_t ndx) const {return m_refs.GetAsRef(ndx);}

    bool IsEmpty() const {return m_refs.IsEmpty();}
    size_t GetSize() const {return m_refs.Size();}

    // Getting values
    int64_t Get(size_t column_id, size_t ndx) const;
    bool GetBool(size_t column_id, size_t ndx) const;
    time_t GetDate(size_t column_id, size_t ndx) const;
    const char* GetString(size_t column_id, size_t ndx) const;

    // Setting values
    void Set(size_t column_id, size_t ndx, int64_t value);
    void SetBool(size_t column_id, size_t ndx, bool value);
    void SetDate(size_t column_id, size_t ndx, time_t value);
    void SetString(size_t column_id, size_t ndx, const char* value);
    void Sort(size_t column, bool Ascending = true);
    // Sub-tables
    TableRef GetTable(size_t column_id, size_t ndx); // FIXME: Const version? Two kinds of TableView, one for const, one for non-const?

    // Deleting
    void Delete(size_t ndx);
    void Clear();

    // Finding
    size_t Find(size_t column_id, int64_t value) const;
    void FindAll(TableView& tv, size_t column_id, int64_t value);
    size_t FindString(size_t column_id, const char* value) const;
    void FindAllString(TableView& tv, size_t column_id, const char *value);

    // Aggregate functions
    int64_t Sum(size_t column_id) const;
    int64_t Max(size_t column_id) const;
    int64_t Min(size_t column_id) const;

    Table *GetTable(); // todo, temporary for tests FIXME: Is this still needed????

private:
    // Don't allow copying
    TableView& operator=(const TableView&) {return *this;}

    Table& m_table;
    Array m_refs;
};




// Implementation:

inline TableRef Table::GetTable(size_t column_id, size_t ndx)
{
    return TableRef(get_subtable_ptr(column_id, ndx));
}

inline ConstTableRef Table::GetTable(size_t column_id, size_t ndx) const
{
    return ConstTableRef(get_subtable_ptr(column_id, ndx));
}


}

#endif //__TDB_TABLE__
