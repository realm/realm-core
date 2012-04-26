#ifndef TIGHTDB_TABLE_H
#define TIGHTDB_TABLE_H

#include "Column.hpp"
#include "ColumnString.hpp"
#include "ColumnStringEnum.hpp"
#include "ColumnBinary.hpp"
#include "alloc.hpp"
#include "TableRef.hpp"
#include "spec.hpp"
#include "mixed.hpp"
#include "table_view.hpp"

namespace tightdb {


class ColumnTable;
class ColumnMixed;



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
    /**
     * Construct a new top-level table with an independent schema.
     */
    Table(Allocator& alloc = GetDefaultAllocator());

    ~Table();

    TableRef GetTableRef() { return TableRef(this); }
    ConstTableRef GetTableRef() const { return ConstTableRef(this); }

    // Column meta info
    std::size_t GetColumnCount() const;
    const char* GetColumnName(std::size_t ndx) const;
    std::size_t GetColumnIndex(const char* name) const;
    ColumnType GetColumnType(std::size_t ndx) const;

    Spec GetSpec();
    const Spec GetSpec() const;

    /// Must not be called for a table with shared schema
    void UpdateFromSpec(std::size_t ref_specSet);

    bool IsEmpty() const {return m_size == 0;}
    std::size_t GetSize() const {return m_size;}

    std::size_t AddRow();
    void Clear();
    void DeleteRow(std::size_t ndx);
    void PopBack() {if (!IsEmpty()) DeleteRow(m_size-1);}

    // Adaptive ints
    int64_t Get(std::size_t column_id, std::size_t ndx) const;
    void Set(std::size_t column_id, std::size_t ndx, int64_t value);
    bool GetBool(std::size_t column_id, std::size_t ndx) const;
    void SetBool(std::size_t column_id, std::size_t ndx, bool value);
    std::time_t GetDate(std::size_t column_id, std::size_t ndx) const;
    void SetDate(std::size_t column_id, std::size_t ndx, std::time_t value);

    // NOTE: Low-level insert functions. Always insert in all columns at once
    // and call InsertDone after to avoid table getting un-balanced.
    void InsertInt(std::size_t column_id, std::size_t ndx, int64_t value);
    void InsertBool(std::size_t column_id, std::size_t ndx, bool value);
    void InsertDate(std::size_t column_id, std::size_t ndx, std::time_t value);
    template<class T> void InsertEnum(std::size_t column_id, std::size_t ndx, T value);
    void InsertString(std::size_t column_id, std::size_t ndx, const char* value);
    void InsertBinary(std::size_t column_id, std::size_t ndx, const char* value, std::size_t len);
    void InsertDone();

    // Strings
    const char* GetString(std::size_t column_id, std::size_t ndx) const;
    void SetString(std::size_t column_id, std::size_t ndx, const char* value);

    // Binary
    BinaryData GetBinary(std::size_t column_id, std::size_t ndx) const;
    void SetBinary(std::size_t column_id, std::size_t ndx, const char* value, std::size_t len);

    // Sub-tables
    TableRef GetTable(std::size_t column_id, std::size_t ndx);
    ConstTableRef GetTable(std::size_t column_id, std::size_t ndx) const;
    std::size_t GetTableSize(std::size_t column_id, std::size_t ndx) const;
    void   InsertTable(std::size_t column_id, std::size_t ndx);
    void   ClearTable(std::size_t column_id, std::size_t ndx);

    // Mixed
    Mixed GetMixed(std::size_t column_id, std::size_t ndx) const;
    ColumnType GetMixedType(std::size_t column_id, std::size_t ndx) const;
    void InsertMixed(std::size_t column_id, std::size_t ndx, Mixed value);
    void SetMixed(std::size_t column_id, std::size_t ndx, Mixed value);

    std::size_t register_column(ColumnType type, const char* name);

    Column& GetColumn(std::size_t ndx);
    const Column& GetColumn(std::size_t ndx) const;
    AdaptiveStringColumn& GetColumnString(std::size_t ndx);
    const AdaptiveStringColumn& GetColumnString(std::size_t ndx) const;
    ColumnBinary& GetColumnBinary(std::size_t ndx);
    const ColumnBinary& GetColumnBinary(std::size_t ndx) const;
    ColumnStringEnum& GetColumnStringEnum(std::size_t ndx);
    const ColumnStringEnum& GetColumnStringEnum(std::size_t ndx) const;
    ColumnTable& GetColumnTable(std::size_t ndx);
    const ColumnTable& GetColumnTable(std::size_t ndx) const;
    ColumnMixed& GetColumnMixed(std::size_t ndx);
    const ColumnMixed& GetColumnMixed(std::size_t ndx) const;

    // Searching
    std::size_t Find(std::size_t column_id, int64_t value) const;
    std::size_t FindBool(std::size_t column_id, bool value) const;
    std::size_t FindString(std::size_t column_id, const char* value) const;
    std::size_t FindDate(std::size_t column_id, std::time_t value) const;
    void FindAll(TableView& tv, std::size_t column_id, int64_t value);
    void FindAllBool(TableView& tv, std::size_t column_id, bool value);
    void FindAllString(TableView& tv, std::size_t column_id, const char *value);
    void FindAllHamming(TableView& tv, std::size_t column_id, uint64_t value, std::size_t max);

    // Indexing
    bool HasIndex(std::size_t column_id) const;
    void SetIndex(std::size_t column_id);

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
    const ColumnBase& GetColumnBase(std::size_t ndx) const;
    ColumnType GetRealColumnType(std::size_t ndx) const;

    class Parent;

protected:
    friend class Group;
    friend class ColumnMixed;

    /**
     * Construct a top-level table with independent schema from ref.
     */
    Table(Allocator& alloc, std::size_t top_ref, Parent* parent, std::size_t ndx_in_parent);

    /**
     * Used when constructing subtables, that is, tables whose
     * lifetime is managed by reference counting, and not by the
     * application.
     */
    class SubtableTag {};

    /**
     * Construct a subtable with independent schema from ref.
     */
    Table(SubtableTag, Allocator& alloc, std::size_t top_ref,
          Parent* parent, std::size_t ndx_in_parent);

    /**
     * Construct a subtable with shared schema from ref.
     *
     * It is possible to construct a 'null' table by passing zero for
     * columns_ref, in this case the columns will be created on
     * demand.
     */
    Table(SubtableTag, Allocator& alloc, std::size_t schema_ref, std::size_t columns_ref,
          Parent* parent, std::size_t ndx_in_parent);

    void Create(std::size_t ref_specSet, std::size_t ref_columns,
                ArrayParent* parent, std::size_t ndx_in_parent);
    void CreateColumns();
    void CacheColumns();
    void ClearCachedColumns();

    // Specification
    std::size_t GetColumnRefPos(std::size_t column_ndx) const;
    void UpdateColumnRefs(std::size_t column_ndx, int diff);
    void UpdateFromParent();


#ifdef _DEBUG
    void ToDotInternal(std::ostream& out) const;
#endif //_DEBUG

    // Member variables
    std::size_t m_size;

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

    ColumnBase& GetColumnBase(std::size_t ndx);
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




// Implementation:

inline void Table::InsertBool(std::size_t column_id, std::size_t ndx, bool value)
{
    InsertInt(column_id, ndx, value);
}

inline void Table::InsertDate(std::size_t column_id, std::size_t ndx, std::time_t value)
{
    InsertInt(column_id, ndx, value);
}

template<class T> inline void Table::InsertEnum(std::size_t column_id, std::size_t ndx, T value)
{
    InsertInt(column_id, ndx, value);
}

inline TableRef Table::GetTable(std::size_t column_id, std::size_t ndx)
{
    return TableRef(get_subtable_ptr(column_id, ndx));
}

inline ConstTableRef Table::GetTable(std::size_t column_id, std::size_t ndx) const
{
    return ConstTableRef(get_subtable_ptr(column_id, ndx));
}


} // namespace tightdb

#endif // TIGHTDB_TABLE_H
