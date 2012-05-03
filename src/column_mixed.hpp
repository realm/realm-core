#ifndef __TDB_COLUMN_MIXED__
#define __TDB_COLUMN_MIXED__

#include "column.hpp"
#include "column_type.hpp"
#include "column_table.hpp"
#include "table.hpp"
#include "index.hpp"

namespace tightdb {

// Pre-declarations
class ColumnBinary;

class ColumnMixed : public ColumnBase {
public:
    /**
     * Create a freestanding mixed column.
     */
    ColumnMixed();

    /**
     * Create a mixed column and have it instantiate a new array
     * structure.
     *
     * \param tab If this column is used as part of a table you must
     * pass a pointer to that table. Otherwise you may pass null.
     */
    ColumnMixed(Allocator& alloc, const Table* tab);

    /**
     * Create a mixed column and attach it to an already existing
     * array structure.
     *
     * \param tab If this column is used as part of a table you must
     * pass a pointer to that table. Otherwise you may pass null.
     */
    ColumnMixed(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc, const Table* tab);

    ~ColumnMixed();
    void Destroy();

    void SetParent(ArrayParent* parent, size_t pndx);
    void UpdateFromParent();

    ColumnType GetType(size_t ndx) const;
    size_t Size() const {return m_types->Size();}
    bool is_empty() const {return m_types->is_empty();}

    int64_t GetInt(size_t ndx) const;
    bool GetBool(size_t ndx) const;
    time_t GetDate(size_t ndx) const;
    const char* GetString(size_t ndx) const;
    BinaryData GetBinary(size_t ndx) const;

    /**
     * The returned table pointer must always end up being wrapped in
     * an instance of BasicTableRef.
     */
    Table* get_subtable_ptr(std::size_t subtable_ndx) const;

    void SetInt(size_t ndx, int64_t value);
    void SetBool(size_t ndx, bool value);
    void SetDate(size_t ndx, time_t value);
    void SetString(size_t ndx, const char* value);
    void SetBinary(size_t ndx, const char* value, size_t len);
    void SetTable(size_t ndx);

    void InsertInt(size_t ndx, int64_t value);
    void InsertBool(size_t ndx, bool value);
    void InsertDate(size_t ndx, time_t value);
    void InsertString(size_t ndx, const char* value);
    void InsertBinary(size_t ndx, const char* value, size_t len);
    void InsertTable(size_t ndx);

    bool Add();
    void Clear();
    void Delete(size_t ndx);

    // Indexing
    bool HasIndex() const {return false;}
    void BuildIndex(Index& index) {(void)index;}
    void ClearIndex() {}

    size_t GetRef() const {return m_array->GetRef();}

#ifdef _DEBUG
    void verify() const;
    void ToDot(std::ostream& out, const char* title) const;
#endif //_DEBUG

private:
    void Create(Allocator& alloc, const Table* tab);
    void Create(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc, const Table* tab);
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
    RefsColumn(Allocator& alloc, const Table* tab):
        ColumnSubtableParent(NULL, 0, alloc, tab) {}
    RefsColumn(std::size_t ref, ArrayParent* parent, std::size_t pndx,
               Allocator& alloc, const Table* tab):
        ColumnSubtableParent(ref, parent, pndx, alloc, tab) {}
    using ColumnSubtableParent::get_subtable_ptr;
    using ColumnSubtableParent::get_subtable;
};


inline ColumnMixed::ColumnMixed(): m_data(NULL)
{
    Create(GetDefaultAllocator(), 0);
}

inline ColumnMixed::ColumnMixed(Allocator& alloc, const Table* tab): m_data(NULL)
{
    Create(alloc, tab);
}

inline ColumnMixed::ColumnMixed(size_t ref, ArrayParent* parent, size_t pndx,
                                Allocator& alloc, const Table* tab): m_data(NULL)
{
    Create(ref, parent, pndx, alloc, tab);
}

inline Table* ColumnMixed::get_subtable_ptr(size_t subtable_ndx) const
{
    assert(subtable_ndx < m_types->Size());
    assert(m_types->Get(subtable_ndx) == COLUMN_TYPE_TABLE);
    return m_refs->get_subtable_ptr(subtable_ndx);
}

}

#endif //__TDB_COLUMN_MIXED__
