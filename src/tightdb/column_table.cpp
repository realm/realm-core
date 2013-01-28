#include <tightdb/column_table.hpp>

using namespace std;

namespace tightdb {

void ColumnSubtableParent::child_destroyed(size_t subtable_ndx)
{
    m_subtable_map.remove(subtable_ndx);
    // Note that this column instance may be destroyed upon return
    // from Table::unbind_ref().
    if (m_table && m_subtable_map.empty()) m_table->unbind_ref();
}

bool ColumnTable::has_subtable(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());

    const size_t ref_columns = GetAsRef(ndx);
    return (ref_columns != 0);
}

size_t ColumnTable::get_subtable_size(size_t ndx) const TIGHTDB_NOEXCEPT
{
    // FIXME: If the table object is cached, it is possible to get the
    // size from it. Maybe it is faster in general to check for the
    // presence of the cached object and use it when available.
    TIGHTDB_ASSERT(ndx < Size());

    const size_t ref_columns = GetAsRef(ndx);
    if (ref_columns == 0) return 0;

    const size_t ref_first_col = Array(ref_columns, 0, 0, GetAllocator()).GetAsRef(0);
    return get_size_from_ref(ref_first_col, GetAllocator());
}

bool ColumnTable::add()
{
    Insert(Size()); // zero-ref indicates empty table
    return true;
}

void ColumnTable::Insert(size_t ndx)
{
    TIGHTDB_ASSERT(ndx <= Size());

    // zero-ref indicates empty table
    Column::Insert(ndx, 0);
}

void ColumnTable::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<int64_t, Column>(i, 0); // zero-ref indicates empty table
    }
}

void ColumnTable::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());

    const size_t ref_columns = GetAsRef(ndx);

    // Delete sub-tree
    if (ref_columns != 0) {
        Allocator& alloc = GetAllocator();
        Array columns(ref_columns, (Array*)NULL, 0, alloc);
        columns.Destroy();
    }

    Column::Delete(ndx);

    invalidate_subtables();
}

void ColumnTable::ClearTable(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());

    const size_t ref_columns = GetAsRef(ndx);
    if (ref_columns == 0) return; // already empty

    // Delete sub-tree
    Allocator& alloc = GetAllocator();
    Array columns(ref_columns, (Array*)NULL, 0, alloc);
    columns.Destroy();

    // Mark as empty table
    Set(ndx, 0);
}

bool ColumnTable::Compare(const ColumnTable& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        ConstTableRef t1 = get_subtable_ptr(i)->get_table_ref();
        ConstTableRef t2 = c.get_subtable_ptr(i)->get_table_ref();
        if (!compare_subtable_rows(*t1, *t2)) return false;
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void ColumnTable::Verify() const
{
    Column::Verify();

    // Verify each sub-table
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        // We want to verify any cached table instance so we do not
        // want to skip null refs here.
        const ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->Verify();
    }
}

void ColumnTable::LeafToDot(std::ostream& out, const Array& array) const
{
    array.ToDot(out);

    const size_t count = array.Size();
    for (size_t i = 0; i < count; ++i) {
        if (array.GetAsRef(i) == 0) continue;
        const ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->to_dot(out);
    }
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
