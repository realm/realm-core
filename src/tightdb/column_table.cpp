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

void ColumnSubtableParent::move_last_over(size_t ndx) {
    TIGHTDB_ASSERT(ndx+1 < size());

    // Delete sub-tree
    const size_t ref_columns = get_as_ref(ndx);
    if (ref_columns != 0) {
        Allocator& alloc = get_alloc();
        Array columns(ref_columns, NULL, 0, alloc);
        columns.destroy();
    }

    const size_t ndx_last = size()-1;
    const int64_t v = get(ndx_last);

    set(ndx, v);

    // We do a Column::erase() to avoid
    // recursive delete of the copied table(s)
    Column::erase(ndx_last);
}

bool ColumnTable::has_subtable(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < size());

    const size_t ref_columns = get_as_ref(ndx);
    return (ref_columns != 0);
}

size_t ColumnTable::get_subtable_size(size_t ndx) const TIGHTDB_NOEXCEPT
{
    // FIXME: If the table object is cached, it is possible to get the
    // size from it. Maybe it is faster in general to check for the
    // presence of the cached object and use it when available.
    TIGHTDB_ASSERT(ndx < size());

    const size_t ref_columns = get_as_ref(ndx);
    if (ref_columns == 0) return 0;

    const size_t ref_first_col = Array(ref_columns, 0, 0, get_alloc()).get_as_ref(0);
    return get_size_from_ref(ref_first_col, get_alloc());
}

void ColumnTable::add()
{
    invalidate_subtables();
    add(0); // Null-pointer indicates empty table
}

void ColumnTable::insert(size_t ndx)
{
    invalidate_subtables();
    insert(ndx, 0); // Null-pointer indicates empty table
}

void ColumnTable::insert(size_t ndx, const Table* subtable)
{
    TIGHTDB_ASSERT(ndx <= size());

    size_t columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable);

    Column::insert(ndx, columns_ref);
}

void ColumnTable::set(size_t ndx, const Table* subtable)
{
    TIGHTDB_ASSERT(ndx < size());

    size_t columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable);

    Column::set(ndx, columns_ref);
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

void ColumnTable::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());

    const size_t ref_columns = get_as_ref(ndx);

    // Delete sub-tree
    if (ref_columns != 0) {
        Allocator& alloc = get_alloc();
        Array columns(ref_columns, 0, 0, alloc);
        columns.destroy();
    }

    Column::erase(ndx);

    invalidate_subtables();
}

void ColumnTable::clear_table(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());

    const size_t ref_columns = get_as_ref(ndx);
    if (ref_columns == 0) return; // already empty

    // Delete sub-tree
    Allocator& alloc = get_alloc();
    Array columns(ref_columns, 0, 0, alloc);
    columns.destroy();

    // Mark as empty table
    set(ndx, 0);
}

bool ColumnTable::compare(const ColumnTable& c) const
{
    const size_t n = size();
    if (c.size() != n) return false;
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
    const size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        // We want to verify any cached table instance so we do not
        // want to skip null refs here.
        const ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->Verify();
    }
}

void ColumnTable::LeafToDot(ostream& out, const Array& array) const
{
    array.ToDot(out);

    const size_t count = array.size();
    for (size_t i = 0; i < count; ++i) {
        if (array.get_as_ref(i) == 0) continue;
        const ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->to_dot(out);
    }
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
