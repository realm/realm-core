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

size_t ColumnTable::get_subtable_size(size_t ndx) const TIGHTDB_NOEXCEPT
{
    // FIXME: If the table object is cached, it is possible to get the
    // size from it. Maybe it is faster in general to check for the
    // presence of the cached object and use it when available.
    TIGHTDB_ASSERT(ndx < size());

    ref_type columns_ref = get_as_ref(ndx);
    if (columns_ref == 0) return 0;

    ref_type first_col_ref = Array(columns_ref, 0, 0, get_alloc()).get_as_ref(0);
    return get_size_from_ref(first_col_ref, get_alloc());
}

void ColumnTable::add()
{
    add(0); // Null-pointer indicates empty table
}

void ColumnTable::insert(size_t ndx)
{
    insert(ndx, 0); // Null-pointer indicates empty table
}

void ColumnTable::insert(size_t ndx, const Table* subtable)
{
    TIGHTDB_ASSERT(ndx <= size());
    invalidate_subtables(); // FIXME: Rename to detach_subtable_accessors().

    ref_type columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable);

    Column::insert(ndx, columns_ref);
}

void ColumnTable::set(size_t ndx, const Table* subtable)
{
    TIGHTDB_ASSERT(ndx < size());
    invalidate_subtables(); // FIXME: Rename to detach_subtable_accessors().
    destroy_subtable(ndx);

    ref_type columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable);

    Column::set(ndx, columns_ref);
}

void ColumnTable::fill(size_t n)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < n; ++i) {
        add(0); // zero-ref indicates empty table
    }
}

void ColumnTable::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());
    invalidate_subtables(); // FIXME: Rename to detach_subtable_accessors().
    destroy_subtable(ndx);
    Column::erase(ndx);
}

void ColumnTable::clear()
{
    invalidate_subtables();
    Column::clear();
    // FIXME: This one is needed because Column::clear() forgets about
    // the leaf type. A better solution should probably be found.
    m_array->set_type(Array::type_HasRefs);
}

void ColumnTable::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());
    invalidate_subtables(); // FIXME: Rename to detach_subtable_accessors().
    destroy_subtable(ndx);

    size_t ndx_last = size()-1;
    int64_t v = get(ndx_last);
    Column::set(ndx, v);
    Column::erase(ndx_last);
}

void ColumnTable::destroy_subtable(size_t ndx)
{
    ref_type ref_columns = get_as_ref(ndx);
    if (ref_columns == 0) return; // It was never created

    // Delete sub-tree
    Allocator& alloc = get_alloc();
    Array columns(ref_columns, 0, 0, alloc);
    columns.destroy();
}

bool ColumnTable::compare_table(const ColumnTable& c) const
{
    size_t n = size();
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
    size_t n = size();
    for (size_t i = 0; i < n; ++i) {
        // We want to verify any cached table instance so we do not
        // want to skip null refs here.
        ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->Verify();
    }
}

void ColumnTable::leaf_to_dot(ostream& out, const Array& array) const
{
    array.to_dot(out);

    size_t n = array.size();
    for (size_t i = 0; i < n; ++i) {
        if (array.get_as_ref(i) == 0)
            continue;
        ConstTableRef subtable = get_subtable(i, m_ref_specSet);
        subtable->to_dot(out);
    }
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
