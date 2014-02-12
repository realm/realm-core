#include <iostream>
#include <iomanip>

#include <tightdb/column_table.hpp>

using namespace std;
using namespace tightdb;


void ColumnSubtableParent::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    if (!m_array->update_from_parent(old_baseline))
        return;
    m_subtable_map.update_from_parent(old_baseline);
}

void ColumnSubtableParent::child_accessor_destroyed(size_t child_ndx) TIGHTDB_NOEXCEPT
{
    m_subtable_map.remove(child_ndx);
    // Note that this column instance may be destroyed upon return
    // from Table::unbind_ref(), i.e., a so-called suicide is
    // possible.
    if (m_table && m_subtable_map.empty())
        _impl::TableFriend::unbind_ref(*m_table);
}

Table* ColumnSubtableParent::get_parent_table(size_t* column_ndx_out) const TIGHTDB_NOEXCEPT
{
    if (column_ndx_out)
        *column_ndx_out = m_column_ndx;
    return m_table;
}

#ifdef TIGHTDB_DEBUG

pair<ref_type, size_t> ColumnSubtableParent::get_to_dot_parent(size_t ndx_in_parent) const
{
    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_parent);
    return make_pair(p.first.m_ref, p.second);
}

#endif


size_t ColumnTable::get_subtable_size(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());

    ref_type columns_ref = get_as_ref(ndx);
    if (columns_ref == 0)
        return 0;

    typedef _impl::TableFriend tf;
    size_t subspec_ndx = get_subspec_ndx();
    Spec* spec = tf::get_spec(*m_table);
    ref_type subspec_ref = spec->get_subspec_ref(subspec_ndx);
    Allocator& alloc = spec->get_alloc();
    return tf::get_size_from_ref(subspec_ref, columns_ref, alloc);
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
    detach_subtable_accessors();

    ref_type columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable); // Throws

    Column::insert(ndx, columns_ref); // Throws
}

void ColumnTable::set(size_t ndx, const Table* subtable)
{
    TIGHTDB_ASSERT(ndx < size());
    detach_subtable_accessors();
    destroy_subtable(ndx);

    ref_type columns_ref = 0;
    if (subtable)
        columns_ref = clone_table_columns(subtable);

    Column::set(ndx, columns_ref);
}

void ColumnTable::erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < size());
    detach_subtable_accessors();
    destroy_subtable(ndx);
    Column::erase(ndx, is_last);
}

void ColumnTable::clear()
{
    detach_subtable_accessors();
    Column::clear();
    // FIXME: This one is needed because Column::clear() forgets about
    // the leaf type. A better solution should probably be found.
    m_array->set_type(Array::type_HasRefs);
}

void ColumnTable::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());
    detach_subtable_accessors();
    destroy_subtable(ndx);

    size_t last_ndx = size() - 1;
    int_fast64_t v = get(last_ndx);
    Column::set(ndx, v);

    bool is_last = true;
    Column::erase(last_ndx, is_last);
}

void ColumnTable::destroy_subtable(size_t ndx)
{
    ref_type columns_ref = get_as_ref(ndx);
    if (columns_ref == 0)
        return; // It was never created

    // Delete sub-tree
    Allocator& alloc = get_alloc();
    Array columns(columns_ref, 0, 0, alloc);
    columns.destroy();
}

bool ColumnTable::compare_table(const ColumnTable& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        ConstTableRef t1 = get_subtable_ptr(i)->get_table_ref(); // Throws
        ConstTableRef t2 = c.get_subtable_ptr(i)->get_table_ref(); // throws
        if (!compare_subtable_rows(*t1, *t2))
            return false;
    }
    return true;
}

void ColumnTable::do_detach_subtable_accessors() TIGHTDB_NOEXCEPT
{
    detach_subtable_accessors();
}


#ifdef TIGHTDB_DEBUG

void ColumnTable::Verify() const
{
    Column::Verify();

    // Verify each sub-table
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        // We want to verify any cached table accessors so we do not
        // want to skip null refs here.
        ConstTableRef subtable = get_subtable_ptr(i)->get_table_ref();
        subtable->Verify();
    }
}

void ColumnTable::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_array->get_ref();
    out << "subgraph cluster_subtable_column" << ref << " {" << endl;
    out << " label = \"Subtable column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;
    tree_to_dot(out);
    out << "}" << endl;

    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (get_as_ref(i) == 0)
            continue;
        ConstTableRef subtable = get_subtable_ptr(i)->get_table_ref();
        subtable->to_dot(out);
    }
}

namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    Array leaf(mem, 0, 0, alloc);
    int indent = level * 2;
    out << setw(indent) << "" << "Subtable leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void ColumnTable::dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
