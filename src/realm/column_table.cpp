#include <iostream>
#include <iomanip>

#include <realm/column_table.hpp>

using namespace realm;
using namespace realm::util;

void SubtableColumnBase::update_from_parent(size_t old_baseline) noexcept
{
    IntegerColumn::update_from_parent(old_baseline);
    m_subtable_map.update_from_parent(old_baseline);
}


#ifdef REALM_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.verify();
    REALM_ASSERT(leaf.has_refs());
    return leaf.size();
}

} // anonymous namespace

void SubtableColumnBase::verify() const
{
    if (root_is_leaf()) {
        IntegerColumn::verify();
        REALM_ASSERT(get_root_array()->has_refs());
        return;
    }

    get_root_array()->verify_bptree(&verify_leaf);
}

void SubtableColumnBase::verify(const Table& table, size_t col_ndx) const
{
    IntegerColumn::verify(table, col_ndx);

    REALM_ASSERT(m_table == &table);
    REALM_ASSERT_3(m_column_ndx, ==, col_ndx);
}

#endif


Table* SubtableColumnBase::get_subtable_ptr(size_t subtable_ndx)
{
    REALM_ASSERT_3(subtable_ndx, <, size());
    if (Table* subtable = m_subtable_map.find(subtable_ndx))
        return subtable;

    typedef _impl::TableFriend tf;
    ref_type top_ref = get_as_ref(subtable_ndx);
    Allocator& alloc = get_alloc();
    SubtableColumnBase* parent = this;
    std::unique_ptr<Table> subtable(tf::create_accessor(alloc, top_ref, parent, subtable_ndx)); // Throws
    // FIXME: Note that if the following map insertion fails, then the
    // destructor of the newly created child will call
    // SubtableColumnBase::child_accessor_destroyed() with a pointer that is not
    // in the map. Fortunatly, that situation is properly handled.
    bool was_empty = m_subtable_map.empty();
    m_subtable_map.add(subtable_ndx, subtable.get()); // Throws
    if (was_empty && m_table)
        tf::bind_ref(*m_table);
    return subtable.release();
}


Table* SubtableColumn::get_subtable_ptr(size_t subtable_ndx)
{
    REALM_ASSERT_3(subtable_ndx, <, size());
    if (Table* subtable = m_subtable_map.find(subtable_ndx))
        return subtable;

    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(*m_table);
    size_t subspec_ndx = get_subspec_ndx();
    ConstSubspecRef shared_subspec = spec.get_subspec_by_ndx(subspec_ndx);
    SubtableColumn* parent = this;
    std::unique_ptr<Table> subtable(tf::create_accessor(shared_subspec, parent, subtable_ndx)); // Throws
    // FIXME: Note that if the following map insertion fails, then the
    // destructor of the newly created child will call
    // SubtableColumnBase::child_accessor_destroyed() with a pointer that is not
    // in the map. Fortunately, that situation is properly handled.
    bool was_empty = m_subtable_map.empty();
    m_subtable_map.add(subtable_ndx, subtable.get()); // Throws
    if (was_empty && m_table)
        tf::bind_ref(*m_table);
    return subtable.release();
}


void SubtableColumnBase::child_accessor_destroyed(Table* child) noexcept
{
    // This function must assume no more than minimal consistency of the
    // accessor hierarchy. This means in particular that it cannot access the
    // underlying node structure. See AccessorConsistencyLevels.

    // Note that due to the possibility of a failure during child creation, it
    // is possible that the calling child is not in the map.

    bool last_entry_removed = m_subtable_map.remove(child);

    // Note that this column instance may be destroyed upon return
    // from Table::unbind_ref(), i.e., a so-called suicide is
    // possible.
    typedef _impl::TableFriend tf;
    if (last_entry_removed && m_table)
        tf::unbind_ref(*m_table);
}


Table* SubtableColumnBase::get_parent_table(size_t* column_ndx_out) noexcept
{
    if (column_ndx_out)
        *column_ndx_out = m_column_ndx;
    return m_table;
}


Table* SubtableColumnBase::SubtableMap::find(size_t subtable_ndx) const noexcept
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        if (i->m_subtable_ndx == subtable_ndx)
            return i->m_table;
    return 0;
}


bool SubtableColumnBase::SubtableMap::detach_and_remove_all() noexcept
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        // Must hold a counted reference while detaching
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::detach(*table);
    }
    bool was_empty = m_entries.empty();
    m_entries.clear();
    return !was_empty;
}


bool SubtableColumnBase::SubtableMap::detach_and_remove(size_t subtable_ndx) noexcept
{
    typedef entries::iterator iter;
    iter i = m_entries.begin(), end = m_entries.end();
    for (;;) {
        if (i == end)
            return false;
        if (i->m_subtable_ndx == subtable_ndx)
            break;
        ++i;
    }

    // Must hold a counted reference while detaching
    TableRef table(i->m_table);
    typedef _impl::TableFriend tf;
    tf::detach(*table);

    *i = *--end; // Move last over
    m_entries.pop_back();
    return m_entries.empty();
}


bool SubtableColumnBase::SubtableMap::remove(Table* subtable) noexcept
{
    typedef entries::iterator iter;
    iter i = m_entries.begin(), end = m_entries.end();
    for (;;) {
        if (i == end)
            return false;
        if (i->m_table == subtable)
            break;
        ++i;
    }
    *i = *--end; // Move last over
    m_entries.pop_back();
    return m_entries.empty();
}


void SubtableColumnBase::SubtableMap::update_from_parent(size_t old_baseline)
    const noexcept
{
    typedef _impl::TableFriend tf;
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        tf::update_from_parent(*i->m_table, old_baseline);
}


void SubtableColumnBase::SubtableMap::
update_accessors(const size_t* col_path_begin, const size_t* col_path_end,
                 _impl::TableFriend::AccessorUpdater& updater)
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        // Must hold a counted reference while updating
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::update_accessors(*table, col_path_begin, col_path_end, updater);
    }
}


void SubtableColumnBase::SubtableMap::recursive_mark() noexcept
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::recursive_mark(*table);
    }
}


void SubtableColumnBase::SubtableMap::refresh_accessor_tree(size_t spec_ndx_in_parent)
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        // Must hold a counted reference while refreshing
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::set_shared_subspec_ndx_in_parent(*table, spec_ndx_in_parent);
        tf::set_ndx_in_parent(*table, i->m_subtable_ndx);
        if (tf::is_marked(*table)) {
            tf::refresh_accessor_tree(*table);
            bool bump_global = false;
            tf::bump_version(*table, bump_global);
        }
    }
}


#ifdef REALM_DEBUG

std::pair<ref_type, size_t> SubtableColumnBase::get_to_dot_parent(size_t ndx_in_parent) const
{
    std::pair<MemRef, size_t> p = get_root_array()->get_bptree_leaf(ndx_in_parent);
    return std::make_pair(p.first.m_ref, p.second);
}

#endif


size_t SubtableColumn::get_subtable_size(size_t ndx) const noexcept
{
    REALM_ASSERT_3(ndx, <, size());

    ref_type columns_ref = get_as_ref(ndx);
    if (columns_ref == 0)
        return 0;

    typedef _impl::TableFriend tf;
    size_t subspec_ndx = get_subspec_ndx();
    Spec& spec = tf::get_spec(*m_table);
    ref_type subspec_ref = spec.get_subspec_ref(subspec_ndx);
    Allocator& alloc = spec.get_alloc();
    return tf::get_size_from_ref(subspec_ref, columns_ref, alloc);
}


void SubtableColumn::add(const Table* subtable)
{
    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    size_t row_ndx = realm::npos;
    int_fast64_t value = int_fast64_t(columns_ref);
    size_t num_rows = 1;
    do_insert(row_ndx, value, num_rows); // Throws
}


void SubtableColumn::insert(size_t row_ndx, const Table* subtable)
{
    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    size_t size = this->size(); // Slow
    REALM_ASSERT_3(row_ndx, <=, size);
    size_t row_ndx_2 = row_ndx == size ? realm::npos : row_ndx;
    int_fast64_t value = int_fast64_t(columns_ref);
    size_t num_rows = 1;
    do_insert(row_ndx_2, value, num_rows); // Throws
}


void SubtableColumn::set(size_t row_ndx, const Table* subtable)
{
    REALM_ASSERT_3(row_ndx, <, size());
    destroy_subtable(row_ndx);

    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    int_fast64_t value = int_fast64_t(columns_ref);
    IntegerColumn::set(row_ndx, value); // Throws

    // Refresh the accessors, if present
    if (Table* table = m_subtable_map.find(row_ndx)) {
        TableRef table_2;
        table_2.reset(table); // Must hold counted reference
        typedef _impl::TableFriend tf;
        tf::discard_child_accessors(*table_2);
        tf::refresh_accessor_tree(*table_2);
        bool bump_global = false;
        tf::bump_version(*table_2, bump_global);
    }
}


void SubtableColumn::erase_rows(size_t row_ndx, size_t num_rows_to_erase, size_t prior_num_rows,
                             bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_ndx <= prior_num_rows - num_rows_to_erase);

    for (size_t i = 0; i < num_rows_to_erase; ++i)
        destroy_subtable(row_ndx + i);

    SubtableColumnBase::erase_rows(row_ndx, num_rows_to_erase, prior_num_rows,
                                   broken_reciprocal_backlinks); // Throws
}


void SubtableColumn::move_last_row_over(size_t row_ndx, size_t prior_num_rows,
                                     bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_ndx < prior_num_rows);

    destroy_subtable(row_ndx);

    SubtableColumnBase::move_last_row_over(row_ndx, prior_num_rows,
                                           broken_reciprocal_backlinks); // Throws
}


void SubtableColumn::destroy_subtable(size_t ndx) noexcept
{
    if (ref_type ref = get_as_ref(ndx))
        Array::destroy_deep(ref, get_alloc());
}


bool SubtableColumn::compare_table(const SubtableColumn& c) const
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


void SubtableColumn::do_discard_child_accessors() noexcept
{
    discard_child_accessors();
}


#ifdef REALM_DEBUG

void SubtableColumn::verify(const Table& table, size_t col_ndx) const
{
    SubtableColumnBase::verify(table, col_ndx);

    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    size_t subspec_ndx = spec.get_subspec_ndx(col_ndx);
    if (m_subspec_ndx != realm::npos)
        REALM_ASSERT(m_subspec_ndx == realm::npos || m_subspec_ndx == subspec_ndx);

    // Verify each subtable
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        // We want to verify any cached table accessors so we do not
        // want to skip null refs here.
        ConstTableRef subtable = get_subtable_ptr(i)->get_table_ref();
        REALM_ASSERT_3(tf::get_spec(*subtable).get_ndx_in_parent(), ==, subspec_ndx);
        REALM_ASSERT_3(subtable->get_parent_row_index(), ==, i);
        subtable->verify();
    }
}

void SubtableColumn::to_dot(std::ostream& out, StringData title) const
{
    ref_type ref = get_root_array()->get_ref();
    out << "subgraph cluster_subtable_column" << ref << " {" << std::endl;
    out << " label = \"Subtable column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;
    tree_to_dot(out);
    out << "}" << std::endl;

    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        if (get_as_ref(i) == 0)
            continue;
        ConstTableRef subtable = get_subtable_ptr(i)->get_table_ref();
        subtable->to_dot(out);
    }
}

namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, std::ostream& out, int level)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    int indent = level * 2;
    out << std::setw(indent) << "" << "Subtable leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void SubtableColumn::do_dump_node_structure(std::ostream& out, int level) const
{
    get_root_array()->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // REALM_DEBUG
