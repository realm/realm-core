#include <iostream>
#include <iomanip>

#include <realm/column_table.hpp>

using namespace realm;
using namespace realm::util;

void SubtableColumnParent::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
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

void SubtableColumnParent::verify() const
{
    if (root_is_leaf()) {
        IntegerColumn::verify();
        REALM_ASSERT(get_root_array()->has_refs());
        return;
    }

    get_root_array()->verify_bptree(&verify_leaf);
}

void SubtableColumnParent::verify(const Table& table, size_t column_index) const
{
    IntegerColumn::verify(table, column_index);

    REALM_ASSERT(m_table == &table);
    REALM_ASSERT_3(m_column_index, ==, column_index);
}

#endif


Table* SubtableColumnParent::get_subtable_ptr(size_t subtable_index)
{
    REALM_ASSERT_3(subtable_index, <, size());
    if (Table* subtable = m_subtable_map.find(subtable_index))
        return subtable;

    typedef _impl::TableFriend tf;
    ref_type top_ref = get_as_ref(subtable_index);
    Allocator& alloc = get_alloc();
    SubtableColumnParent* parent = this;
    std::unique_ptr<Table> subtable(tf::create_accessor(alloc, top_ref, parent, subtable_index)); // Throws
    // FIXME: Note that if the following map insertion fails, then the
    // destructor of the newly created child will call
    // SubtableColumnParent::child_accessor_destroyed() with a pointer that is
    // not in the map. Fortunatly, that situation is properly handled.
    bool was_empty = m_subtable_map.empty();
    m_subtable_map.add(subtable_index, subtable.get()); // Throws
    if (was_empty && m_table)
        tf::bind_ref(*m_table);
    return subtable.release();
}


Table* SubtableColumn::get_subtable_ptr(size_t subtable_index)
{
    REALM_ASSERT_3(subtable_index, <, size());
    if (Table* subtable = m_subtable_map.find(subtable_index))
        return subtable;

    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(*m_table);
    size_t subspec_index = get_subspec_index();
    ConstSubspecRef shared_subspec = spec.get_subspec_by_index(subspec_index);
    SubtableColumn* parent = this;
    std::unique_ptr<Table> subtable(tf::create_accessor(shared_subspec, parent, subtable_index)); // Throws
    // FIXME: Note that if the following map insertion fails, then the
    // destructor of the newly created child will call
    // SubtableColumnParent::child_accessor_destroyed() with a pointer that is
    // not in the map. Fortunately, that situation is properly handled.
    bool was_empty = m_subtable_map.empty();
    m_subtable_map.add(subtable_index, subtable.get()); // Throws
    if (was_empty && m_table)
        tf::bind_ref(*m_table);
    return subtable.release();
}


void SubtableColumnParent::child_accessor_destroyed(Table* child) REALM_NOEXCEPT
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


Table* SubtableColumnParent::get_parent_table(size_t* column_index_out) REALM_NOEXCEPT
{
    if (column_index_out)
        *column_index_out = m_column_index;
    return m_table;
}


Table* SubtableColumnParent::SubtableMap::find(size_t subtable_index) const REALM_NOEXCEPT
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        if (i->m_subtable_index == subtable_index)
            return i->m_table;
    return 0;
}


bool SubtableColumnParent::SubtableMap::detach_and_remove_all() REALM_NOEXCEPT
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


bool SubtableColumnParent::SubtableMap::detach_and_remove(size_t subtable_index) REALM_NOEXCEPT
{
    typedef entries::iterator iter;
    iter i = m_entries.begin(), end = m_entries.end();
    for (;;) {
        if (i == end)
            return false;
        if (i->m_subtable_index == subtable_index)
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


bool SubtableColumnParent::SubtableMap::remove(Table* subtable) REALM_NOEXCEPT
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


void SubtableColumnParent::SubtableMap::update_from_parent(size_t old_baseline)
    const REALM_NOEXCEPT
{
    typedef _impl::TableFriend tf;
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i)
        tf::update_from_parent(*i->m_table, old_baseline);
}


void SubtableColumnParent::SubtableMap::
update_accessors(const size_t* column_path_begin, const size_t* column_path_end,
                 _impl::TableFriend::AccessorUpdater& updater)
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        // Must hold a counted reference while updating
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::update_accessors(*table, column_path_begin, column_path_end, updater);
    }
}


void SubtableColumnParent::SubtableMap::recursive_mark() REALM_NOEXCEPT
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::recursive_mark(*table);
    }
}


void SubtableColumnParent::SubtableMap::refresh_accessor_tree(size_t spec_index_in_parent)
{
    typedef entries::const_iterator iter;
    iter end = m_entries.end();
    for (iter i = m_entries.begin(); i != end; ++i) {
        // Must hold a counted reference while refreshing
        TableRef table(i->m_table);
        typedef _impl::TableFriend tf;
        tf::set_shared_subspec_index_in_parent(*table, spec_index_in_parent);
        tf::set_index_in_parent(*table, i->m_subtable_index);
        if (tf::is_marked(*table)) {
            tf::refresh_accessor_tree(*table);
            bool bump_global = false;
            tf::bump_version(*table, bump_global);
        }
    }
}


#ifdef REALM_DEBUG

std::pair<ref_type, size_t> SubtableColumnParent::get_to_dot_parent(size_t index_in_parent) const
{
    std::pair<MemRef, size_t> p = get_root_array()->get_bptree_leaf(index_in_parent);
    return std::make_pair(p.first.m_ref, p.second);
}

#endif


size_t SubtableColumn::get_subtable_size(size_t index) const REALM_NOEXCEPT
{
    REALM_ASSERT_3(index, <, size());

    ref_type columns_ref = get_as_ref(index);
    if (columns_ref == 0)
        return 0;

    typedef _impl::TableFriend tf;
    size_t subspec_index = get_subspec_index();
    Spec& spec = tf::get_spec(*m_table);
    ref_type subspec_ref = spec.get_subspec_ref(subspec_index);
    Allocator& alloc = spec.get_alloc();
    return tf::get_size_from_ref(subspec_ref, columns_ref, alloc);
}


void SubtableColumn::add(const Table* subtable)
{
    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    std::size_t row_index = realm::npos;
    int_fast64_t value = int_fast64_t(columns_ref);
    std::size_t num_rows = 1;
    do_insert(row_index, value, num_rows); // Throws
}


void SubtableColumn::insert(size_t row_index, const Table* subtable)
{
    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    std::size_t size = this->size(); // Slow
    REALM_ASSERT_3(row_index, <=, size);
    std::size_t row_index_2 = row_index == size ? realm::npos : row_index;
    int_fast64_t value = int_fast64_t(columns_ref);
    std::size_t num_rows = 1;
    do_insert(row_index_2, value, num_rows); // Throws
}


void SubtableColumn::set(size_t row_index, const Table* subtable)
{
    REALM_ASSERT_3(row_index, <, size());
    destroy_subtable(row_index);

    ref_type columns_ref = 0;
    if (subtable && !subtable->is_empty())
        columns_ref = clone_table_columns(subtable); // Throws

    int_fast64_t value = int_fast64_t(columns_ref);
    IntegerColumn::set(row_index, value); // Throws

    // Refresh the accessors, if present
    if (Table* table = m_subtable_map.find(row_index)) {
        TableRef table_2;
        table_2.reset(table); // Must hold counted reference
        typedef _impl::TableFriend tf;
        tf::discard_child_accessors(*table_2);
        tf::refresh_accessor_tree(*table_2);
        bool bump_global = false;
        tf::bump_version(*table_2, bump_global);
    }
}


void SubtableColumn::erase_rows(size_t row_index, size_t num_rows_to_erase, size_t prior_num_rows,
                             bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(num_rows_to_erase <= prior_num_rows);
    REALM_ASSERT(row_index <= prior_num_rows - num_rows_to_erase);

    for (size_t i = 0; i < num_rows_to_erase; ++i)
        destroy_subtable(row_index + i);

    SubtableColumnParent::erase_rows(row_index, num_rows_to_erase, prior_num_rows,
                                     broken_reciprocal_backlinks); // Throws
}


void SubtableColumn::move_last_row_over(size_t row_index, size_t prior_num_rows,
                                     bool broken_reciprocal_backlinks)
{
    REALM_ASSERT_DEBUG(prior_num_rows == size());
    REALM_ASSERT(row_index < prior_num_rows);

    destroy_subtable(row_index);

    SubtableColumnParent::move_last_row_over(row_index, prior_num_rows,
                                             broken_reciprocal_backlinks); // Throws
}


void SubtableColumn::destroy_subtable(size_t index) REALM_NOEXCEPT
{
    if (ref_type ref = get_as_ref(index))
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


void SubtableColumn::do_discard_child_accessors() REALM_NOEXCEPT
{
    discard_child_accessors();
}


#ifdef REALM_DEBUG

void SubtableColumn::verify(const Table& table, size_t column_index) const
{
    SubtableColumnParent::verify(table, column_index);

    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    size_t subspec_index = spec.get_subspec_index(column_index);
    if (m_subspec_index != realm::npos)
        REALM_ASSERT(m_subspec_index == realm::npos || m_subspec_index == subspec_index);

    // Verify each subtable
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
        // We want to verify any cached table accessors so we do not
        // want to skip null refs here.
        ConstTableRef subtable = get_subtable_ptr(i)->get_table_ref();
        REALM_ASSERT_3(tf::get_spec(*subtable).get_index_in_parent(), ==, subspec_index);
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
