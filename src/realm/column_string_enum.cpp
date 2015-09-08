#include <iostream>
#include <iomanip>

#include <memory>

#include <realm/column_string_enum.hpp>
#include <realm/column_string.hpp>
#include <realm/index_string.hpp>
#include <realm/table.hpp>
#include <realm/query_engine.hpp>
#include <realm/column_tpl.hpp>

using namespace realm;
using namespace realm::util;

StringEnumColumn::StringEnumColumn(Allocator& alloc, ref_type ref, ref_type keys_ref, bool nullable):
    IntegerColumn(alloc, ref), // Throws
    m_keys(alloc, keys_ref, nullable), // Throws
    m_nullable(nullable)
{
}

StringEnumColumn::~StringEnumColumn() noexcept
{
}

void StringEnumColumn::destroy() noexcept
{
    m_keys.destroy();
    IntegerColumn::destroy();
    if (m_search_index)
        m_search_index->destroy();
}

MemRef StringEnumColumn::clone_deep(Allocator& alloc) const
{
    ref_type ref = StringColumn::create(alloc); // Throws
    StringColumn new_col(alloc, ref, is_nullable()); // Throws
    // FIXME: Should be optimized with something like
    // new_col.add(seq_tree_accessor.begin(),
    // seq_tree_accessor.end())
    size_t n = size();
    for (size_t i = 0; i < n; ++i)
        new_col.add(get(i)); // Throws
    return MemRef{new_col.get_ref(), alloc};
}

void StringEnumColumn::adjust_keys_ndx_in_parent(int diff) noexcept
{
    m_keys.get_root_array()->adjust_ndx_in_parent(diff);
}

void StringEnumColumn::update_from_parent(size_t old_baseline) noexcept
{
    IntegerColumn::update_from_parent(old_baseline);
    m_keys.update_from_parent(old_baseline);
}

bool StringEnumColumn::is_nullable() const noexcept
{
    return m_nullable;
}

void StringEnumColumn::set(size_t ndx, StringData value)
{
    REALM_ASSERT_3(ndx, <, IntegerColumn::size());

    if (!is_nullable() && value.is_null()) {
        throw LogicError{LogicError::column_not_nullable};
    }

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index) {
        m_search_index->set(ndx, value);
    }

    size_t key_ndx = get_key_ndx_or_add(value);
    set_without_updating_index(ndx, key_ndx);
}


void StringEnumColumn::do_insert(size_t row_ndx, StringData value, size_t num_rows)
{
    size_t key_ndx = get_key_ndx_or_add(value);
    int64_t value_2 = int64_t(key_ndx);
    insert_without_updating_index(row_ndx, value_2, num_rows); // Throws

    if (m_search_index) {
        bool is_append = row_ndx == realm::npos;
        size_t row_ndx_2 = is_append ? size() - num_rows : row_ndx;
        m_search_index->insert(row_ndx_2, value, num_rows, is_append); // Throws
    }
}


void StringEnumColumn::do_insert(size_t row_ndx, StringData value, size_t num_rows, bool is_append)
{
    size_t key_ndx = get_key_ndx_or_add(value);
    size_t row_ndx_2 = is_append ? realm::npos : row_ndx;
    int64_t value_2 = int64_t(key_ndx);
    insert_without_updating_index(row_ndx_2, value_2, num_rows); // Throws

    if (m_search_index)
        m_search_index->insert(row_ndx, value, num_rows, is_append); // Throws
}


void StringEnumColumn::do_erase(size_t ndx, bool is_last)
{
    REALM_ASSERT_3(ndx, <, IntegerColumn::size());

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index)
        m_search_index->erase<StringData>(ndx, is_last);

    erase_without_updating_index(ndx, is_last);
}


void StringEnumColumn::do_move_last_over(size_t row_ndx, size_t last_row_ndx)
{
    REALM_ASSERT_3(row_ndx, <=, last_row_ndx);
    REALM_ASSERT_3(last_row_ndx + 1, ==, size());

    if (m_search_index) {
        // remove the value to be overwritten from index
        bool is_last = true; // This tells StringIndex::erase() to not adjust subsequent indexes
        m_search_index->erase<StringData>(row_ndx, is_last); // Throws

        // update index to point to new location
        if (row_ndx != last_row_ndx) {
            StringData moved_value = get(last_row_ndx);
            m_search_index->update_ref(moved_value, last_row_ndx, row_ndx); // Throws
        }
    }

    move_last_over_without_updating_index(row_ndx, last_row_ndx); // Throws
}


void StringEnumColumn::do_clear()
{
    // Note that clearing a StringEnum does not remove keys
    clear_without_updating_index();

    if (m_search_index)
        m_search_index->clear();
}


size_t StringEnumColumn::count(size_t key_ndx) const
{
    return IntegerColumn::count(key_ndx);
}

size_t StringEnumColumn::count(StringData value) const
{
    if (m_search_index)
        return m_search_index->count(value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == not_found)
        return 0;
    return IntegerColumn::count(key_ndx);
}


void StringEnumColumn::find_all(IntegerColumn& res, StringData value, size_t begin, size_t end) const
{
    if (m_search_index && begin == 0 && end == size_t(-1))
        return m_search_index->find_all(res, value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1))
        return;
    IntegerColumn::find_all(res, key_ndx, begin, end);
}

void StringEnumColumn::find_all(IntegerColumn& res, size_t key_ndx, size_t begin, size_t end) const
{
    if (key_ndx == size_t(-1))
        return;
    IntegerColumn::find_all(res, key_ndx, begin, end);
}

FindRes StringEnumColumn::find_all_indexref(StringData value, size_t& dst) const
{
//    REALM_ASSERT(value.m_data); fixme
    REALM_ASSERT(m_search_index);

    return m_search_index->find_all(value, dst);
}

size_t StringEnumColumn::find_first(size_t key_ndx, size_t begin, size_t end) const
{
    // Find key
    if (key_ndx == size_t(-1))
        return size_t(-1);

    return IntegerColumn::find_first(key_ndx, begin, end);
}

size_t StringEnumColumn::find_first(StringData value, size_t begin, size_t end) const
{
    if (m_search_index && begin == 0 && end == npos)
        return m_search_index->find_first(value);

    // Find key
    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1))
        return size_t(-1);

    return IntegerColumn::find_first(key_ndx, begin, end);
}

size_t StringEnumColumn::get_key_ndx(StringData value) const
{
    return m_keys.find_first(value);
}

size_t StringEnumColumn::get_key_ndx_or_add(StringData value)
{
    size_t res = m_keys.find_first(value);
    if (res != realm::not_found)
        return res;

    // Add key if it does not exist
    size_t pos = m_keys.size();
    m_keys.add(value);
    return pos;
}

bool StringEnumColumn::compare_string(const StringColumn& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}

bool StringEnumColumn::compare_string(const StringEnumColumn& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


StringIndex* StringEnumColumn::create_search_index()
{
    REALM_ASSERT(!m_search_index);

    std::unique_ptr<StringIndex> index;
    index.reset(new StringIndex(this, get_alloc())); // Throws

    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        StringData value = get(row_ndx);
        size_t num_rows = 1;
        bool is_append = true;
        index->insert(row_ndx, value, num_rows, is_append); // Throws
    }

    m_search_index = std::move(index);
    return m_search_index.get();
}

void StringEnumColumn::destroy_search_index() noexcept
{
    m_search_index.reset();
}


StringData StringEnumColumn::get_index_data(std::size_t ndx, char*) const noexcept
{
    return get(ndx);
}


void StringEnumColumn::set_search_index_allow_duplicate_values(bool allow) noexcept
{
    m_search_index->set_allow_duplicate_values(allow);
}


void StringEnumColumn::install_search_index(std::unique_ptr<StringIndex> index) noexcept
{
    REALM_ASSERT(!m_search_index);

    index->set_target(this);
    m_search_index = std::move(index); // we now own this index
}


void StringEnumColumn::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    IntegerColumn::refresh_accessor_tree(col_ndx, spec);
    size_t ndx_is_spec_enumkeys = spec.get_enumkeys_ndx(col_ndx);
    m_keys.get_root_array()->set_ndx_in_parent(ndx_is_spec_enumkeys);
    m_keys.refresh_accessor_tree(0, spec);

    // Refresh search index
    if (m_search_index) {
        size_t ndx_in_parent = get_root_array()->get_ndx_in_parent();
        m_search_index->set_ndx_in_parent(ndx_in_parent + 1);
        m_search_index->refresh_accessor_tree(col_ndx, spec); // Throws
    }
}


#ifdef REALM_DEBUG

void StringEnumColumn::verify() const
{
    m_keys.verify();
    IntegerColumn::verify();

    if (m_search_index) {
        m_search_index->verify();
        // FIXME: Verify search index contents in a way similar to what is done
        // in StringColumn::verify().
    }
}


void StringEnumColumn::verify(const Table& table, size_t col_ndx) const
{
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT_3(m_keys.get_root_array()->get_ndx_in_parent(), ==, spec.get_enumkeys_ndx(col_ndx));

    IntegerColumn::verify(table, col_ndx);

    ColumnAttr attr = spec.get_column_attr(col_ndx);
    bool has_search_index = (attr & col_attr_Indexed) != 0;
    REALM_ASSERT_3(has_search_index, ==, bool(m_search_index));
    if (has_search_index) {
        REALM_ASSERT_3(m_search_index->get_ndx_in_parent(), ==,
                       get_root_array()->get_ndx_in_parent() + 1);
    }
}


void StringEnumColumn::to_dot(std::ostream& out, StringData title) const
{
    ref_type ref = m_keys.get_ref();
    out << "subgraph cluster_string_enum_column" << ref << " {" << std::endl;
    out << " label = \"String enum column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    m_keys.to_dot(out, "keys");
    IntegerColumn::to_dot(out, "values");

    out << "}" << std::endl;
}


namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, std::ostream& out, int level)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    int indent = level * 2;
    out << std::setw(indent) << "" << "String enumeration leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void StringEnumColumn::do_dump_node_structure(std::ostream& out, int level) const
{
    get_root_array()->dump_bptree_structure(out, level, &leaf_dumper);
    int indent = level * 2;
    out << std::setw(indent) << "" << "Search index\n";
    m_search_index->do_dump_node_structure(out, level+1);
}

#endif // REALM_DEBUG
