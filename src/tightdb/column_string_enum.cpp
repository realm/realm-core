#include <iostream>
#include <iomanip>

#include <tightdb/util/unique_ptr.hpp>

#include <tightdb/column_string_enum.hpp>
#include <tightdb/index_string.hpp>
#include <tightdb/table.hpp>

using namespace std;
using namespace realm;
using namespace realm::util;


namespace {

// Getter function for index. For integer index, the caller must supply a buffer that we can store the 
// extracted value in (it may be bitpacked, so we cannot return a pointer in to the Array as we do with 
// String index).
StringData get_string(void* column, size_t ndx, char*)
{
    return static_cast<ColumnStringEnum*>(column)->get(ndx);
}

} // anonymous namespace


ColumnStringEnum::ColumnStringEnum(Allocator& alloc, ref_type ref, ref_type keys_ref):
    Column(alloc, ref), // Throws
    m_keys(alloc, keys_ref), // Throws
    m_search_index(0)
{
}

ColumnStringEnum::~ColumnStringEnum() REALM_NOEXCEPT
{
    delete m_search_index;
}

void ColumnStringEnum::destroy() REALM_NOEXCEPT
{
    m_keys.destroy();
    Column::destroy();
    if (m_search_index)
        m_search_index->destroy();
}

void ColumnStringEnum::adjust_keys_ndx_in_parent(int diff) REALM_NOEXCEPT
{
    m_keys.get_root_array()->adjust_ndx_in_parent(diff);
}

void ColumnStringEnum::update_from_parent(size_t old_baseline) REALM_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);
    m_keys.update_from_parent(old_baseline);
    if (m_search_index)
        m_search_index->update_from_parent(old_baseline);
}

void ColumnStringEnum::set(size_t ndx, StringData value)
{
    REALM_ASSERT_3(ndx, <, Column::size());

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index) {
        m_search_index->set(ndx, value);
    }

    size_t key_ndx = GetKeyNdxOrAdd(value);
    Column::set(ndx, key_ndx);
}


void ColumnStringEnum::do_insert(size_t row_ndx, StringData value, size_t num_rows)
{
    size_t key_ndx = GetKeyNdxOrAdd(value);
    int64_t value_2 = int64_t(key_ndx);
    Column::do_insert(row_ndx, value_2, num_rows); // Throws

    if (m_search_index) {
        bool is_append = row_ndx == realm::npos;
        size_t row_ndx_2 = is_append ? size() - num_rows : row_ndx;
        m_search_index->insert(row_ndx_2, value, num_rows, is_append); // Throws
    }
}


void ColumnStringEnum::do_insert(size_t row_ndx, StringData value, size_t num_rows, bool is_append)
{
    size_t key_ndx = GetKeyNdxOrAdd(value);
    size_t row_ndx_2 = is_append ? realm::npos : row_ndx;
    int64_t value_2 = int64_t(key_ndx);
    Column::do_insert(row_ndx_2, value_2, num_rows); // Throws

    if (m_search_index)
        m_search_index->insert(row_ndx, value, num_rows, is_append); // Throws
}


void ColumnStringEnum::do_erase(size_t ndx, bool is_last)
{
    REALM_ASSERT_3(ndx, <, Column::size());

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index)
        m_search_index->erase<StringData>(ndx, is_last);

    Column::do_erase(ndx, is_last);
}


void ColumnStringEnum::do_move_last_over(size_t row_ndx, size_t last_row_ndx)
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

    Column::do_move_last_over(row_ndx, last_row_ndx); // Throws
}


void ColumnStringEnum::do_clear()
{
    // Note that clearing a StringEnum does not remove keys
    Column::do_clear();

    if (m_search_index)
        m_search_index->clear();
}


size_t ColumnStringEnum::count(size_t key_ndx) const
{
    return Column::count(key_ndx);
}

size_t ColumnStringEnum::count(StringData value) const
{
    if (m_search_index)
        return m_search_index->count(value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == not_found)
        return 0;
    return Column::count(key_ndx);
}


void ColumnStringEnum::find_all(Column& res, StringData value, size_t begin, size_t end) const
{
    if (m_search_index && begin == 0 && end == size_t(-1))
        return m_search_index->find_all(res, value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1))
        return;
    Column::find_all(res, key_ndx, begin, end);
}

void ColumnStringEnum::find_all(Column& res, size_t key_ndx, size_t begin, size_t end) const
{
    if (key_ndx == size_t(-1))
        return;
    Column::find_all(res, key_ndx, begin, end);
}

FindRes ColumnStringEnum::find_all_indexref(StringData value, size_t& dst) const
{
//    REALM_ASSERT(value.m_data); fixme
    REALM_ASSERT(m_search_index);

    return m_search_index->find_all(value, dst);
}

size_t ColumnStringEnum::find_first(size_t key_ndx, size_t begin, size_t end) const
{
    // Find key
    if (key_ndx == size_t(-1))
        return size_t(-1);

    return Column::find_first(key_ndx, begin, end);
}

size_t ColumnStringEnum::find_first(StringData value, size_t begin, size_t end) const
{
    if (m_search_index && begin == 0 && end == npos)
        return m_search_index->find_first(value);

    // Find key
    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1))
        return size_t(-1);

    return Column::find_first(key_ndx, begin, end);
}

size_t ColumnStringEnum::GetKeyNdx(StringData value) const
{
    return m_keys.find_first(value);
}

size_t ColumnStringEnum::GetKeyNdxOrAdd(StringData value)
{
    size_t res = m_keys.find_first(value);
    if (res != realm::not_found)
        return res;

    // Add key if it does not exist
    size_t pos = m_keys.size();
    m_keys.add(value);
    return pos;
}

bool ColumnStringEnum::compare_string(const AdaptiveStringColumn& c) const
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

bool ColumnStringEnum::compare_string(const ColumnStringEnum& c) const
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


StringIndex* ColumnStringEnum::create_search_index()
{
    REALM_ASSERT(!m_search_index);

    UniquePtr<StringIndex> index;
    index.reset(new StringIndex(this, &get_string, m_array->get_alloc())); // Throws

    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        StringData value = get(row_ndx);
        size_t num_rows = 1;
        bool is_append = true;
        index->insert(row_ndx, value, num_rows, is_append); // Throws
    }

    m_search_index = index.release();
    return m_search_index;
}


void ColumnStringEnum::destroy_search_index() REALM_NOEXCEPT
{
    delete m_search_index;
    m_search_index = 0;
}


void ColumnStringEnum::set_search_index_ref(ref_type ref, ArrayParent* parent,
                                            size_t ndx_in_parent, bool allow_duplicate_valaues)
{
    REALM_ASSERT(!m_search_index);
    m_search_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string,
                                     !allow_duplicate_valaues, m_array->get_alloc()); // Throws
}


void ColumnStringEnum::set_search_index_allow_duplicate_values(bool allow) REALM_NOEXCEPT
{
    m_search_index->set_allow_duplicate_values(allow);
}


void ColumnStringEnum::install_search_index(StringIndex* index) REALM_NOEXCEPT
{
    REALM_ASSERT(!m_search_index);

    index->set_target(this, &get_string);
    m_search_index = index; // we now own this index
}


void ColumnStringEnum::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    Column::refresh_accessor_tree(col_ndx, spec);
    size_t ndx_is_spec_enumkeys = spec.get_enumkeys_ndx(col_ndx);
    m_keys.get_root_array()->set_ndx_in_parent(ndx_is_spec_enumkeys);
    m_keys.refresh_accessor_tree(0, spec);

    // Refresh search index
    if (m_search_index) {
        size_t ndx_in_parent = m_array->get_ndx_in_parent();
        m_search_index->get_root_array()->set_ndx_in_parent(ndx_in_parent + 1);
        m_search_index->refresh_accessor_tree(col_ndx, spec); // Throws
    }
}


#ifdef REALM_DEBUG

void ColumnStringEnum::Verify() const
{
    m_keys.Verify();
    Column::Verify();

    if (m_search_index) {
        m_search_index->Verify();
        // FIXME: Verify search index contents in a way similar to what is done
        // in AdaptiveStringColumn::Verify().
    }
}


void ColumnStringEnum::Verify(const Table& table, size_t col_ndx) const
{
    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    REALM_ASSERT_3(m_keys.get_root_array()->get_ndx_in_parent(), ==, spec.get_enumkeys_ndx(col_ndx));

    Column::Verify(table, col_ndx);

    ColumnAttr attr = spec.get_column_attr(col_ndx);
    bool has_search_index = (attr & col_attr_Indexed) != 0;
    REALM_ASSERT_3(has_search_index, ==, bool(m_search_index));
    if (has_search_index) {
        REALM_ASSERT_3(m_search_index->get_root_array()->get_ndx_in_parent(), ==,
                       m_array->get_ndx_in_parent() + 1);
    }
}


void ColumnStringEnum::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_keys.get_ref();
    out << "subgraph cluster_string_enum_column" << ref << " {" << endl;
    out << " label = \"String enum column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;

    m_keys.to_dot(out, "keys");
    Column::to_dot(out, "values");

    out << "}" << endl;
}


namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    Array leaf(alloc);
    leaf.init_from_mem(mem);
    int indent = level * 2;
    out << setw(indent) << "" << "String enumeration leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void ColumnStringEnum::do_dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
    int indent = level * 2;
    out << setw(indent) << "" << "Search index\n";
    m_search_index->do_dump_node_structure(out, level+1);
}

#endif // REALM_DEBUG
