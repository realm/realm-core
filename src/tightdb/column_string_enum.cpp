#include <iostream>
#include <iomanip>

#include <tightdb/column_string_enum.hpp>
#include <tightdb/index_string.hpp>

using namespace std;
using namespace tightdb;


namespace {

// Getter function for string index
StringData get_string(void* column, size_t ndx)
{
    return static_cast<ColumnStringEnum*>(column)->get(ndx);
}

} // anonymous namespace


ColumnStringEnum::ColumnStringEnum(ref_type keys, ref_type values, ArrayParent* column_parent,
                                   size_t column_ndx_in_parent, ArrayParent* keys_parent,
                                   size_t keys_ndx_in_parent, Allocator& alloc):
    Column(values, column_parent, column_ndx_in_parent, alloc), // Throws
    m_keys(keys,   keys_parent,   keys_ndx_in_parent,   alloc), // Throws
    m_index(0)
{
}

ColumnStringEnum::~ColumnStringEnum() TIGHTDB_NOEXCEPT
{
    delete m_index;
}

void ColumnStringEnum::destroy() TIGHTDB_NOEXCEPT
{
    m_keys.destroy();
    Column::destroy();
    if (m_index)
        m_index->destroy();
}

void ColumnStringEnum::adjust_keys_ndx_in_parent(int diff) TIGHTDB_NOEXCEPT
{
    m_keys.get_root_array()->adjust_ndx_in_parent(diff);
}

void ColumnStringEnum::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);
    m_keys.update_from_parent(old_baseline);
}

void ColumnStringEnum::set(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < Column::size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        m_index->set(ndx, oldVal, value);
    }

    size_t key_ndx = GetKeyNdxOrAdd(value);
    Column::set(ndx, key_ndx);
}


void ColumnStringEnum::do_insert(size_t row_ndx, StringData value, size_t num_rows)
{
    size_t key_ndx = GetKeyNdxOrAdd(value);
    int_fast64_t value_2 = int_fast64_t(key_ndx);
    Column::do_insert(row_ndx, value_2, num_rows); // Throws

    if (m_index) {
        bool is_append = row_ndx == tightdb::npos;
        size_t row_ndx_2 = is_append ? size() - num_rows : row_ndx;
        m_index->insert(row_ndx_2, value, num_rows, is_append); // Throws
    }
}


void ColumnStringEnum::do_insert(size_t row_ndx, StringData value, size_t num_rows, bool is_append)
{
    size_t key_ndx = GetKeyNdxOrAdd(value);
    size_t row_ndx_2 = is_append ? tightdb::npos : row_ndx;
    int_fast64_t value_2 = int_fast64_t(key_ndx);
    Column::do_insert(row_ndx_2, value_2, num_rows); // Throws

    if (m_index)
        m_index->insert(row_ndx, value, num_rows, is_append); // Throws
}


void ColumnStringEnum::erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < Column::size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData old_val = get(ndx);
        m_index->erase(ndx, old_val, is_last);
    }

    Column::erase(ndx, is_last);
}

void ColumnStringEnum::clear()
{
    // Note that clearing a StringEnum does not remove keys
    Column::clear();

    if (m_index)
        m_index->clear();
}

size_t ColumnStringEnum::count(size_t key_ndx) const
{
    return Column::count(key_ndx);
}

size_t ColumnStringEnum::count(StringData value) const
{
    if (m_index)
        return m_index->count(value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == not_found)
        return 0;
    return Column::count(key_ndx);
}

void ColumnStringEnum::find_all(Column& res, StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1))
        return m_index->find_all(res, value);

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
//    TIGHTDB_ASSERT(value.m_data); fixme
    TIGHTDB_ASSERT(m_index);

    return m_index->find_all(value, dst);
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
    if (m_index && begin == 0 && end == size_t(-1))
        return m_index->find_first(value);

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
    if (res != tightdb::not_found)
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


StringIndex& ColumnStringEnum::create_index()
{
    TIGHTDB_ASSERT(!m_index);

    // Create new index
    m_index = new StringIndex(this, &get_string, m_array->get_alloc()); // Throws

    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        StringData value = get(row_ndx);
        size_t num_rows = 1;
        bool is_append = true;
        m_index->insert(row_ndx, value, num_rows, is_append); // Throws
    }

    return *m_index;
}


void ColumnStringEnum::set_index_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent)
{
    TIGHTDB_ASSERT(!m_index);
    m_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string,
                              m_array->get_alloc()); // Throws
}


void ColumnStringEnum::install_index(StringIndex* index) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_index == null_ptr);

    index->set_target(this, &get_string);
    m_index = index; // we now own this index
}


void ColumnStringEnum::update_column_index(size_t new_col_ndx, const Spec& spec) TIGHTDB_NOEXCEPT
{
    Column::update_column_index(new_col_ndx, spec);
    std::size_t ndx_is_spec_enumkeys = spec.get_enumkeys_ndx(new_col_ndx);
    m_keys.get_root_array()->set_ndx_in_parent(ndx_is_spec_enumkeys);
    if (m_index) {
        size_t ndx_in_parent = m_array->get_ndx_in_parent();
        m_index->get_root_array()->set_ndx_in_parent(ndx_in_parent + 1);
    }
}


void ColumnStringEnum::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    Column::refresh_accessor_tree(col_ndx, spec);
    size_t ndx_is_spec_enumkeys = spec.get_enumkeys_ndx(col_ndx);
    m_keys.get_root_array()->set_ndx_in_parent(ndx_is_spec_enumkeys);
    m_keys.refresh_accessor_tree(0, spec);

    // Refresh search index
    if (m_index) {
        size_t ndx_in_parent = m_array->get_ndx_in_parent();
        m_index->get_root_array()->set_ndx_in_parent(ndx_in_parent + 1);
        // FIXME: The cached root array needs to be refreshed; however, it is
        // unkown to me (Kristian) whether the root node can change between
        // different Array-like classes. If it can, then the refresh operation
        // could be as non-trivial as it is in the case of
        // ColumnBinary::refresh_accessor_tree(). For that reason, the current
        // work-around is to simply recreate the search index.
        bool use_workaround = true;
        if (use_workaround) {
            delete m_index;
            m_index = 0;
            ref_type ref = m_index->get_root_array()->get_ref_from_parent();
            ArrayParent* parent = m_index->get_root_array()->get_parent();
            Allocator& alloc = m_array->get_alloc();
            m_index = new StringIndex(ref, parent, ndx_in_parent+1, this,
                                      &get_string, alloc); // Throws
        }
        else {
            m_index->refresh_accessor_tree(col_ndx, spec); // Throws
        }
    }
}


#ifdef TIGHTDB_DEBUG

void ColumnStringEnum::Verify() const
{
    m_keys.Verify();
    Column::Verify();

    if (m_index)
        m_index->Verify();
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
    Array leaf(mem, 0, 0, alloc);
    int indent = level * 2;
    out << setw(indent) << "" << "String enumeration leaf (size: "<<leaf.size()<<")\n";
}

} // anonymous namespace

void ColumnStringEnum::dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
