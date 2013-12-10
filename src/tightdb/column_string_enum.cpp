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
    m_keys.adjust_ndx_in_parent(diff);
}

void ColumnStringEnum::adjust_ndx_in_parent(int diff) TIGHTDB_NOEXCEPT
{
    Column::adjust_ndx_in_parent(diff);
}

void ColumnStringEnum::update_from_parent(size_t old_baseline) TIGHTDB_NOEXCEPT
{
    m_array->update_from_parent(old_baseline);
    m_keys.update_from_parent(old_baseline);
}

void ColumnStringEnum::add(StringData value)
{
    insert(Column::size(), value);
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

void ColumnStringEnum::insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= Column::size());

    size_t key_ndx = GetKeyNdxOrAdd(value);
    Column::insert(ndx, key_ndx);

    if (m_index) {
        bool is_last = ndx+1 == size();
        m_index->insert(ndx, value, is_last);
    }
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
    if (key_ndx == not_found) return 0;
    return Column::count(key_ndx);
}

void ColumnStringEnum::find_all(Array& res, StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1))
        return m_index->find_all(res, value);

    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1)) return;
    Column::find_all(res, key_ndx, begin, end);
    return;
}

void ColumnStringEnum::find_all(Array& res, size_t key_ndx, size_t begin, size_t end) const
{
    if (key_ndx == size_t(-1)) return;
    Column::find_all(res, key_ndx, begin, end);
    return;
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
    if (key_ndx == size_t(-1)) return size_t(-1);

    return Column::find_first(key_ndx, begin, end);
}

size_t ColumnStringEnum::find_first(StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1))
        return m_index->find_first(value);

    // Find key
    size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == size_t(-1)) return size_t(-1);

    return Column::find_first(key_ndx, begin, end);
}

size_t ColumnStringEnum::GetKeyNdx(StringData value) const
{
    return m_keys.find_first(value);
}

size_t ColumnStringEnum::GetKeyNdxOrAdd(StringData value)
{
    size_t res = m_keys.find_first(value);
    if (res != size_t(-1)) return res;
    else {
        // Add key if it does not exist
        size_t pos = m_keys.size();
        m_keys.add(value);
        return pos;
    }
}

bool ColumnStringEnum::compare_string(const AdaptiveStringColumn& c) const
{
    const size_t n = size();
    if (c.size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i)) return false;
    }
    return true;
}

bool ColumnStringEnum::compare_string(const ColumnStringEnum& c) const
{
    const size_t n = size();
    if (c.size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i)) return false;
    }
    return true;
}


void ColumnStringEnum::ForEachIndexOp::handle_chunk(const int64_t* begin, const int64_t* end)
    TIGHTDB_NOEXCEPT
{
    const int buf_size = 16;
    StringData buf[buf_size];
    while (buf_size < end - begin) {
        for (int i=0; i<buf_size; ++i) {
            buf[i] = m_keys.get(*(begin++));
        }
        m_op->handle_chunk(buf, buf + buf_size);
    }
    for (int i = 0; i < int(end - begin); ++i) {
        buf[i] = m_keys.get(*(begin+i));
    }
    m_op->handle_chunk(buf, buf + (end - begin));
}


StringIndex& ColumnStringEnum::create_index()
{
    TIGHTDB_ASSERT(m_index == null_ptr);

    // Create new index
    m_index = new StringIndex(this, &get_string, m_array->get_alloc());

    // Populate the index
    const size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = get(i);
        m_index->insert(i, value, true);
    }

    return *m_index;
}

void ColumnStringEnum::set_index_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent)
{
    TIGHTDB_ASSERT(!m_index);
    m_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string, m_array->get_alloc());
}

void ColumnStringEnum::install_index(StringIndex* index) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_index == null_ptr);

    index->set_target(this, &get_string);
    m_index = index; // we now own this index
}


#ifdef TIGHTDB_DEBUG

void ColumnStringEnum::Verify() const
{
    m_keys.Verify();
    Column::Verify();
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
