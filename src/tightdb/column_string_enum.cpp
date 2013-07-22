#include <tightdb/column_string_enum.hpp>
#include <tightdb/index_string.hpp>

using namespace std;

namespace {

// Getter function for string index
tightdb::StringData get_string(void* column, size_t ndx)
{
    return static_cast<tightdb::ColumnStringEnum*>(column)->get(ndx);
}

} // anonymous namespace


namespace tightdb {

ColumnStringEnum::ColumnStringEnum(ref_type keys, ref_type values, ArrayParent* parent,
                                   size_t ndx_in_parent, Allocator& alloc):
    Column(values, parent, ndx_in_parent+1, alloc), // Throws
    m_keys(keys,   parent, ndx_in_parent,   alloc), // Throws
    m_index(0) {}

ColumnStringEnum::~ColumnStringEnum()
{
    if (m_index)
        delete m_index;
}

void ColumnStringEnum::destroy()
{
    m_keys.destroy();
    Column::destroy();

    if (m_index)
        m_index->destroy();
}

void ColumnStringEnum::UpdateParentNdx(int diff)
{
    m_keys.UpdateParentNdx(diff);
    Column::UpdateParentNdx(diff);
}

void ColumnStringEnum::UpdateFromParent()
{
    m_array->UpdateFromParent();
    m_keys.UpdateFromParent();
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

void ColumnStringEnum::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Column::size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData old_val = get(ndx);
        const bool is_last = ndx == size();
        m_index->erase(ndx, old_val, is_last);
    }

    Column::erase(ndx);
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
    Column::find_all(res, key_ndx, 0, begin, end);
    return;
}

void ColumnStringEnum::find_all(Array& res, size_t key_ndx, size_t begin, size_t end) const
{
    if (key_ndx == size_t(-1)) return;
    Column::find_all(res, key_ndx, 0, begin, end);
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

bool ColumnStringEnum::compare(const AdaptiveStringColumn& c) const
{
    const size_t n = size();
    if (c.size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i)) return false;
    }
    return true;
}

bool ColumnStringEnum::compare(const ColumnStringEnum& c) const
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


StringIndex& ColumnStringEnum::CreateIndex()
{
    TIGHTDB_ASSERT(m_index == NULL);

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

void ColumnStringEnum::SetIndexRef(ref_type ref, ArrayParent* parent, size_t ndx_in_parent)
{
    TIGHTDB_ASSERT(!m_index);
    m_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string, m_array->get_alloc());
}

void ColumnStringEnum::ReuseIndex(StringIndex& index)
{
    TIGHTDB_ASSERT(m_index == NULL);

    index.SetTarget(this, &get_string);
    m_index = &index; // we now own this index
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

    out << "subgraph cluster_columnstringenum" << ref << " {" << endl;
    out << " label = \"ColumnStringEnum";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    m_keys.to_dot(out, "keys");
    Column::to_dot(out, "values");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
