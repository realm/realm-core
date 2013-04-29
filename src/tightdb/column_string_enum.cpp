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

ColumnStringEnum::ColumnStringEnum(size_t ref_keys, size_t ref_values, ArrayParent* parent,
                                   size_t pndx, Allocator& alloc):
    Column(ref_values, parent, pndx+1, alloc), m_keys(ref_keys, parent, pndx, alloc), m_index(NULL) {}

ColumnStringEnum::~ColumnStringEnum()
{
    if (m_index)
        delete m_index;
}

void ColumnStringEnum::Destroy()
{
    m_keys.Destroy();
    Column::Destroy();

    if (m_index)
        m_index->Destroy();
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

StringData ColumnStringEnum::get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < Column::Size());
    size_t key_ndx = Column::GetAsRef(ndx);
    return m_keys.get(key_ndx);
}

void ColumnStringEnum::add(StringData value)
{
    insert(Column::Size(), value);
}

void ColumnStringEnum::set(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < Column::Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        m_index->Set(ndx, oldVal, value);
    }

    size_t key_ndx = GetKeyNdxOrAdd(value);
    Column::set(ndx, key_ndx);
}

void ColumnStringEnum::insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx <= Column::Size());

    const size_t key_ndx = GetKeyNdxOrAdd(value);
    Column::insert(ndx, key_ndx);

    if (m_index) {
        const bool isLast = ndx+1 == Size();
        m_index->Insert(ndx, value, isLast);
    }
}

void ColumnStringEnum::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Column::Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        const bool isLast = ndx == Size();
        m_index->erase(ndx, oldVal, isLast);
    }

    Column::erase(ndx);
}

void ColumnStringEnum::Clear()
{
    // Note that clearing a StringEnum does not remove keys
    Column::Clear();

    if (m_index)
        m_index->Clear();
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
    const size_t res = m_keys.find_first(value);
    if (res != size_t(-1)) return res;
    else {
        // Add key if it does not exist
        const size_t pos = m_keys.Size();
        m_keys.add(value);
        return pos;
    }
}

bool ColumnStringEnum::compare(const AdaptiveStringColumn& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i)) return false;
    }
    return true;
}

bool ColumnStringEnum::compare(const ColumnStringEnum& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i)) return false;
    }
    return true;
}


StringIndex& ColumnStringEnum::CreateIndex()
{
    TIGHTDB_ASSERT(m_index == NULL);

    // Create new index
    m_index = new StringIndex(this, &get_string, m_array->GetAllocator());

    // Populate the index
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = get(i);
        m_index->Insert(i, value, true);
    }

    return *m_index;
}

void ColumnStringEnum::SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx)
{
    TIGHTDB_ASSERT(m_index == NULL);
    m_index = new StringIndex(ref, parent, pndx, this, &get_string, m_array->GetAllocator());
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

void ColumnStringEnum::ToDot(ostream& out, StringData title) const
{
    const size_t ref = m_keys.GetRef();

    out << "subgraph cluster_columnstringenum" << ref << " {" << endl;
    out << " label = \"ColumnStringEnum";
    if (0 < title.size()) out << "\\n'" << title << "'";
    out << "\";" << endl;

    m_keys.ToDot(out, "keys");
    Column::ToDot(out, "values");

    out << "}" << endl;
}

#endif // TIGHTDB_DEBUG

}
