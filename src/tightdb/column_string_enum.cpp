#include <tightdb/column_string_enum.hpp>
#include <tightdb/index_string.hpp>

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

size_t ColumnStringEnum::Size() const
{
    return Column::Size();
}

bool ColumnStringEnum::is_empty() const
{
    return Column::is_empty();
}

const char* ColumnStringEnum::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Column::Size());
    const size_t key_ndx = Column::GetAsRef(ndx);
    return m_keys.Get(key_ndx);
}

bool ColumnStringEnum::add(const char* value)
{
    return Insert(Column::Size(), value);
}

bool ColumnStringEnum::Set(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx < Column::Size());
    TIGHTDB_ASSERT(value);

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        const char* const oldVal = Get(ndx);
        m_index->Set(ndx, oldVal, value);
    }

    const size_t key_ndx = GetKeyNdxOrAdd(value);
    return Column::Set(ndx, key_ndx);
}

bool ColumnStringEnum::Insert(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx <= Column::Size());
    TIGHTDB_ASSERT(value);

    const size_t key_ndx = GetKeyNdxOrAdd(value);
    const bool res = Column::Insert(ndx, key_ndx);
    if (!res) return false;

    if (m_index) {
        const bool isLast = (ndx+1 == Size());
        m_index->Insert(ndx, value, isLast);
    }

    return true;
}

void ColumnStringEnum::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Column::Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        const char* const oldVal = Get(ndx);
        const bool isLast = (ndx == Size());
        m_index->Delete(ndx, oldVal, isLast);
    }

    Column::Delete(ndx);
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

size_t ColumnStringEnum::count(const char* value) const
{
    if (m_index)
        return m_index->count(value);

    const size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == not_found) return 0;
    return Column::count(key_ndx);
}

void ColumnStringEnum::find_all(Array& res, const char* value, size_t start, size_t end) const
{
    const size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == (size_t)-1) return;
    Column::find_all(res, key_ndx, 0, start, end);
    return;
}

void ColumnStringEnum::find_all(Array& res, size_t key_ndx, size_t start, size_t end) const
{
    if (key_ndx == (size_t)-1) return;
    Column::find_all(res, key_ndx, 0, start, end);
    return;
}


size_t ColumnStringEnum::find_first(size_t key_ndx, size_t start, size_t end) const
{
    // Find key
    if (key_ndx == (size_t)-1) return (size_t)-1;

    return Column::find_first(key_ndx, start, end);
}

size_t ColumnStringEnum::find_first(const char* value, size_t start, size_t end) const
{
    if (m_index && start == 0 && end == (size_t)-1)
        return m_index->find_first(value);

    // Find key
    const size_t key_ndx = m_keys.find_first(value);
    if (key_ndx == (size_t)-1) return (size_t)-1;

    return Column::find_first(key_ndx, start, end);
}

size_t ColumnStringEnum::GetKeyNdx(const char* value) const
{
    return m_keys.find_first(value);
}

size_t ColumnStringEnum::GetKeyNdxOrAdd(const char* value)
{
    const size_t res = m_keys.find_first(value);
    if (res != (size_t)-1) return res;
    else {
        // Add key if it does not exist
        const size_t pos = m_keys.Size();
        m_keys.add(value);
        return pos;
    }
}

bool ColumnStringEnum::Compare(const ColumnStringEnum& c) const
{
    const size_t n = Size();
    if (c.Size() != n) return false;
    for (size_t i=0; i<n; ++i) {
        const char* s1 = Get(i);
        const char* s2 = c.Get(i);
        if (strcmp(s1, s2) != 0) return false;
    }
    return true;
}

// Getter function for string index
static const char* GetString(void* column, size_t ndx)
{
    return ((ColumnStringEnum*)column)->Get(ndx);
}

StringIndex& ColumnStringEnum::CreateIndex()
{
    TIGHTDB_ASSERT(m_index == NULL);

    // Create new index
    m_index = new StringIndex(this, &GetString, m_array->GetAllocator());

    // Populate the index
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        const char* const value = Get(i);
        m_index->Insert(i, value, true);
    }

    return *m_index;
}

void ColumnStringEnum::SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx)
{
    TIGHTDB_ASSERT(m_index == NULL);
    m_index = new StringIndex(ref, parent, pndx, this, &GetString, m_array->GetAllocator());
}

void ColumnStringEnum::TakeOverIndex(StringIndex& index)
{
    TIGHTDB_ASSERT(m_index == NULL);

    index.SetTarget(this, &GetString);
    m_index = &index;
}


#ifdef TIGHTDB_DEBUG

void ColumnStringEnum::Verify() const
{
    m_keys.Verify();
    Column::Verify();
}

void ColumnStringEnum::ToDot(std::ostream& out, const char* title) const
{
    const size_t ref = m_keys.GetRef();

    out << "subgraph cluster_columnstringenum" << ref << " {" << std::endl;
    out << " label = \"ColumnStringEnum";
    if (title) out << "\\n'" << title << "'";
    out << "\";" << std::endl;

    m_keys.ToDot(out, "keys");
    Column::ToDot(out, "values");

    out << "}" << std::endl;
}

#endif // TIGHTDB_DEBUG

}
