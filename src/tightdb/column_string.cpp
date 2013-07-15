#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#ifdef _MSC_VER
#  include <win32\types.h>
#endif

#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>

using namespace std;


namespace {

tightdb::Array::ColumnDef get_coldef_from_ref(size_t ref, tightdb::Allocator& alloc)
{
    const char* header = static_cast<char*>(alloc.Translate(ref));
    return tightdb::Array::get_coldef_from_header(header);
}

// Getter function for string index
tightdb::StringData get_string(void* column, size_t ndx)
{
    return static_cast<tightdb::AdaptiveStringColumn*>(column)->get(ndx);
}

} // anonymous namespace


namespace tightdb {

AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc) : m_index(NULL)
{
    m_array = new ArrayString(NULL, 0, alloc);
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc) : m_index(NULL)
{
    Array::ColumnDef type = get_coldef_from_ref(ref, alloc);
    switch (type) {
        case Array::coldef_InnerNode:
            m_array = new Array(ref, parent, pndx, alloc);
            break;
        case Array::coldef_HasRefs:
            m_array = new ArrayStringLong(ref, parent, pndx, alloc);
            break;
        case Array::coldef_Normal:
            m_array = new ArrayString(ref, parent, pndx, alloc);
            break;
    }
}

AdaptiveStringColumn::~AdaptiveStringColumn()
{
    delete m_array;
    if (m_index)
        delete m_index;
}

void AdaptiveStringColumn::Destroy()
{
    if (IsNode())
        m_array->Destroy();
    else if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->Destroy();
    }
    else {
        static_cast<ArrayString*>(m_array)->Destroy();
    }

    if (m_index)
        m_index->Destroy();
}


void AdaptiveStringColumn::update_ref(size_t ref)
{
    TIGHTDB_ASSERT(get_coldef_from_ref(ref, m_array->GetAllocator()) == Array::coldef_InnerNode); // Can only be called when creating node

    if (IsNode())
        m_array->update_ref(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx   = m_array->GetParentNdx();

        // Replace the string array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent)
            parent->update_child_ref(pndx, ref);
    }
}

StringIndex& AdaptiveStringColumn::CreateIndex()
{
    TIGHTDB_ASSERT(!m_index);

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

void AdaptiveStringColumn::SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx)
{
    TIGHTDB_ASSERT(!m_index);
    m_index = new StringIndex(ref, parent, pndx, this, &get_string, m_array->GetAllocator());
}

bool AdaptiveStringColumn::is_empty() const TIGHTDB_NOEXCEPT
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else if (IsLongStrings()) {
        return static_cast<ArrayStringLong*>(m_array)->is_empty();
    }
    else {
        return static_cast<ArrayString*>(m_array)->is_empty();
    }
}

size_t AdaptiveStringColumn::Size() const TIGHTDB_NOEXCEPT
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
        return size;
    }
    else if (IsLongStrings()) {
        return static_cast<ArrayStringLong*>(m_array)->size();
    }
    else {
        return static_cast<ArrayString*>(m_array)->size();
    }
}

void AdaptiveStringColumn::Clear()
{
    if (m_array->IsNode()) {
        // Revert to string array
        m_array->Destroy();
        Array* array = new ArrayString(m_array->GetParent(), m_array->GetParentNdx(), m_array->GetAllocator());
        delete m_array;
        m_array = array;
    }
    else if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->Clear();
    }
    else {
        static_cast<ArrayString*>(m_array)->Clear();
    }

    if (m_index)
        m_index->Clear();
}

void AdaptiveStringColumn::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)

    if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->Resize(ndx);
    }
    else {
        static_cast<ArrayString*>(m_array)->Resize(ndx);
    }

}

void AdaptiveStringColumn::move_last_over(size_t ndx) 
{
    TIGHTDB_ASSERT(ndx+1 < Size());

    const size_t ndx_last = Size()-1;
    StringData v = get(ndx_last);

    if (m_index) {
        // remove the value to be overwritten from index
        StringData oldVal = get(ndx);
        m_index->erase(ndx, oldVal, true);

        // update index to point to new location
        m_index->update_ref(v, ndx_last, ndx);
    }

    TreeSet<StringData, AdaptiveStringColumn>(ndx, v);

    // If the copy happened within the same array
    // it might have moved the source data when making
    // room for the insert. In that case we wil have to
    // copy again from the new position
    // TODO: manual resize before copy
    StringData v2 = get(ndx_last);
    if (v != v2)
        TreeSet<StringData, AdaptiveStringColumn>(ndx, v2);

    TreeDelete<StringData, AdaptiveStringColumn>(ndx_last);
}

void AdaptiveStringColumn::set(size_t ndx, StringData str)
{
    TIGHTDB_ASSERT(ndx < Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        m_index->set(ndx, oldVal, str);
    }

    TreeSet<StringData, AdaptiveStringColumn>(ndx, str);
}

void AdaptiveStringColumn::insert(size_t ndx, StringData str)
{
    TIGHTDB_ASSERT(ndx <= Size());

    TreeInsert<StringData, AdaptiveStringColumn>(ndx, str);

    if (m_index) {
        const bool isLast = (ndx+1 == Size());
        m_index->Insert(ndx, str, isLast);
    }
}

void AdaptiveStringColumn::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());
    TIGHTDB_ASSERT(!m_index);

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<StringData, AdaptiveStringColumn>(i, StringData());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

void AdaptiveStringColumn::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        const bool isLast = (ndx == Size());
        m_index->erase(ndx, oldVal, isLast);
    }

    TreeDelete<StringData, AdaptiveStringColumn>(ndx);
}

size_t AdaptiveStringColumn::count(StringData target) const
{
    if (m_index) {
        return m_index->count(target);
    }

    size_t count = 0;

    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.size();

        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.get_as_ref(i);
            const AdaptiveStringColumn col(ref, NULL, 0, m_array->GetAllocator());

            count += col.count(target);
        }
    }
    else {
        if (IsLongStrings())
            count += static_cast<ArrayStringLong*>(m_array)->count(target);
        else
            count += static_cast<ArrayString*>(m_array)->count(target);
    }

    return count;
}

size_t AdaptiveStringColumn::find_first(StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1)) {
        return m_index->find_first(value);
    }

    return TreeFind<StringData, AdaptiveStringColumn, Equal>(value, begin, end);
}


void AdaptiveStringColumn::find_all(Array &result, StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1)) {
        return m_index->find_all(result, value);
    }

    TreeFindAll<StringData, AdaptiveStringColumn>(result, value, 0, begin, end);
}


FindRes AdaptiveStringColumn::find_all_indexref(StringData value, size_t& dst) const
{
    TIGHTDB_ASSERT(value.data());
    TIGHTDB_ASSERT(m_index);

    return m_index->find_all(value, dst);
}


StringData AdaptiveStringColumn::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    if (IsLongStrings()) {
        return static_cast<ArrayStringLong*>(m_array)->get(ndx);
    }
    else {
        return static_cast<ArrayString*>(m_array)->get(ndx);
    }
}

void AdaptiveStringColumn::LeafSet(size_t ndx, StringData value)
{
    // Easy to set if the strings fit
    if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->set(ndx, value);
        return;
    }
    if (value.size() < 16) {
        static_cast<ArrayString*>(m_array)->set(ndx, value);
        return;
    }

    // Replace string array with long string array
    ArrayStringLong* const newarray =
        new ArrayStringLong(static_cast<Array*>(0), 0, m_array->GetAllocator());

    // Copy strings to new array
    ArrayString* const oldarray = static_cast<ArrayString*>(m_array);
    for (size_t i = 0; i < oldarray->size(); ++i) {
        newarray->add(oldarray->get(i));
    }
    newarray->set(ndx, value);

    // Update parent to point to new array
    ArrayParent *const parent = oldarray->GetParent();
    if (parent) {
        const size_t pndx = oldarray->GetParentNdx();
        parent->update_child_ref(pndx, newarray->get_ref());
        newarray->set_parent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = newarray;
    oldarray->Destroy();
    delete oldarray;
}

void AdaptiveStringColumn::LeafInsert(size_t ndx, StringData value)
{
    // Easy to insert if the strings fit
    if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->insert(ndx, value);
        return;
    }
    if (value.size() < 16) {
        static_cast<ArrayString*>(m_array)->insert(ndx, value);
        return;
    }

    // Replace string array with long string array
    ArrayStringLong* const newarray = new ArrayStringLong(static_cast<Array*>(0), 0, m_array->GetAllocator());

    // Copy strings to new array
    ArrayString* const oldarray = static_cast<ArrayString*>(m_array);
    const size_t n = oldarray->size();
    for (size_t i=0; i<n; ++i) {
        newarray->add(oldarray->get(i));
    }
    newarray->insert(ndx, value);

    // Update parent to point to new array
    ArrayParent *const parent = oldarray->GetParent();
    if (parent) {
        const size_t pndx = oldarray->GetParentNdx();
        parent->update_child_ref(pndx, newarray->get_ref());
        newarray->set_parent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = newarray;
    oldarray->Destroy();
    delete oldarray;
}

template<class> size_t AdaptiveStringColumn::LeafFind(StringData value, size_t begin, size_t end) const
{
    if (IsLongStrings()) {
        return static_cast<ArrayStringLong*>(m_array)->find_first(value, begin, end);
    }
    return static_cast<ArrayString*>(m_array)->find_first(value, begin, end);
}

void AdaptiveStringColumn::LeafFindAll(Array &result, StringData value, size_t add_offset, size_t begin, size_t end) const
{
    if (IsLongStrings()) {
        return static_cast<ArrayStringLong*>(m_array)->find_all(result, value, add_offset, begin, end);
    }
    return static_cast<ArrayString*>(m_array)->find_all(result, value, add_offset, begin, end);
}


void AdaptiveStringColumn::LeafDelete(size_t ndx)
{
    if (IsLongStrings()) {
        static_cast<ArrayStringLong*>(m_array)->erase(ndx);
    }
    else {
        static_cast<ArrayString*>(m_array)->erase(ndx);
    }
}

bool AdaptiveStringColumn::AutoEnumerate(size_t& ref_keys, size_t& ref_values) const
{
    AdaptiveStringColumn keys(m_array->GetAllocator());

    // Generate list of unique values (keys)
    size_t n = Size();
    for (size_t i=0; i<n; ++i) {
        StringData v = get(i);

        // Insert keys in sorted order, ignoring duplicates
        size_t pos = keys.lower_bound(v);
        if (pos != keys.Size() && keys.get(pos) == v)
            continue;

        // Don't bother auto enumerating if there are too few duplicates
        if (n/2 < keys.Size()) {
            keys.Destroy(); // cleanup
            return false;
        }

        keys.insert(pos, v);
    }

    // Generate enumerated list of entries
    Column values(m_array->GetAllocator());
    for (size_t i=0; i<n; ++i) {
        StringData v = get(i);
        size_t pos = keys.lower_bound(v);
        TIGHTDB_ASSERT(pos != keys.Size());
        values.add(pos);
    }

    ref_keys   = keys.get_ref();
    ref_values = values.get_ref();
    return true;
}

bool AdaptiveStringColumn::compare(const AdaptiveStringColumn& c) const
{
    const size_t n = Size();
    if (c.Size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


#ifdef TIGHTDB_DEBUG

void AdaptiveStringColumn::Verify() const
{
    if (m_index) {
        m_index->verify_entries(*this);
    }
}

void AdaptiveStringColumn::LeafToDot(ostream& out, const Array& array) const
{
    const bool isLongStrings = array.HasRefs(); // HasRefs indicates long string array

    if (isLongStrings) {
        // ArrayStringLong has more members than Array, so we have to
        // really instantiate it (it is not enough with a cast)
        const size_t ref = array.get_ref();
        ArrayStringLong str_array(ref, static_cast<Array*>(0), 0, array.GetAllocator());
        str_array.ToDot(out);
    }
    else {
        static_cast<const ArrayString&>(array).ToDot(out);
    }
}

#endif // TIGHTDB_DEBUG

} // namespace tightdb
