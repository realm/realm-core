#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#ifdef _MSC_VER
    #include <win32\types.h>
#endif

#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>

namespace {

using namespace tightdb;

ColumnDef GetTypeFromArray(size_t ref, Allocator& alloc)
{
    const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
    const bool isNode = (header[0] & 0x80) != 0;
    const bool hasRefs  = (header[0] & 0x40) != 0;

    if (isNode) return COLUMN_NODE;
    else if (hasRefs) return COLUMN_HASREFS;
    else return COLUMN_NORMAL;
}

}


namespace tightdb {

AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc) : m_index(NULL)
{
    m_array = new ArrayString(NULL, 0, alloc);
}

AdaptiveStringColumn::AdaptiveStringColumn(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc) : m_index(NULL)
{
    const ColumnDef type = GetTypeFromArray(ref, alloc);
    if (type == COLUMN_NODE) {
        m_array = new Array(ref, parent, pndx, alloc);
    }
    else if (type == COLUMN_HASREFS) {
        m_array = new ArrayStringLong(ref, parent, pndx, alloc);
    }
    else {
        m_array = new ArrayString(ref, parent, pndx, alloc);
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
    if (IsNode()) m_array->Destroy();
    else if (IsLongStrings()) {
        ((ArrayStringLong*)m_array)->Destroy();
    }
    else ((ArrayString*)m_array)->Destroy();

    if (m_index)
        m_index->Destroy();
}


void AdaptiveStringColumn::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(GetTypeFromArray(ref, m_array->GetAllocator()) == COLUMN_NODE); // Can only be called when creating node

    if (IsNode()) m_array->UpdateRef(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx   = m_array->GetParentNdx();

        // Replace the string array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent) parent->update_child_ref(pndx, ref);
    }
}

// Getter function for string index
static const char* GetString(void* column, size_t ndx)
{
    return ((AdaptiveStringColumn*)column)->Get(ndx);
}

StringIndex& AdaptiveStringColumn::CreateIndex()
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

void AdaptiveStringColumn::SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx)
{
    TIGHTDB_ASSERT(m_index == NULL);
    m_index = new StringIndex(ref, parent, pndx, this, &GetString, m_array->GetAllocator());
}

bool AdaptiveStringColumn::is_empty() const
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else if (IsLongStrings()) {
        return ((ArrayStringLong*)m_array)->is_empty();
    }
    else {
        return ((ArrayString*)m_array)->is_empty();
    }
}

size_t AdaptiveStringColumn::Size() const
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : (size_t)offsets.back();
        return size;
    }
    else if (IsLongStrings()) {
        return ((ArrayStringLong*)m_array)->Size();
    }
    else {
        return ((ArrayString*)m_array)->Size();
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
        ((ArrayStringLong*)m_array)->Clear();
    }
    else ((ArrayString*)m_array)->Clear();

    if (m_index)
        m_index->Clear();
}

void AdaptiveStringColumn::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)

    if (IsLongStrings()) {
        ((ArrayStringLong*)m_array)->Resize(ndx);
    }
    else ((ArrayString*)m_array)->Resize(ndx);

}

const char* AdaptiveStringColumn::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    return m_array->ColumnStringGet(ndx);
    //return TreeGet<const char*, AdaptiveStringColumn>(ndx);
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx < Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        const char* const oldVal = Get(ndx);
        m_index->Set(ndx, oldVal, value);
    }

    return TreeSet<const char*, AdaptiveStringColumn>(ndx, value);
}

bool AdaptiveStringColumn::add(const char* value)
{
    return Insert(Size(), value);
}

bool AdaptiveStringColumn::Insert(size_t ndx, const char* value)
{
    TIGHTDB_ASSERT(ndx <= Size());

    const bool res = TreeInsert<const char*, AdaptiveStringColumn>(ndx, value);
    if (!res) return false;

    if (m_index) {
        const bool isLast = (ndx+1 == Size());
        m_index->Insert(ndx, value, isLast);
    }
    return true;
}

void AdaptiveStringColumn::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());
    TIGHTDB_ASSERT(!m_index);

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<const char*, AdaptiveStringColumn>(i, "");
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

void AdaptiveStringColumn::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        const char* const oldVal = Get(ndx);
        const bool isLast = (ndx == Size());
        m_index->Delete(ndx, oldVal, isLast);
    }

    TreeDelete<const char*, AdaptiveStringColumn>(ndx);
}

size_t AdaptiveStringColumn::count(const char* target) const
{
    TIGHTDB_ASSERT(target);

    if (m_index)
        return m_index->count(target);

    size_t count = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const AdaptiveStringColumn col(ref, NULL, 0, m_array->GetAllocator());
            
            count += col.count(target);
        }
    }
    else {
        if (IsLongStrings())
            count += ((ArrayStringLong*)m_array)->count(target);
        else
            count +=((ArrayString*)m_array)->count(target);
    }
    
    return count;
}

size_t AdaptiveStringColumn::find_first(const char* value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    if (m_index && start == 0 && end == (size_t)-1)
        return m_index->find_first(value);

    return TreeFind<const char*, AdaptiveStringColumn, EQUAL>(value, start, end);
}


void AdaptiveStringColumn::find_all(Array &result, const char* value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    if (m_index && start == 0 && end == (size_t)-1)
        return m_index->find_all(result, value);

    TreeFindAll<const char*, AdaptiveStringColumn>(result, value, 0, start, end);
}

const char* AdaptiveStringColumn::LeafGet(size_t ndx) const
{
    if (IsLongStrings()) {
        return ((ArrayStringLong*)m_array)->Get(ndx);
    }
    else {
        return ((ArrayString*)m_array)->Get(ndx);
    }
}

bool AdaptiveStringColumn::LeafSet(size_t ndx, const char* value)
{
    // Easy to set if the strings fit
    const size_t len = strlen(value);
    if (IsLongStrings()) {
        ((ArrayStringLong*)m_array)->Set(ndx, value, len);
        return true;
    }
    else if (len < 16) {
        return ((ArrayString*)m_array)->Set(ndx, value);
    }

    // Replace string array with long string array
    ArrayStringLong* const newarray = new ArrayStringLong((Array*)NULL, 0, m_array->GetAllocator());

    // Copy strings to new array
    ArrayString* const oldarray = (ArrayString*)m_array;
    for (size_t i = 0; i < oldarray->Size(); ++i) {
        newarray->add(oldarray->Get(i));
    }
    newarray->Set(ndx, value, len);

    // Update parent to point to new array
    ArrayParent *const parent = oldarray->GetParent();
    if (parent) {
        const size_t pndx = oldarray->GetParentNdx();
        parent->update_child_ref(pndx, newarray->GetRef());
        newarray->SetParent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = newarray;
    oldarray->Destroy();
    delete oldarray;

    return true;}

bool AdaptiveStringColumn::LeafInsert(size_t ndx, const char* value)
{
    // Easy to insert if the strings fit
    const size_t len = strlen(value);
    if (IsLongStrings()) {
        ((ArrayStringLong*)m_array)->Insert(ndx, value, len);
        return true;
    }
    else if (len < 16) {
        return ((ArrayString*)m_array)->Insert(ndx, value);
    }

    // Replace string array with long string array
    ArrayStringLong* const newarray = new ArrayStringLong((Array*)NULL, 0, m_array->GetAllocator());

    // Copy strings to new array
    ArrayString* const oldarray = (ArrayString*)m_array;
    const size_t n = oldarray->Size();
    for (size_t i=0; i<n; ++i) {
        newarray->add(oldarray->Get(i));
    }
    newarray->Insert(ndx, value, len);

    // Update parent to point to new array
    ArrayParent *const parent = oldarray->GetParent();
    if (parent) {
        const size_t pndx = oldarray->GetParentNdx();
        parent->update_child_ref(pndx, newarray->GetRef());
        newarray->SetParent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = newarray;
    oldarray->Destroy();
    delete oldarray;

    return true;
}

template<class F>
size_t AdaptiveStringColumn::LeafFind(const char* value, size_t start, size_t end) const
{
    if (IsLongStrings()) {
        return ((ArrayStringLong*)m_array)->find_first(value, start, end);
    }
    else {
        return ((ArrayString*)m_array)->find_first(value, start, end);
    }
}

void AdaptiveStringColumn::LeafFindAll(Array &result, const char* value, size_t add_offset, size_t start, size_t end) const
{
    if (IsLongStrings()) {
        return ((ArrayStringLong*)m_array)->find_all(result, value, add_offset, start, end);
    }
    else {
        return ((ArrayString*)m_array)->find_all(result, value, add_offset, start, end);
    }
}


void AdaptiveStringColumn::LeafDelete(size_t ndx)
{
    if (IsLongStrings()) {
        ((ArrayStringLong*)m_array)->Delete(ndx);
    }
    else {
        ((ArrayString*)m_array)->Delete(ndx);
    }
}

bool AdaptiveStringColumn::FindKeyPos(const char* target, size_t& pos) const
{
    const int len = (int)Size();
    bool found = false;
    ssize_t low  = -1;
    ssize_t high = len;

    // Binary search based on:
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary
    // Finds position of closest value BIGGER OR EQUAL to the target (for
    // lookups in indexes)
    while (high - low > 1) {
        const ssize_t probe = ((size_t)low + (size_t)high) >> 1;
        const char* v = Get(probe);

        const int cmp = strcmp(v, target);

        if (cmp < 0) low  = probe;
        else {
            high = probe;
            if (cmp == 0) found = true;
        }
    }

    pos = high;
    return found;
}

bool AdaptiveStringColumn::AutoEnumerate(size_t& ref_keys, size_t& ref_values) const
{
    AdaptiveStringColumn keys(m_array->GetAllocator());

    // Generate list of unique values (keys)
    const size_t count = Size();
    for (size_t i = 0; i < count; ++i) {
        const char* v = Get(i);

        // Insert keys in sorted order, ignoring duplicates
        size_t pos;
        if (!keys.FindKeyPos(v, pos)) {
            // Don't bother auto enumerating if there are too few duplicates
            if (keys.Size() > (count / 2)) {
                keys.Destroy(); // cleanup
                return false;
            }

            keys.Insert(pos, v);
        }
    }

    // Generate enumerated list of entries
    Column values(m_array->GetAllocator());
    for (size_t i = 0; i < count; ++i) {
        const char* v = Get(i);

        size_t pos;
        const bool res = keys.FindKeyPos(v, pos);  // todo/fixme, res isn't used
        TIGHTDB_ASSERT(res);
        (void)res;

        values.add(pos);
    }

    ref_keys   = keys.GetRef();
    ref_values = values.GetRef();
    return true;
}

bool AdaptiveStringColumn::Compare(const AdaptiveStringColumn& c) const
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


#ifdef TIGHTDB_DEBUG

void AdaptiveStringColumn::LeafToDot(std::ostream& out, const Array& array) const
{
    const bool isLongStrings = array.HasRefs(); // HasRefs indicates long string array

    if (isLongStrings) {
        // ArrayStringLong has more members than Array, so we have to
        // really instantiate it (it is not enough with a cast)
        const size_t ref = array.GetRef();
        ArrayStringLong str_array(ref, (Array*)NULL, 0, array.GetAllocator());
        str_array.ToDot(out);
    }
    else {
        ((ArrayString&)array).ToDot(out);
    }
}

#endif // TIGHTDB_DEBUG

}
