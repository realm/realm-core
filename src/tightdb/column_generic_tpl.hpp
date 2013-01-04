#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#ifdef _MSC_VER
    #include <win32\types.h>
#endif

#include <tightdb/query_conditions.hpp>
#include <tightdb/column_float.hpp>

namespace {

using namespace tightdb;

// TODO: This could be common 
bool IsNodeFromRef(size_t ref, Allocator& alloc)
{
    const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
    const bool isNode = (header[0] & 0x80) != 0;

    return isNode;
}

}


namespace tightdb {

template<typename T>
ColumnGeneric<T>::ColumnGeneric(Allocator& alloc)
{
    m_array = new ArrayGeneric<T>(NULL, 0, alloc);
}

template<typename T>
ColumnGeneric<T>::ColumnGeneric(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = IsNodeFromRef(ref, alloc);
    if (isNode)
        m_array = new Array(ref, parent, pndx, alloc);
    else
        m_array = new ArrayGeneric<T>(ref, parent, pndx, alloc);
}

template<typename T>
ColumnGeneric<T>::~ColumnGeneric()
{
    if (IsNode()) 
        delete m_array;
    else 
        delete (ArrayGeneric<T>*)m_array;
}

template<typename T>
void ColumnGeneric<T>::Destroy()
{
    if (IsNode()) 
        m_array->Destroy();
    else 
        ((ArrayGeneric<T>*)m_array)->Destroy();
}


template<typename T>
void ColumnGeneric<T>::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(IsNodeFromRef(ref, m_array->GetAllocator())); // Can only be called when creating node

    if (IsNode()) 
        m_array->UpdateRef(ref);
    else {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Replace the generic array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->GetAllocator());
        delete m_array;
        m_array = array;

        // Update ref in parent
        if (parent) 
            parent->update_child_ref(pndx, ref);
    }
}

template<typename T>
bool ColumnGeneric<T>::is_empty() const
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return ((ArrayGeneric<T>*)m_array)->is_empty();
    }
}

template<typename T>
size_t ColumnGeneric<T>::Size() const
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : (size_t)offsets.back();
        return size;
    }
    else {
        return ((ArrayGeneric<T>*)m_array)->Size();
    }
}

template<typename T>
void ColumnGeneric<T>::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to generic array
        ArrayGeneric<T>* array = new ArrayGeneric<T>(parent, pndx, m_array->GetAllocator());
        if (parent) 
            parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else 
        ((ArrayGeneric<T>*)m_array)->Clear();
}

template<typename T>
void ColumnGeneric<T>::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    ((ArrayGeneric<T>*)m_array)->Resize(ndx);
}

template<typename T>
T ColumnGeneric<T>::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    // return m_array->Column???StringGet(ndx);
    // FIXME??? why different in stringColumn?
    return TreeGet<T, ColumnGeneric<T>>(ndx);
}

template<typename T>
bool ColumnGeneric<T>::Set(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeSet<T,ColumnGeneric<T>>(ndx, value);
}

template<typename T>
bool ColumnGeneric<T>::add(T value)
{
    return Insert(Size(), value);
}

template<typename T>
bool ColumnGeneric<T>::Insert(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= Size());
    return TreeInsert<T, ColumnGeneric<T>>(ndx, value);
}

template<typename T>
void ColumnGeneric<T>::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<T, ColumnGeneric<T>>(i, 0);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template<typename T>
bool ColumnGeneric<T>::Compare(const ColumnGeneric& c) const
{
    const size_t n = Size();
    if (c.Size() != n) 
        return false;
    for (size_t i=0; i<n; ++i) {
        const T v1 = Get(i);
        const T v2 = c.Get(i);
        if (v1 == v2) 
            return false;
    }
    return true;
}


template<typename T>
void ColumnGeneric<T>::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<T, ColumnGeneric<T>>(ndx);
}

template<typename T>
T ColumnGeneric<T>::LeafGet(size_t ndx) const
{
    return ((ArrayGeneric<T>*)m_array)->Get(ndx);
}

template<typename T>
bool ColumnGeneric<T>::LeafSet(size_t ndx, T value)
{
    ((ArrayGeneric<T>*)m_array)->Set(ndx, value);
    return true;
}

template<typename T>
bool ColumnGeneric<T>::LeafInsert(size_t ndx, T value)
{
    ((ArrayGeneric<T>*)m_array)->Insert(ndx, value);
    return true;
}

template<typename T>
void ColumnGeneric<T>::LeafDelete(size_t ndx)
{
    ((ArrayGeneric<T>*)m_array)->Delete(ndx);
}


#ifdef TIGHTDB_DEBUG

template<typename T>
void ColumnGeneric<T>::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const ArrayGeneric<T> newArray(ref, NULL, 0, array.GetAllocator());

    newArray.ToDot(out);
}

#endif // TIGHTDB_DEBUG


template<typename T> template<class F>
size_t ColumnGeneric<T>::LeafFind(T value, size_t start, size_t end) const
{
    return ((ArrayGeneric<T>*)m_array)->find_first(value, start, end);
}

template<typename T>
void ColumnGeneric<T>::LeafFindAll(Array &result, T value, size_t add_offset, size_t start, size_t end) const
{
    return ((ArrayGeneric<T>*)m_array)->find_all(result, value, add_offset, start, end);
}

template<typename T>
size_t ColumnGeneric<T>::find_first(T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    return TreeFind<T, ColumnGeneric<T>, EQUAL>(value, start, end);
}

template<typename T>
void ColumnGeneric<T>::find_all(Array &result, T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    TreeFindAll<T, ColumnGeneric<T>>(result, value, 0, start, end);
}

template<typename T>
size_t ColumnGeneric<T>::count(T target) const
{
    size_t count = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnGeneric<T> col(ref, NULL, 0, m_array->GetAllocator());
            
            count += col.count(target);
        }
    }
    else {
            count += ((ArrayGeneric<T>*)m_array)->count(target);
    }
    return count;
}

}
