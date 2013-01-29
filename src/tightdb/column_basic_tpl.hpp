/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COLUMN_BASIC_TPL_HPP
#define TIGHTDB_COLUMN_BASIC_TPL_HPP

#include <tightdb/query_engine.hpp>


namespace {

using namespace tightdb;

// TODO: IsNodeFromRef() could be common in column.hpp?
bool IsNodeFromRef(size_t ref, Allocator& alloc)
{
    const uint8_t* const header = (uint8_t*)alloc.Translate(ref);
    const bool isNode = (header[0] & 0x80) != 0;
    return isNode;
}

}


namespace tightdb {
// Predeclarations from query_engine.hpp
class ParentNode;
template<class T, class F> class BASICNODE;
template<class T> class SequentialGetter;


template<typename T>
ColumnBasic<T>::ColumnBasic(Allocator& alloc)
{
    m_array = new ArrayBasic<T>(NULL, 0, alloc);
}

template<typename T>
ColumnBasic<T>::ColumnBasic(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = IsNodeFromRef(ref, alloc);
    if (isNode)
        m_array = new Array(ref, parent, pndx, alloc);
    else
        m_array = new ArrayBasic<T>(ref, parent, pndx, alloc);
}

template<typename T>
ColumnBasic<T>::~ColumnBasic()
{
    if (IsNode()) 
        delete m_array;
    else 
        delete (ArrayBasic<T>*)m_array;
}

template<typename T>
void ColumnBasic<T>::Destroy()
{
    if (IsNode()) 
        m_array->Destroy();
    else 
        ((ArrayBasic<T>*)m_array)->Destroy();
}


template<typename T>
void ColumnBasic<T>::UpdateRef(size_t ref)
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
bool ColumnBasic<T>::is_empty() const TIGHTDB_NOEXCEPT
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return ((ArrayBasic<T>*)m_array)->is_empty();
    }
}

template<typename T>
size_t ColumnBasic<T>::Size() const TIGHTDB_NOEXCEPT
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : (size_t)offsets.back();
        return size;
    }
    else {
        return ((ArrayBasic<T>*)m_array)->Size();
    }
}

template<typename T>
void ColumnBasic<T>::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to generic array
        ArrayBasic<T>* array = new ArrayBasic<T>(parent, pndx, m_array->GetAllocator());
        if (parent) 
            parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else 
        ((ArrayBasic<T>*)m_array)->Clear();
}

template<typename T>
void ColumnBasic<T>::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    ((ArrayBasic<T>*)m_array)->Resize(ndx);
}

template<typename T>
T ColumnBasic<T>::Get(size_t ndx) const
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeGet<T, ColumnBasic<T> >(ndx); // Throws
}

template<typename T>
bool ColumnBasic<T>::Set(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < Size());
    return TreeSet<T,ColumnBasic<T> >(ndx, value);
}

template<typename T>
bool ColumnBasic<T>::add(T value)
{
    return Insert(Size(), value);
}

template<typename T>
bool ColumnBasic<T>::Insert(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= Size());
    return TreeInsert<T, ColumnBasic<T> >(ndx, value);
}

template<typename T>
void ColumnBasic<T>::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<T, ColumnBasic<T> >(i, 0);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template<typename T>
bool ColumnBasic<T>::Compare(const ColumnBasic& c) const
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
void ColumnBasic<T>::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<T, ColumnBasic<T> >(ndx);
}

template<typename T>
T ColumnBasic<T>::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return ((ArrayBasic<T>*)m_array)->Get(ndx);
}

template<typename T>
bool ColumnBasic<T>::LeafSet(size_t ndx, T value)
{
    ((ArrayBasic<T>*)m_array)->Set(ndx, value);
    return true;
}

template<typename T>
bool ColumnBasic<T>::LeafInsert(size_t ndx, T value)
{
    ((ArrayBasic<T>*)m_array)->Insert(ndx, value);
    return true;
}

template<typename T>
void ColumnBasic<T>::LeafDelete(size_t ndx)
{
    ((ArrayBasic<T>*)m_array)->Delete(ndx);
}


#ifdef TIGHTDB_DEBUG

template<typename T>
void ColumnBasic<T>::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const ArrayBasic<T> newArray(ref, NULL, 0, array.GetAllocator());

    newArray.ToDot(out);
}

#endif // TIGHTDB_DEBUG


template<typename T> template<class F>
size_t ColumnBasic<T>::LeafFind(T value, size_t start, size_t end) const
{
    return ((ArrayBasic<T>*)m_array)->find_first(value, start, end);
}

template<typename T>
void ColumnBasic<T>::LeafFindAll(Array &result, T value, size_t add_offset, size_t start, size_t end) const
{
    return ((ArrayBasic<T>*)m_array)->find_all(result, value, add_offset, start, end);
}

template<typename T>
size_t ColumnBasic<T>::find_first(T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    return TreeFind<T, ColumnBasic<T>, EQUAL>(value, start, end);
}

template<typename T>
void ColumnBasic<T>::find_all(Array &result, T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    TreeFindAll<T, ColumnBasic<T> >(result, value, 0, start, end);
}


#if 1

template<typename T>
size_t ColumnBasic<T>::count(T target) const
{
    return size_t(ColumnBase::aggregate<T, int64_t, TDB_COUNT, EQUAL>(target, 0, Size(), NULL));
}

template<typename T>
T ColumnBasic<T>::sum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, T, TDB_SUM, NONE>(0, start, end, NULL);
}

template<typename T>
double ColumnBasic<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    T sum1 = ColumnBase::aggregate<T, T, TDB_SUM, NONE>(0, start, end, NULL);
    double avg = double(sum1) / double( size == 0 ? 1 : size ); 
    return avg;
}

template<typename T>
T ColumnBasic<T>::minimum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, T, TDB_MIN, NONE>(0, start, end, NULL);
}

template<typename T>
T ColumnBasic<T>::maximum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, T, TDB_MAX, NONE>(0, start, end, NULL);
}

/*
template<typename T>
void ColumnBasic<T>::sort(size_t start, size_t end)
{
    // TODO
    assert(0);
}
*/

#else

// Alternative 'naive' implementation:
// TODO: test performance of column aggregates

template<typename T>
size_t ColumnBasic<T>::count(T target) const
{
    size_t count = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            
            count += col.count(target);
        }
    }
    else {
        count += ((ArrayBasic<T>*)m_array)->count(target);
    }
    return count;
}

template<typename T>
T ColumnBasic<T>::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    double sum = 0;
    
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            
            sum += col.sum(start, end);
        }
    }
    else {
        sum += ((ArrayBasic<T>*)m_array)->sum(start, end);
    }
    return sum;
}

template<typename T>
double ColumnBasic<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    double sum2 = sum(start, end);
    double avg = sum2 / double( size == 0 ? 1 : size );
    return avg;
}

#include <iostream>

template<typename T>
T ColumnBasic<T>::minimum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    T min_val = T(987.0);
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            T val = col.minimum(start, end);
            if (val < min_val || i == start) {
                //std::cout << "Min " << i << ": " << min_val << " new val: " << val << "\n";
                val = min_val;
            }
        }
    }
    else {
       // std::cout << "array-min before: " << min_val;
        ((ArrayBasic<T>*)m_array)->minimum(min_val, start, end);
       // std::cout << " after: " << min_val << "\n";
    }
    return min_val;
}

template<typename T>
T ColumnBasic<T>::maximum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    T max_val = T(0.0);
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.Size();
        
        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const ColumnBasic<T> col(ref, NULL, 0, m_array->GetAllocator());
            T val = col.maximum(start, end);
            if (val > max_val || i == start)
                val = max_val;
        }
    }
    else {
        ((ArrayBasic<T>*)m_array)->maximum(max_val, start, end);
    }
    return max_val;
}

#endif

} // namespace tightdb

#endif // TIGHTDB_COLUMN_BASIC_TPL_HPP
