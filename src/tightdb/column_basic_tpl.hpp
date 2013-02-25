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


namespace tightdb {

// Predeclarations from query_engine.hpp
class ParentNode;
template<class T, class F> class BasicNode;
template<class T> class SequentialGetter;


template<typename T>
BasicColumn<T>::BasicColumn(Allocator& alloc)
{
    m_array = new BasicArray<T>(NULL, 0, alloc);
}

template<typename T>
BasicColumn<T>::BasicColumn(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc)
{
    const bool isNode = is_node_from_ref(ref, alloc);
    if (isNode)
        m_array = new Array(ref, parent, pndx, alloc);
    else
        m_array = new BasicArray<T>(ref, parent, pndx, alloc);
}

template<typename T>
BasicColumn<T>::~BasicColumn()
{
    if (IsNode())
        delete m_array;
    else
        delete static_cast<BasicArray<T>*>(m_array);
}

template<typename T>
void BasicColumn<T>::Destroy()
{
    if (IsNode())
        m_array->Destroy();
    else
        static_cast<BasicArray<T>*>(m_array)->Destroy();
}


template<typename T>
void BasicColumn<T>::UpdateRef(size_t ref)
{
    TIGHTDB_ASSERT(is_node_from_ref(ref, m_array->GetAllocator())); // Can only be called when creating node

    if (IsNode())
        m_array->UpdateRef(ref);
    else {
        ArrayParent* const parent = m_array->GetParent();
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
bool BasicColumn<T>::is_empty() const TIGHTDB_NOEXCEPT
{
    if (IsNode()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return static_cast<BasicArray<T>*>(m_array)->is_empty();
    }
}

template<typename T>
size_t BasicColumn<T>::Size() const TIGHTDB_NOEXCEPT
{
    if (IsNode())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
        return size;
    }
    else {
        return m_array->size();
    }
}

template<typename T>
void BasicColumn<T>::Clear()
{
    if (m_array->IsNode()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to generic array
        BasicArray<T>* array = new BasicArray<T>(parent, pndx, m_array->GetAllocator());
        if (parent)
            parent->update_child_ref(pndx, array->GetRef());

        // Remove original node
        m_array->Destroy();
        delete m_array;

        m_array = array;
    }
    else
        static_cast<BasicArray<T>*>(m_array)->Clear();
}

template<typename T>
void BasicColumn<T>::Resize(size_t ndx)
{
    TIGHTDB_ASSERT(!IsNode()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < Size());
    static_cast<BasicArray<T>*>(m_array)->Resize(ndx);
}

template<typename T>
T BasicColumn<T>::Get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < Size());
    return BasicArray<T>::column_get(m_array, ndx);
}

template<typename T>
void BasicColumn<T>::Set(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeSet<T,BasicColumn<T> >(ndx, value);
}

template<typename T>
void BasicColumn<T>::add(T value)
{
    Insert(Size(), value);
}

template<typename T>
void BasicColumn<T>::Insert(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= Size());
    TreeInsert<T, BasicColumn<T> >(ndx, value);
}

template<typename T>
void BasicColumn<T>::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        TreeInsert<T, BasicColumn<T> >(i, 0);
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template<typename T>
bool BasicColumn<T>::compare(const BasicColumn& c) const
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
void BasicColumn<T>::Delete(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < Size());
    TreeDelete<T, BasicColumn<T> >(ndx);
}

template<typename T>
T BasicColumn<T>::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return static_cast<BasicArray<T>*>(m_array)->Get(ndx);
}

template<typename T>
void BasicColumn<T>::LeafSet(size_t ndx, T value)
{
    static_cast<BasicArray<T>*>(m_array)->Set(ndx, value);
}

template<typename T>
void BasicColumn<T>::LeafInsert(size_t ndx, T value)
{
    static_cast<BasicArray<T>*>(m_array)->Insert(ndx, value);
}

template<typename T>
void BasicColumn<T>::LeafDelete(size_t ndx)
{
    static_cast<BasicArray<T>*>(m_array)->Delete(ndx);
}


#ifdef TIGHTDB_DEBUG

template<typename T>
void BasicColumn<T>::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.GetRef();
    const BasicArray<T> newArray(ref, NULL, 0, array.GetAllocator());

    newArray.ToDot(out);
}

#endif // TIGHTDB_DEBUG


template<typename T> template<class F>
size_t BasicColumn<T>::LeafFind(T value, size_t start, size_t end) const
{
    return static_cast<BasicArray<T>*>(m_array)->find_first(value, start, end);
}

template<typename T>
void BasicColumn<T>::LeafFindAll(Array &result, T value, size_t add_offset, size_t start, size_t end) const
{
    return static_cast<BasicArray<T>*>(m_array)->find_all(result, value, add_offset, start, end);
}

template<typename T>
size_t BasicColumn<T>::find_first(T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    return TreeFind<T, BasicColumn<T>, Equal>(value, start, end);
}

template<typename T>
void BasicColumn<T>::find_all(Array &result, T value, size_t start, size_t end) const
{
    TIGHTDB_ASSERT(value);

    TreeFindAll<T, BasicColumn<T> >(result, value, 0, start, end);
}


#if 1

template<typename T>
size_t BasicColumn<T>::count(T target) const
{
    return size_t(ColumnBase::aggregate<T, int64_t, act_Count, Equal>(target, 0, Size(), NULL));
}

template<typename T>
typename BasicColumn<T>::SumType BasicColumn<T>::sum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, SumType, act_Sum, None>(0, start, end, NULL);
}

template<typename T>
double BasicColumn<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    double sum1 = ColumnBase::aggregate<T, SumType, act_Sum, None>(0, start, end, NULL);
    double avg = sum1 / ( size == 0 ? 1 : size );
    return avg;
}

template<typename T>
T BasicColumn<T>::minimum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, T, act_Min, None>(0, start, end, NULL);
}

template<typename T>
T BasicColumn<T>::maximum(size_t start, size_t end) const
{
    return ColumnBase::aggregate<T, T, act_Max, None>(0, start, end, NULL);
}

/*
template<typename T>
void BasicColumn<T>::sort(size_t start, size_t end)
{
    // TODO
    assert(0);
}
*/

#else

// Alternative 'naive' reference implementation - useful for reference performance testing.
// TODO: test performance of column aggregates

template<typename T>
size_t BasicColumn<T>::count(T target) const
{
    size_t count = 0;

    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.size();

        for (size_t i = 0; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const BasicColumn<T> col(ref, NULL, 0, m_array->GetAllocator());

            count += col.count(target);
        }
    }
    else {
        count += ((BasicArray<T>*)m_array)->count(target);
    }
    return count;
}

template<typename T>
T BasicColumn<T>::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    double sum = 0;

    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.size();

        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const BasicColumn<T> col(ref, NULL, 0, m_array->GetAllocator());

            sum += col.sum(start, end);
        }
    }
    else {
        sum += ((BasicArray<T>*)m_array)->sum(start, end);
    }
    return sum;
}

template<typename T>
double BasicColumn<T>::average(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();
    size_t size = end - start;
    double sum2 = sum(start, end);
    double avg = sum2 / double( size == 0 ? 1 : size );
    return avg;
}

// #include <iostream>

template<typename T>
T BasicColumn<T>::minimum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    T min_val = T(987.0);
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.size();

        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const BasicColumn<T> col(ref, NULL, 0, m_array->GetAllocator());
            T val = col.minimum(start, end);
            if (val < min_val || i == start) {
                //std::cout << "Min " << i << ": " << min_val << " new val: " << val << "\n";
                val = min_val;
            }
        }
    }
    else {
       // std::cout << "array-min before: " << min_val;
        ((BasicArray<T>*)m_array)->minimum(min_val, start, end);
       // std::cout << " after: " << min_val << "\n";
    }
    return min_val;
}

template<typename T>
T BasicColumn<T>::maximum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = Size();

    T max_val = T(0.0);
    if (m_array->IsNode()) {
        const Array refs = NodeGetRefs();
        const size_t n = refs.size();

        for (size_t i = start; i < n; ++i) {
            const size_t ref = refs.GetAsRef(i);
            const BasicColumn<T> col(ref, NULL, 0, m_array->GetAllocator());
            T val = col.maximum(start, end);
            if (val > max_val || i == start)
                val = max_val;
        }
    }
    else {
        ((BasicArray<T>*)m_array)->maximum(max_val, start, end);
    }
    return max_val;
}

#endif // reference implementation of aggregates

} // namespace tightdb

#endif // TIGHTDB_COLUMN_BASIC_TPL_HPP
