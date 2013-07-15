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
    if (!root_is_leaf())
        delete m_array;
    else
        delete static_cast<BasicArray<T>*>(m_array);
}

template<typename T>
void BasicColumn<T>::destroy()
{
    if (!root_is_leaf())
        m_array->destroy();
    else
        static_cast<BasicArray<T>*>(m_array)->destroy();
}


template<typename T>
void BasicColumn<T>::update_ref(size_t ref)
{
    TIGHTDB_ASSERT(is_node_from_ref(ref, m_array->get_alloc())); // Can only be called when creating node

    if (!root_is_leaf())
        m_array->update_ref(ref);
    else {
        ArrayParent* const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Replace the generic array with int array for node
        Array* array = new Array(ref, parent, pndx, m_array->get_alloc());
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
    if (!root_is_leaf()) {
        const Array offsets = NodeGetOffsets();
        return offsets.is_empty();
    }
    else {
        return static_cast<BasicArray<T>*>(m_array)->is_empty();
    }
}

template<typename T>
size_t BasicColumn<T>::size() const TIGHTDB_NOEXCEPT
{
    if (!root_is_leaf())  {
        const Array offsets = NodeGetOffsets();
        const size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
        return size;
    }
    else {
        return m_array->size();
    }
}

template<typename T>
void BasicColumn<T>::clear()
{
    if (!m_array->is_leaf()) {
        ArrayParent *const parent = m_array->GetParent();
        const size_t pndx = m_array->GetParentNdx();

        // Revert to generic array
        BasicArray<T>* array = new BasicArray<T>(parent, pndx, m_array->get_alloc());
        if (parent)
            parent->update_child_ref(pndx, array->get_ref());

        // Remove original node
        m_array->destroy();
        delete m_array;

        m_array = array;
    }
    else
        static_cast<BasicArray<T>*>(m_array)->clear();
}

template<typename T>
void BasicColumn<T>::resize(size_t ndx)
{
    TIGHTDB_ASSERT(root_is_leaf()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < size());
    static_cast<BasicArray<T>*>(m_array)->resize(ndx);
}

template<typename T>
void BasicColumn<T>::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    const size_t ndx_last = size()-1;
    const T v = get(ndx_last);

    set(ndx, v);
    erase(ndx_last);
}


template<typename T>
T BasicColumn<T>::get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());
    return BasicArray<T>::column_get(m_array, ndx);
}

template<typename T>
void BasicColumn<T>::set(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<T,BasicColumn<T> >(ndx, value);
}

template<typename T>
void BasicColumn<T>::add(T value)
{
    insert(size(), value);
}

template<typename T>
void BasicColumn<T>::insert(size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= size());
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
    const size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
        const T v1 = get(i);
        const T v2 = c.get(i);
        if (v1 == v2)
            return false;
    }
    return true;
}


template<class T>
inline void BasicColumn<T>::foreach(Array::ForEachOp<T>* op) const TIGHTDB_NOEXCEPT
{
    if (TIGHTDB_LIKELY(m_array->is_leaf())) {
        static_cast<const BasicArray<T>*>(m_array)->foreach(op);
        return;
    }

    foreach(m_array, op);
}


template<class T>
inline void BasicColumn<T>::foreach(const Array* parent, Array::ForEachOp<T>* op) TIGHTDB_NOEXCEPT
{
    Allocator& alloc = parent->GetAllocator();
    Array children(parent->GetAsRef(1), 0, 0, alloc);
    const std::size_t n = children.size();
    for (std::size_t i=0; i<n; ++i) {
        const std::size_t ref = children.GetAsRef(i);
        Array child(ref, 0, 0, alloc);
        if (TIGHTDB_LIKELY(child.is_leaf())) {
            BasicArray<T>::foreach(&child, op);
        }
        else {
            foreach(&child, op);
        }
    }
}


template<typename T>
void BasicColumn<T>::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeDelete<T, BasicColumn<T> >(ndx);
}

template<typename T>
T BasicColumn<T>::LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return static_cast<BasicArray<T>*>(m_array)->get(ndx);
}

template<typename T>
void BasicColumn<T>::LeafSet(size_t ndx, T value)
{
    static_cast<BasicArray<T>*>(m_array)->set(ndx, value);
}

template<typename T>
void BasicColumn<T>::LeafInsert(size_t ndx, T value)
{
    static_cast<BasicArray<T>*>(m_array)->insert(ndx, value);
}

template<typename T>
void BasicColumn<T>::LeafDelete(size_t ndx)
{
    static_cast<BasicArray<T>*>(m_array)->erase(ndx);
}


#ifdef TIGHTDB_DEBUG

template<typename T>
void BasicColumn<T>::LeafToDot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    const size_t ref = array.get_ref();
    const BasicArray<T> newArray(ref, NULL, 0, array.get_alloc());

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
    return TreeFind<T, BasicColumn<T>, Equal>(value, start, end);
}

template<typename T>
void BasicColumn<T>::find_all(Array &result, T value, size_t start, size_t end) const
{
    TreeFindAll<T, BasicColumn<T> >(result, value, 0, start, end);
}

template<typename T>
size_t BasicColumn<T>::count(T target) const
{
    return size_t(ColumnBase::aggregate<T, int64_t, act_Count, Equal>(target, 0, size(), NULL));
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
        end = size();
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

} // namespace tightdb

#endif // TIGHTDB_COLUMN_BASIC_TPL_HPP
