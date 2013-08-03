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


template<class T>
BasicColumn<T>::BasicColumn(Allocator& alloc)
{
    m_array = new BasicArray<T>(0, 0, alloc);
}

template<class T>
BasicColumn<T>::BasicColumn(ref_type ref, ArrayParent* parent, std::size_t pndx, Allocator& alloc)
{
    bool root_is_leaf = root_is_leaf_from_ref(ref, alloc);
    if (root_is_leaf)
        m_array = new BasicArray<T>(ref, parent, pndx, alloc);
    else
        m_array = new Array(ref, parent, pndx, alloc);
}

template<class T>
BasicColumn<T>::~BasicColumn()
{
    if (root_is_leaf())
        delete static_cast<BasicArray<T>*>(m_array);
    else
        delete m_array;
}

template<class T>
void BasicColumn<T>::destroy()
{
    if (root_is_leaf())
        static_cast<BasicArray<T>*>(m_array)->destroy();
    else
        m_array->destroy();
}


template<class T>
void BasicColumn<T>::update_ref(ref_type ref)
{
    TIGHTDB_ASSERT(!root_is_leaf_from_ref(ref, m_array->get_alloc())); // Can only be called when creating node

    if (!root_is_leaf()) {
        m_array->update_ref(ref);
        return;
    }

    ArrayParent* parent = m_array->get_parent();
    std::size_t pndx = m_array->get_ndx_in_parent();

    // Replace the generic array with int array for node
    Array* array = new Array(ref, parent, pndx, m_array->get_alloc());
    delete m_array;
    m_array = array;

    // Update ref in parent
    if (parent)
        parent->update_child_ref(pndx, ref);
}

template<class T>
bool BasicColumn<T>::is_empty() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return static_cast<BasicArray<T>*>(m_array)->is_empty();

    Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

template<class T>
std::size_t BasicColumn<T>::size() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf())
        return m_array->size();

    Array offsets = NodeGetOffsets();
    std::size_t size = offsets.is_empty() ? 0 : to_size_t(offsets.back());
    return size;
}

template<class T>
void BasicColumn<T>::clear()
{
    if (m_array->is_leaf()) {
        static_cast<BasicArray<T>*>(m_array)->clear();
        return;
    }

    ArrayParent* parent = m_array->get_parent();
    std::size_t pndx = m_array->get_ndx_in_parent();

    // Revert to generic array
    BasicArray<T>* array = new BasicArray<T>(parent, pndx, m_array->get_alloc());
    if (parent)
        parent->update_child_ref(pndx, array->get_ref());

    // Remove original node
    m_array->destroy();
    delete m_array;

    m_array = array;
}

template<class T>
void BasicColumn<T>::resize(std::size_t ndx)
{
    TIGHTDB_ASSERT(root_is_leaf()); // currently only available on leaf level (used by b-tree code)
    TIGHTDB_ASSERT(ndx < size());
    static_cast<BasicArray<T>*>(m_array)->resize(ndx);
}

template<class T>
void BasicColumn<T>::move_last_over(std::size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    std::size_t ndx_last = size()-1;
    T v = get(ndx_last);

    set(ndx, v);
    erase(ndx_last);
}


template<class T>
T BasicColumn<T>::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());
    if (root_is_leaf())
        return static_cast<const BasicArray<T>*>(m_array)->get(ndx);

    std::pair<MemRef, std::size_t> p = m_array->find_btree_leaf(ndx);
    const char* leaf_header = p.first.m_addr;
    std::size_t ndx_in_leaf = p.second;
    return BasicArray<T>::get(leaf_header, ndx_in_leaf);
}

template<class T>
void BasicColumn<T>::set(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeSet<T,BasicColumn<T> >(ndx, value);
}

template<class T>
void BasicColumn<T>::add(T value)
{
    do_insert(npos, value);
}

template<class T>
void BasicColumn<T>::insert(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= size());
    if (size() <= ndx) ndx = npos;
    do_insert(ndx, value);
}

template<class T>
void BasicColumn<T>::fill(std::size_t count)
{
    TIGHTDB_ASSERT(is_empty());

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (std::size_t i = 0; i < count; ++i) {
        add(T());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

template<class T>
bool BasicColumn<T>::compare(const BasicColumn& c) const
{
    std::size_t n = size();
    if (c.size() != n)
        return false;
    for (std::size_t i=0; i<n; ++i) {
        T v1 = get(i);
        T v2 = c.get(i);
        if (v1 == v2)
            return false;
    }
    return true;
}


template<class T>
void BasicColumn<T>::erase(std::size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());
    TreeDelete<T, BasicColumn<T> >(ndx);
}

template<class T>
void BasicColumn<T>::LeafSet(std::size_t ndx, T value)
{
    static_cast<BasicArray<T>*>(m_array)->set(ndx, value);
}

template<class T>
void BasicColumn<T>::LeafDelete(std::size_t ndx)
{
    static_cast<BasicArray<T>*>(m_array)->erase(ndx);
}


#ifdef TIGHTDB_DEBUG

template<class T>
void BasicColumn<T>::leaf_to_dot(std::ostream& out, const Array& array) const
{
    // Rebuild array to get correct type
    ref_type ref = array.get_ref();
    BasicArray<T> new_array(ref, 0, 0, array.get_alloc());

    new_array.to_dot(out);
}

#endif // TIGHTDB_DEBUG


template<class T> template<class F>
std::size_t BasicColumn<T>::LeafFind(T value, std::size_t start, std::size_t end) const
{
    return static_cast<BasicArray<T>*>(m_array)->find_first(value, start, end);
}

template<class T>
void BasicColumn<T>::LeafFindAll(Array &result, T value, std::size_t add_offset,
                                 std::size_t start, std::size_t end) const
{
    return static_cast<BasicArray<T>*>(m_array)->find_all(result, value, add_offset, start, end);
}

template<class T>
std::size_t BasicColumn<T>::find_first(T value, std::size_t start, std::size_t end) const
{
    return TreeFind<T, BasicColumn<T>, Equal>(value, start, end);
}

template<class T>
void BasicColumn<T>::find_all(Array &result, T value, std::size_t start, std::size_t end) const
{
    TreeFindAll<T, BasicColumn<T> >(result, value, 0, start, end);
}

template<class T>
std::size_t BasicColumn<T>::count(T target) const
{
    return std::size_t(ColumnBase::aggregate<T, int64_t, act_Count, Equal>(target, 0, size(), 0));
}

template<class T>
typename BasicColumn<T>::SumType BasicColumn<T>::sum(std::size_t start, std::size_t end) const
{
    return ColumnBase::aggregate<T, SumType, act_Sum, None>(0, start, end, 0);
}

template<class T>
double BasicColumn<T>::average(std::size_t start, std::size_t end) const
{
    if (end == std::size_t(-1))
        end = size();
    std::size_t size = end - start;
    double sum1 = ColumnBase::aggregate<T, SumType, act_Sum, None>(0, start, end, 0);
    double avg = sum1 / ( size == 0 ? 1 : size );
    return avg;
}

template<class T>
T BasicColumn<T>::minimum(std::size_t start, std::size_t end) const
{
    return ColumnBase::aggregate<T, T, act_Min, None>(0, start, end, 0);
}

template<class T>
T BasicColumn<T>::maximum(std::size_t start, std::size_t end) const
{
    return ColumnBase::aggregate<T, T, act_Max, None>(0, start, end, 0);
}


template<class T> void BasicColumn<T>::do_insert(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    Array::TreeInsert<BasicColumn<T> > state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        BasicArray<T>* leaf = static_cast<BasicArray<T>*>(m_array);
        new_sibling_ref = leaf->btree_leaf_insert(ndx, value, state);
    }
    else {
        state.m_value = value;
        new_sibling_ref = m_array->btree_insert(ndx, state);
    }

    if (TIGHTDB_UNLIKELY(new_sibling_ref))
        introduce_new_root(new_sibling_ref, state);
}

template<class T> TIGHTDB_FORCEINLINE
ref_type BasicColumn<T>::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                     std::size_t ndx_in_parent,
                                     Allocator& alloc, std::size_t insert_ndx,
                                     Array::TreeInsert<BasicColumn<T> >& state)
{
    BasicArray<T> leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    return leaf.btree_leaf_insert(insert_ndx, state.m_value, state);
}


template<class T> inline std::size_t BasicColumn<T>::lower_bound(T value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        return static_cast<const BasicArray<T>*>(m_array)->lower_bound(value);
    }
    return ColumnBase::lower_bound(*this, value);
}

template<class T> inline std::size_t BasicColumn<T>::upper_bound(T value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        return static_cast<const BasicArray<T>*>(m_array)->upper_bound(value);
    }
    return ColumnBase::upper_bound(*this, value);
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_BASIC_TPL_HPP
