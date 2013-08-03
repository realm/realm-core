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
#ifndef TIGHTDB_ARRAY_BASIC_TPL_HPP
#define TIGHTDB_ARRAY_BASIC_TPL_HPP

#include <algorithm>

namespace tightdb {

template<class T>
inline ref_type BasicArray<T>::create_empty_basic_array(Allocator& alloc)
{
    std::size_t capacity = Array::initial_capacity;
    MemRef mem_ref = alloc.alloc(capacity); // Throws

    bool is_leaf = true;
    bool has_refs = false;
    int width = sizeof (T);
    std::size_t size = 0;
    init_header(mem_ref.m_addr, is_leaf, has_refs, wtype_Multiply, width, size, capacity);

    return mem_ref.m_ref;
}

template<class T>
inline BasicArray<T>::BasicArray(ArrayParent* parent, std::size_t ndx_in_parent, Allocator& alloc):
    Array(alloc)
{
    ref_type ref = create_empty_basic_array(alloc); // Throws
    init_from_ref(ref);
    set_parent(parent, ndx_in_parent);
    update_ref_in_parent();
}

template<class T>
inline BasicArray<T>::BasicArray(MemRef mem, ArrayParent* parent, std::size_t ndx_in_parent,
                                 Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_mem(mem);
    set_parent(parent, ndx_in_parent);
}

template<class T>
inline BasicArray<T>::BasicArray(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                 Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    set_parent(parent, ndx_in_parent);
}

template<class T>
inline BasicArray<T>::BasicArray(no_prealloc_tag) TIGHTDB_NOEXCEPT: Array(no_prealloc_tag()) {}


template<class T>
inline void BasicArray<T>::clear()
{
    copy_on_write(); // Throws

    // Truncate size to zero (but keep capacity and width)
    m_size = 0;
    set_header_size(0);
}

template<class T>
inline void BasicArray<T>::add(T value)
{
    insert(m_size, value);
}


template<class T> inline T BasicArray<T>::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return *(reinterpret_cast<T*>(m_data) + ndx);
}


template<class T>
inline T BasicArray<T>::get(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT
{
    const char* data = get_data_from_header(header);
    return *(reinterpret_cast<const T*>(data) + ndx);
}


template<class T>
inline void BasicArray<T>::set(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Set the value
    T* data = reinterpret_cast<T*>(m_data) + ndx;
    *data = value;
}

template<class T>
void BasicArray<T>::insert(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // Make room for the new value
    alloc(m_size+1, m_width); // Throws

    // Move values below insertion
    if (ndx != m_size) {
        char* base = reinterpret_cast<char*>(m_data);
        char* src_begin = base + ndx*m_width;
        char* src_end   = base + m_size*m_width;
        char* dst_end   = src_end + m_width;
        std::copy_backward(src_begin, src_end, dst_end);
    }

    // Set the value
    T* data = reinterpret_cast<T*>(m_data) + ndx;
    *data = value;

     ++m_size;
}

template<class T>
void BasicArray<T>::erase(std::size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_size);

    // Check if we need to copy before modifying
    copy_on_write(); // Throws

    // move data under deletion up
    if (ndx < m_size-1) {
        char* base = reinterpret_cast<char*>(m_data);
        char* dst_begin = base + ndx*m_width;
        const char* src_begin = dst_begin + m_width;
        const char* src_end   = base + m_size*m_width;
        std::copy(src_begin, src_end, dst_begin);
    }

    // Update size (also in header)
    --m_size;
    set_header_size(m_size);
}

template<class T>
bool BasicArray<T>::compare_entries(const BasicArray<T>& c) const
{
    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


template<class T>
std::size_t BasicArray<T>::CalcByteLen(std::size_t count, std::size_t) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/safe_int_ops.hpp>
    return header_size + (count * sizeof (T));
}

template<class T>
std::size_t BasicArray<T>::CalcItemCount(std::size_t bytes, std::size_t) const TIGHTDB_NOEXCEPT
{
    // fixme: ??? what about width = 0? return -1?

    std::size_t bytes_without_header = bytes - header_size;
    return bytes_without_header / sizeof (T);
}

template<class T>
std::size_t BasicArray<T>::Find(T target, std::size_t start, std::size_t end) const
{
    if (end == std::size_t(-1))
        end = m_size;
    if (start >= end)
        return not_found;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);
    if (m_size == 0)
        return not_found; // empty list

    for (std::size_t i = start; i < end; ++i) {
        if (target == get(i))
            return i;
    }
    return not_found;
}

template<class T>
std::size_t BasicArray<T>::find_first(T value, std::size_t start, std::size_t end) const
{
    return Find(value, start, end);
}

template<class T>
void BasicArray<T>::find_all(Array& result, T value, std::size_t add_offset,
                             std::size_t start, std::size_t end)
{
    std::size_t first = start - 1;
    for (;;) {
        first = Find(value, first + 1, end);
        if (first != not_found)
            result.add(first + add_offset);
        else
            break;
    }
}

template<class T>
std::size_t BasicArray<T>::count(T value, std::size_t start, std::size_t end) const
{
    std::size_t count = 0;
    std::size_t lastmatch = start - 1;
    for (;;) {
        lastmatch = Find(value, lastmatch+1, end);
        if (lastmatch != not_found)
            ++count;
        else
            break;
    }
    return count;
}

#if 0
// currently unused
template<class T>
double BasicArray<T>::sum(std::size_t start, std::size_t end) const
{
    if (end == std::size_t(-1))
        end = m_size;
    if (m_size == 0)
        return 0;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    R sum = 0;
    for (std::size_t i = start; i < end; ++i) {
        sum += get(i);
    }
    return sum;
}
#endif

template<class T> template<bool find_max>
bool BasicArray<T>::minmax(T& result, std::size_t start, std::size_t end) const
{
    if (end == std::size_t(-1))
        end = m_size;
    if (m_size == 0)
        return false;
    TIGHTDB_ASSERT(start < m_size && end <= m_size && start < end);

    T m = get(start);
    ++start;
    for (; start < end; ++start) {
        T val = get(start);
        if (find_max ? val > m : val < m)
            m = val;
    }
    result = m;
    return true;
}

template<class T>
bool BasicArray<T>::maximum(T& result, std::size_t start, std::size_t end) const
{
    return minmax<true>(result, start, end);
}

template<class T>
bool BasicArray<T>::minimum(T& result, std::size_t start, std::size_t end) const
{
    return minmax<false>(result, start, end);
}


template<class T>
ref_type BasicArray<T>::btree_leaf_insert(size_t ndx, T value, TreeInsertBase& state)
{
    size_t leaf_size = size();
    TIGHTDB_ASSERT(leaf_size <= TIGHTDB_MAX_LIST_SIZE);
    if (leaf_size < ndx) ndx = leaf_size;
    if (TIGHTDB_LIKELY(leaf_size < TIGHTDB_MAX_LIST_SIZE)) {
        insert(ndx, value);
        return 0; // Leaf was not split
    }

    // Split leaf node
    BasicArray<T> new_leaf(0, 0, get_alloc());
    if (ndx == leaf_size) {
        new_leaf.add(value);
        state.m_split_offset = ndx;
    }
    else {
        // FIXME: Could be optimized by first resizing the target
        // array, then copy elements with std::copy().
        for (size_t i = ndx; i != leaf_size; ++i) {
            new_leaf.add(get(i));
        }
        resize(ndx);
        add(value);
        state.m_split_offset = ndx + 1;
    }
    state.m_split_size = leaf_size + 1;
    return new_leaf.get_ref();
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BASIC_TPL_HPP
