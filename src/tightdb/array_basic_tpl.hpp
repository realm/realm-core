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

template<typename T>
inline ref_type BasicArray<T>::create_empty_basic_array(Allocator& alloc)
{
    std::size_t capacity = Array::initial_capacity;
    MemRef mem_ref = alloc.Alloc(capacity); // Throws

    init_header(static_cast<char*>(mem_ref.pointer), false, false, wtype_Multiply,
                sizeof (T), 0, capacity);

    return mem_ref.ref;
}

template<typename T>
inline BasicArray<T>::BasicArray(ArrayParent* parent, std::size_t ndx_in_parent, Allocator& alloc):
    Array(alloc)
{
    ref_type ref = create_empty_basic_array(alloc); // Throws
    init_from_ref(ref);
    set_parent(parent, ndx_in_parent);
    update_ref_in_parent();
}

template<typename T>
inline BasicArray<T>::BasicArray(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                                 Allocator& alloc) TIGHTDB_NOEXCEPT: Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    set_parent(const_cast<ArrayParent*>(parent), ndx_in_parent);
}

template<typename T>
inline BasicArray<T>::BasicArray(no_prealloc_tag) TIGHTDB_NOEXCEPT: Array(no_prealloc_tag()) {}


template<typename T>
inline void BasicArray<T>::clear()
{
    CopyOnWrite(); // Throws

    // Truncate size to zero (but keep capacity and width)
    m_len = 0;
    set_header_len(0);
}

template<typename T>
inline void BasicArray<T>::add(T value)
{
    insert(m_len, value);
}


template<typename T> inline T BasicArray<T>::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return *(reinterpret_cast<T*>(m_data) + ndx);
}


template<class T>
inline T BasicArray<T>::column_get(const Array* root, std::size_t ndx) TIGHTDB_NOEXCEPT
{
    if (root->is_leaf()) return static_cast<const BasicArray*>(root)->get(ndx);
    std::pair<const char*, std::size_t> p = find_leaf(root, ndx);
    const char* data = get_data_from_header(p.first);
    return *(reinterpret_cast<const T*>(data) + p.second);
}


template<typename T>
inline void BasicArray<T>::set(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Set the value
    T* data = reinterpret_cast<T*>(m_data) + ndx;
    *data = value;
}

template<typename T>
void BasicArray<T>::insert(std::size_t ndx, T value)
{
    TIGHTDB_ASSERT(ndx <= m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // Make room for the new value
    Alloc(m_len+1, m_width); // Throws

    // Move values below insertion
    if (ndx != m_len) {
        char* const base = reinterpret_cast<char*>(m_data);
        char* const src_begin = base + ndx*m_width;
        char* const src_end   = base + m_len*m_width;
        char* const dst_end   = src_end + m_width;
        std::copy_backward(src_begin, src_end, dst_end);
    }

    // Set the value
    T* data = reinterpret_cast<T*>(m_data) + ndx;
    *data = value;

     ++m_len;
}

template<typename T>
void BasicArray<T>::erase(std::size_t ndx)
{
    TIGHTDB_ASSERT(ndx < m_len);

    // Check if we need to copy before modifying
    CopyOnWrite(); // Throws

    // move data under deletion up
    if (ndx < m_len-1) {
        char* const base = reinterpret_cast<char*>(m_data);
        char* const dst_begin = base + ndx*m_width;
        const char* const src_begin = dst_begin + m_width;
        const char* const src_end   = base + m_len*m_width;
        std::copy(src_begin, src_end, dst_begin);
    }

    // Update length (also in header)
    --m_len;
    set_header_len(m_len);
}

template<typename T>
bool BasicArray<T>::Compare(const BasicArray<T>& c) const
{
    for (size_t i = 0; i < size(); ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


template<class T>
inline void BasicArray<T>::foreach(ForEachOp<T>* op) const TIGHTDB_NOEXCEPT
{
    foreach(this, op);
}


template<class T>
inline void BasicArray<T>::foreach(const Array* a, ForEachOp<T>* op) TIGHTDB_NOEXCEPT
{
    const T* begin = reinterpret_cast<T*>(a->m_data);
    op->handle_chunk(begin, begin + a->size());
}


template<typename T>
size_t BasicArray<T>::CalcByteLen(size_t count, size_t /*width*/) const
{
    // FIXME: This arithemtic could overflow. Consider using <tightdb/safe_int_ops.hpp>
    return 8 + (count * sizeof(T));
}

template<typename T>
size_t BasicArray<T>::CalcItemCount(size_t bytes, size_t /*width*/) const TIGHTDB_NOEXCEPT
{
    // fixme: ??? what about width = 0? return -1?

    const size_t bytes_without_header = bytes - 8;
    return bytes_without_header / sizeof(T);
}

template<typename T>
size_t BasicArray<T>::Find(T target, size_t start, size_t end) const
{
    if (end == (size_t)-1)
        end = m_len;
    if (start >= end)
        return not_found;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);
    if (m_len == 0)
        return not_found; // empty list

    for (size_t i = start; i < end; ++i) {
        if (target == get(i))
            return i;
    }
    return not_found;
}

template<typename T>
size_t BasicArray<T>::find_first(T value, size_t start, size_t end) const
{
    return Find(value, start, end);
}

template<typename T>
void BasicArray<T>::find_all(Array& result, T value, size_t add_offset, size_t start, size_t end)
{
    size_t first = start - 1;
    for (;;) {
        first = Find(value, first + 1, end);
        if (first != not_found)
            result.add(first + add_offset);
        else
            break;
    }
}

template<typename T>
size_t BasicArray<T>::count(T value, size_t start, size_t end) const
{
    size_t count = 0;
    size_t lastmatch = start - 1;
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
template<typename T>
double BasicArray<T>::sum(size_t start, size_t end) const
{
    if (end == (size_t)-1)
        end = m_len;
    if (m_len == 0)
        return 0;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);

    R sum = 0;
    for (size_t i = start; i < end; ++i) {
        sum += get(i);
    }
    return sum;
}
#endif

template <typename T> template<bool find_max>
bool BasicArray<T>::minmax(T& result, size_t start, size_t end) const
{
    if (end == (size_t)-1)
        end = m_len;
    if (m_len == 0)
        return false;
    TIGHTDB_ASSERT(start < m_len && end <= m_len && start < end);

    T m = get(start);
    ++start;
    for (; start < end; ++start) {
        const T val = get(start);
        if (find_max ? val > m : val < m)
            m = val;
    }
    result = m;
    return true;
}

template <typename T>
bool BasicArray<T>::maximum(T& result, size_t start, size_t end) const
{
    return minmax<true>(result, start, end);
}

template <typename T>
bool BasicArray<T>::minimum(T& result, size_t start, size_t end) const
{
    return minmax<false>(result, start, end);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_BASIC_TPL_HPP
