/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2016] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#ifndef REALM_UTIL_RING_BUFFER_HPP
#define REALM_UTIL_RING_BUFFER_HPP

#include <type_traits>
#include <limits>
#include <memory>
#include <iterator>
#include <utility>
#include <stdexcept>

#include <realm/util/safe_int_ops.hpp>

namespace realm {
namespace util {

/// \brief Double-ended queue based on a "circular buffer".
///
/// As opposed to std::deque, this implementation allows for reservation of
/// buffer space, such that pushing at either end can be guaranteed to not throw
/// in the case where size() < capacity(). It achieves this by using a single
/// contiguous chunk of memory as buffer.
///
/// Pushing at either end occurs in amortized constant time. Popping at eaither
/// end occurs in constant time.
template<class T> class RingBuffer {
private:
    template<class> class Iter;

public:
    static_assert(std::is_nothrow_destructible<T>::value, "T must be no-throw destructible");

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = T*;
    using const_pointer = const T*;

    ~RingBuffer() noexcept;

    bool empty() const noexcept;
    size_t size() const noexcept;
    size_t capacity() const noexcept;

    T& front() noexcept;
    const T& front() const noexcept;

    T& back() noexcept;
    const T& back() const noexcept;

    void push_front(const T&);
    void push_front(T&&);
    void push_back(const T&);
    void push_back(T&&);

    template<class... Args> void emplace_front(Args&&...);
    template<class... Args> void emplace_back(Args&&...);

    void pop_front() noexcept;
    void pop_back() noexcept;

    /// Leaves the capacity unchanged.
    void clear() noexcept;

    void reserve(size_t capacity);

    using iterator       = Iter<T>;
    using const_iterator = Iter<const T>;

    iterator begin() noexcept;
    const_iterator begin() const noexcept;

    iterator end() noexcept;
    const_iterator end() const noexcept;

    // Unimplemented:
    T& at(size_t);
    const T& at(size_t) const;
    T& operator[](size_t) noexcept;
    const T& operator[](size_t) const noexcept;
    void shrink_to_fit();
    template<class... Args>
    iterator emplace(const_iterator, Args&&...);
    iterator insert(const_iterator, const T&);
    iterator insert(const_iterator, size_t count, const T&);
    iterator insert(const_iterator, std::initializer_list<T>);

private:
    using Strut = typename std::aligned_storage<sizeof(T), alignof(T)>::type;
    std::unique_ptr<Strut[]> m_buffer;
    size_t m_offset = 0;
    size_t m_size = 0;
    size_t m_capacity = 0;
    // INVARIANT: m_size == 0 && m_capacity == 0 || m_size < m_capacity

    // Assumption: i < size
    // Assumption: v < size
    static size_t circular_inc(size_t i, size_t size) noexcept;
    static size_t circular_dec(size_t i, size_t size) noexcept;
    static size_t circular_add(size_t i, size_t v, size_t size) noexcept;
};




// Implementation

// FIXME: Should be extended to a random access iterator
template<class T> template<class U> class RingBuffer<T>::Iter:
        public std::iterator<std::bidirectional_iterator_tag, U> {
public:
    template<class V> Iter(const Iter<V>& i):
        m_p(i.m_p),
        m_capacity(i.m_capacity),
        m_buffer(i.m_buffer)
    {
    }
    U& operator*() const noexcept
    {
        return m_buffer[m_p];
    }
    U* operator->() const noexcept
    {
        return &m_buffer[m_p];
    }
    Iter& operator++() noexcept
    {
        m_p = circular_inc(m_p, m_capacity);
        return *this;
    }
    Iter& operator--() noexcept
    {
        m_p = circular_dec(m_p, m_capacity);
        return *this;
    }
    Iter operator++(int) noexcept
    {
        size_t p = m_p;
        operator++();
        return Iter(p, m_capacity, m_buffer);
    }
    Iter operator--(int) noexcept
    {
        size_t p = m_p;
        operator--();
        return Iter(p, m_capacity, m_buffer);
    }
    bool operator==(const Iter& i) const noexcept
    {
        return m_p == i.m_p;
    }
    bool operator!=(const Iter& i) const noexcept
    {
        return m_p != i.m_p;
    }
private:
    size_t m_p;
    size_t m_capacity;
    U* m_buffer;
    Iter(size_t p, size_t capacity, U* buffer):
        m_p(p),
        m_capacity(capacity),
        m_buffer(buffer)
    {
    }
    friend class RingBuffer<T>;
    template<class> friend class Iter;
};

template<class T>
inline RingBuffer<T>::~RingBuffer() noexcept
{
    clear();
}

template<class T>
inline bool RingBuffer<T>::empty() const noexcept
{
    return m_size == 0;
}

template<class T>
inline size_t RingBuffer<T>::size() const noexcept
{
    return m_size;
}

template<class T>
inline size_t RingBuffer<T>::capacity() const noexcept
{
    return (m_capacity > 0 ? m_capacity - 1 : 0);
}

template<class T>
inline T& RingBuffer<T>::front() noexcept
{
    REALM_ASSERT(m_size > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    return buffer_2[m_offset];
}

template<class T>
inline const T& RingBuffer<T>::front() const noexcept
{
    return const_cast<RingBuffer*>(this)->front();
}

template<class T>
inline T& RingBuffer<T>::back() noexcept
{
    REALM_ASSERT(m_size > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t p = circular_add(m_offset, m_size-1, m_capacity);
    return buffer_2[p];
}

template<class T>
inline const T& RingBuffer<T>::back() const noexcept
{
    return const_cast<RingBuffer*>(this)->back();
}

template<class T>
void RingBuffer<T>::push_front(const T& value)
{
    emplace_front(value);
}

template<class T>
void RingBuffer<T>::push_front(T&& value)
{
    emplace_front(std::move(value));
}

template<class T>
void RingBuffer<T>::push_back(const T& value)
{
    emplace_back(value);
}

template<class T>
void RingBuffer<T>::push_back(T&& value)
{
    emplace_back(std::move(value));
}

template<class T>
template<class... Args> inline void RingBuffer<T>::emplace_front(Args&&... args)
{
    size_t new_size = m_size;
    if (int_add_with_overflow_detect(new_size, 1))
        throw std::overflow_error("Size");
    reserve(new_size); // Throws
    REALM_ASSERT(m_capacity > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t new_offset = circular_dec(m_offset, m_capacity);
    new (&buffer_2[new_offset]) T(std::forward<Args>(args)...); // Throws
    m_offset = new_offset;
    m_size = new_size;
}

template<class T>
template<class... Args> inline void RingBuffer<T>::emplace_back(Args&&... args)
{
    size_t new_size = m_size;
    if (int_add_with_overflow_detect(new_size, 1))
        throw std::overflow_error("Size");
    reserve(new_size); // Throws
    REALM_ASSERT(m_capacity > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t new_index = circular_add(m_offset, m_size, m_capacity);
    new (&buffer_2[new_index]) T(std::forward<Args>(args)...); // Throws
    m_size = new_size;
}

template<class T>
inline void RingBuffer<T>::pop_front() noexcept
{
    REALM_ASSERT(m_size > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t p = m_offset;
    buffer_2[p].~T();
    m_offset = circular_inc(m_offset, m_capacity);
    --m_size;
}

template<class T>
inline void RingBuffer<T>::pop_back() noexcept
{
    REALM_ASSERT(m_size > 0);
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t new_size = m_size - 1;
    size_t p = circular_add(m_offset, new_size, m_capacity);
    buffer_2[p].~T();
    m_size = new_size;
}

template<class T>
inline void RingBuffer<T>::clear() noexcept
{
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t p = m_offset;
    for (size_t i = 0; i < m_size; ++i) {
        buffer_2[p].~T();
        p = circular_inc(p, m_capacity);
    }
    m_offset = 0;
    m_size = 0;
}

template<class T>
void RingBuffer<T>::reserve(size_t capacity)
{
    // An extra element of capacity is needed such that the end iterator can
    // always point one beyond the last element.
    size_t capacity_2 = capacity;
    if (capacity_2 > 0 && int_add_with_overflow_detect(capacity_2, 1))
        throw std::overflow_error("Capacity");

    if (capacity_2 <= m_capacity)
        return;

    // Allocate new buffer
    size_t new_capacity = m_capacity;
    // FIXME: Doubling the capacity has pathological behavior leading to increased
    // memory pressure, due to later reallocations never being able to reuse the
    // combined blocks of previous allocations. Multiplying by ~1.5 is much better,
    // because the 3rd reallocation is then able to fit in the memory of the 1st + 2nd
    // allocations.
    if (int_multiply_with_overflow_detect(new_capacity, 2))
        new_capacity = std::numeric_limits<size_t>::max();
    if (new_capacity < capacity_2)
        new_capacity = capacity_2;
    std::unique_ptr<Strut[]> new_buffer(new Strut[new_capacity]); // Throws
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());

    // Move or copy elements to new buffer
    {
        T* new_buffer_2 = reinterpret_cast<T*>(new_buffer.get());
        size_t i = 0;
        try {
            size_t p = m_offset;
            while (i < m_size) {
                new (&new_buffer_2[i]) T(std::move_if_noexcept(buffer_2[p])); // Throws
                ++i;
                p = circular_inc(p, m_capacity);
            }
        }
        catch (...) {
            // Back out in case of a throwing move or throwing copy
            for (size_t p = 0; p < i; ++p)
                new_buffer_2[p].~T();
            throw;
        }
    }

    // Destroy old elements
    {
        size_t p = m_offset;
        for (size_t i = 0; i < m_size; ++i) {
            buffer_2[p].~T();
            p = circular_inc(p, m_capacity);
        }
    }

    m_buffer = std::move(new_buffer);
    m_offset = 0;
    m_capacity = new_capacity;
}

template<class T>
inline typename RingBuffer<T>::iterator RingBuffer<T>::begin() noexcept
{
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t p = m_offset;
    return iterator(p, m_capacity, buffer_2);
}

template<class T>
inline typename RingBuffer<T>::const_iterator RingBuffer<T>::begin() const noexcept
{
    return const_cast<RingBuffer*>(this)->begin();
}

template<class T>
inline typename RingBuffer<T>::iterator RingBuffer<T>::end() noexcept
{
    T* buffer_2 = reinterpret_cast<T*>(m_buffer.get());
    size_t p = circular_add(m_offset, m_size, m_capacity);
    return iterator(p, m_capacity, buffer_2);
}

template<class T>
inline typename RingBuffer<T>::const_iterator RingBuffer<T>::end() const noexcept
{
    return const_cast<RingBuffer*>(this)->end();
}

template<class T>
inline size_t RingBuffer<T>::circular_inc(size_t i, size_t size) noexcept
{
    return i == size-1 ? 0 : i+1;
}

template<class T>
inline size_t RingBuffer<T>::circular_dec(size_t i, size_t size) noexcept
{
    return i == 0 ? size-1 : i-1;
}

template<class T>
inline size_t RingBuffer<T>::circular_add(size_t i, size_t v, size_t size) noexcept
{
    size_t n = size - i;
    return v < n ? i+v : v-n;
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_RING_BUFFER_HPP

