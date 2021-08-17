/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#pragma once

#include <functional>
#include <type_traits>

#include <realm/util/assert.hpp>

namespace realm::util {

/*
 * IntrusivePtr is a lightweight smart-pointer for types that manage their own lifetime (usually with
 * a reference-counter). IntrusivePtr relies on unqualified calls to
 *
 *   void intrusive_ptr_add_ref(T* p);
 *   void intrusive_ptr_release(T* p);
 *
 * These functions should be defined in the same namespace as T, or as friend functions of class T.
 *
 * The implementation of intrusive_ptr_release should destroy the object when there are no further
 * references to the object. IntrusivePtr does not do any memory management on its own.
 *
 * When should I use an IntrusivePtr instead of a std::shared_ptr?
 * - If the type you're managing already has reference counting - for example the CF types from cocoa and
 *   some OpenSSL types.
 * - You don't want to add the overhead of a control block and deleter of std::shared_ptr, i.e. your type
 *   is small, short-lived, and gets created a lot.
 *
 * IntrusivePtr has the same memory overhead as a raw pointer.
 */
template <typename T>
class IntrusivePtr {
public:
    using element_type = T;

    /*
     * Cnstructs an IntrusivePtr holding a nullptr.
     */
    IntrusivePtr() noexcept = default;

    /*
     * Constructs an IntrusivePtr from a raw pointer to a T and adds a reference to the pointed-to-object
     * if the pointer is not null. If you don't need to increment the reference count - for example because
     * the object was just constructed and the reference count is already `1`, you can pass false to
     * `add_ref`.
     */
    IntrusivePtr(T* ptr, bool add_ref = true)
        : m_ptr(ptr)
    {
        if (m_ptr != nullptr && add_ref) {
            intrusive_ptr_add_ref(m_ptr);
        }
    }

    ~IntrusivePtr()
    {
        if (m_ptr != nullptr) {
            intrusive_ptr_release(m_ptr);
        }
    }

    /*
     * You can copy construct/assign IntrusivePtr's from other IntrusivePtr's, including from IntrusivePtr's with
     * type U, if U is convertable to T.
     */
    IntrusivePtr(IntrusivePtr const& other)
        : m_ptr(other.get())
    {
        if (m_ptr != nullptr) {
            intrusive_ptr_add_ref(m_ptr);
        }
    }

    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
    IntrusivePtr(IntrusivePtr<U> const& other)
        : m_ptr(other.get())
    {
        if (m_ptr != nullptr) {
            intrusive_ptr_add_ref(m_ptr);
        }
    }

    IntrusivePtr& operator=(IntrusivePtr const& other)
    {
        IntrusivePtr(other).swap(*this);
        return *this;
    }

    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
    IntrusivePtr& operator=(IntrusivePtr<U> const& other)
    {
        IntrusivePtr(static_cast<IntrusivePtr<U>&&>(other)).swap(*this);
        return *this;
    }

    /*
     * You can move construct/assign IntrusivePtr's from other IntrusivePtr's, including from IntrusivePtr's with
     * type U, if U is convertable to T.
     */
    IntrusivePtr(IntrusivePtr&& other)
        : m_ptr(other.get())
    {
        other.m_ptr = nullptr;
    }

    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
    IntrusivePtr(IntrusivePtr<U>&& other)
        : m_ptr(other.get())
    {
        other.m_ptr = nullptr;
    }

    IntrusivePtr& operator=(IntrusivePtr&& other)
    {
        IntrusivePtr(static_cast<IntrusivePtr&&>(other)).swap(*this);
        return *this;
    }

    template <typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
    IntrusivePtr& operator=(IntrusivePtr<U>&& other)
    {
        IntrusivePtr(static_cast<IntrusivePtr<U>&&>(other)).swap(*this);
        return *this;
    }

    /*
     * Makes this IntrusivePtr point to nothing, decrementing the reference count of anything this pointed to before.
     */
    void reset()
    {
        IntrusivePtr().swap(*this);
    }

    /*
     * Replaces the pointer this IntrusivePtr points to with a new pointer, decrementing the reference count of
     * anything this pointed to before. There is also an overload that allows you to control whether the reference
     * count is incremented as a side-effect of this call.
     */
    void reset(T* other)
    {
        IntrusivePtr(other).swap(*this);
    }

    void reset(T* other, bool add_ref)
    {
        IntrusivePtr(other, add_ref).swap(*this);
    }

    /*
     * Accessors allowing you to dereference/get the raw pointer this IntrusivePtr manages.
     */
    T& operator*() const noexcept
    {
        REALM_ASSERT(m_ptr != nullptr);
        return *m_ptr;
    }

    T* operator->() const noexcept
    {
        REALM_ASSERT(m_ptr != nullptr);
        return m_ptr;
    }

    T* get() const noexcept
    {
        return m_ptr;
    }

    /*
     * Makes this IntrusivePtr point to nothing and returns the pointer it contains. This is the equivelant of the
     * release method on std::unique_ptr.
     */
    T* release() noexcept
    {
        T* ret = m_ptr;
        m_ptr = nullptr;
        return ret;
    }

    /*
     * Returns true if the pointer this manages is not nullptr.
     */
    explicit operator bool() const noexcept
    {
        return m_ptr != nullptr;
    }

    void swap(IntrusivePtr& other) noexcept
    {
        std::swap(m_ptr, other.m_ptr);
    }

private:
    T* m_ptr = nullptr;
};

// Comparison functions between other IntrusivePtr's, raw pointers, and nullptr.
template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator==(IntrusivePtr<T> const& lhs, IntrusivePtr<U> const& rhs) noexcept
{
    return lhs.get() == rhs.get();
}

template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator!=(IntrusivePtr<T> const& lhs, IntrusivePtr<U> const& rhs) noexcept
{
    return lhs.get() != rhs.get();
}

template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator==(IntrusivePtr<T> const& lhs, U* rhs) noexcept
{
    return lhs.get() == rhs;
}

template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator!=(IntrusivePtr<T> const& lhs, U* rhs) noexcept
{
    return lhs.get() != rhs;
}

template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator==(T* lhs, IntrusivePtr<U> const& rhs) noexcept
{
    return lhs == rhs.get();
}

template <typename T, typename U, typename std::enable_if_t<std::is_convertible_v<U, T>, bool> = true>
inline bool operator!=(T* lhs, IntrusivePtr<U> const& rhs) noexcept
{
    return lhs != rhs.get();
}

template <typename T>
inline bool operator==(IntrusivePtr<T> const& ptr, nullptr_t) noexcept
{
    return ptr.get() == nullptr;
}

template <typename T>
inline bool operator!=(IntrusivePtr<T> const& ptr, nullptr_t) noexcept
{
    return ptr.get() != nullptr;
}

template <typename T>
inline bool operator==(nullptr_t, IntrusivePtr<T> const& ptr) noexcept
{
    return ptr.get() == nullptr;
}

template <typename T>
inline bool operator!=(nullptr_t, IntrusivePtr<T> const& ptr) noexcept
{
    return ptr.get() != nullptr;
}

// Implements operator< so you can store IntrusivePtr's in sets/maps.
template <typename T>
inline bool operator<(IntrusivePtr<T>& lhs, IntrusivePtr<T>& rhs) noexcept
{
    return std::less<T*>()(lhs.get(), rhs.get());
}

// Implements std::swap specialization.
template <typename T>
void swap(IntrusivePtr<T>& lhs, IntrusivePtr<T>& rhs)
{
    lhs.swap(rhs);
}

} // namespace realm::util

// Implements std::hash so you can store IntrusivePtr's in unordered sets/maps.
namespace std {
template <typename T>
struct hash<::realm::util::IntrusivePtr<T>> {
    std::size_t operator()(::realm::util::IntrusivePtr<T> const& p) const noexcept
    {
        return std::hash<T*>()(p.get());
    }
};

} // namespace std
