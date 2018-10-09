/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#ifndef REALM_UTIL_ALLOCATOR_HPP
#define REALM_UTIL_ALLOCATOR_HPP

#include <cstdlib>
#include <memory>
#include <realm/util/backtrace.hpp>

namespace realm {
namespace util {

/// Dynamic heap allocation interface.
///
/// Implementors may optionally implement a static method `get_default()`, which
/// should return a reference to an allocator instance. This allows
/// `STLAllocator` to be default-constructed.
///
/// NOTE: This base class is not related to the `realm::Allocator` interface,
/// which is used in the context of allocating memory inside a Realm file.
struct AllocatorBase {
    static constexpr std::size_t max_alignment = 16; // FIXME: This is arch-dependent

    /// Allocate \a size bytes at aligned at \a align.
    ///
    /// May throw `std::bad_alloc` if allocation fails. May **NOT** return
    /// an invalid pointer (such as `nullptr`).
    virtual void* allocate(std::size_t size, std::size_t align) = 0;

    /// Free the previously allocated block of memory. \a size is not required
    /// to be accurate, and is only provided for statistics and debugging
    /// purposes.
    ///
    /// \a ptr may be `nullptr`, in which case this shall be a noop.
    virtual void free(void* ptr, size_t size) noexcept = 0;
};

/// Implementation of AllocatorBase that uses malloc()/free().
struct DefaultAllocator : AllocatorBase {
    static DefaultAllocator& get_default() noexcept;

    void* allocate(std::size_t size, std::size_t align) final;
    void free(void* ptr, std::size_t size) noexcept final;

private:
    static DefaultAllocator g_instance;
    DefaultAllocator()
    {
    }
};

template <class T, class Allocator = AllocatorBase>
struct STLDeleter;

/// STL-compatible static dispatch bridge to a dynamic implementation of
/// `AllocatorBase`. Wraps a pointer to an object that adheres to the
/// `AllocatorBase` interface. It is optional whether the `Allocator` class
/// template argument actually derives from `AllocatorBase`.
///
/// The intention is that users of this class can set `Allocator` to the
/// nearest-known base class of the expected allocator implementations, such
/// that appropriate devirtualization can take place.
template <class T, class Allocator = AllocatorBase>
struct STLAllocator {
    using value_type = T;
    using Deleter = STLDeleter<T, Allocator>;

    /// The default constructor is only availble when the static method
    /// `Allocator::get_default()` exists.
    STLAllocator() noexcept
        : m_allocator(&Allocator::get_default())
    {
    }

    constexpr STLAllocator(Allocator& base) noexcept
        : m_allocator(&base)
    {
    }
    template <class U>
    constexpr STLAllocator(const STLAllocator<U, Allocator>& other) noexcept
        : m_allocator(other.m_allocator)
    {
    }

    T* allocate(std::size_t n)
    {
        static_assert(alignof(T) <= Allocator::max_alignment, "Over-aligned allocation");
        void* ptr = m_allocator->allocate(sizeof(T) * n, alignof(T));
        return static_cast<T*>(ptr);
    }

    void deallocate(T* ptr, std::size_t n) noexcept
    {
        m_allocator->free(ptr, sizeof(T) * n);
    }

    bool operator==(const STLAllocator& other) const
    {
        return m_allocator == other.m_allocator;
    }

    bool operator!=(const STLAllocator& other) const
    {
        return m_allocator != other.m_allocator;
    }

    operator Allocator&() const
    {
        return *m_allocator;
    }

    template <class U>
    struct rebind {
        using other = STLAllocator<U, Allocator>;
    };

private:
    template <class U, class A>
    friend struct STLAllocator;
    Allocator* m_allocator;
};

template <class T, class Allocator>
struct STLDeleter {
    size_t m_size;
    Allocator& m_allocator;
    explicit STLDeleter(Allocator& allocator)
        : STLDeleter(0, allocator)
    {
    }
    explicit STLDeleter(size_t size, Allocator& allocator)
        : m_size(size)
        , m_allocator(allocator)
    {
    }

    template <class U>
    STLDeleter(const STLDeleter<U, Allocator>& other)
        : m_size(other.m_size)
        , m_allocator(other.m_allocator)
    {
    }

    Allocator& get_allocator() const
    {
        return *m_allocator;
    }

    void operator()(T* ptr)
    {
        ptr->~T();
        m_allocator.free(ptr, m_size);
    }
};

template <class T, class Allocator>
struct STLDeleter<T[], Allocator> {
    // Note: Array-allocated pointers cannot be upcast to base classes, because
    // of array slicing.
    size_t m_count;
    Allocator* m_allocator;
    explicit STLDeleter(Allocator& allocator)
        : STLDeleter(0, allocator)
    {
    }
    explicit STLDeleter(size_t count, Allocator& allocator)
        : m_count(count)
        , m_allocator(&allocator)
    {
    }

    STLDeleter(const STLDeleter& other) = default;
    STLDeleter& operator=(const STLDeleter&) = default;

    Allocator& get_allocator() const
    {
        return *m_allocator;
    }

    void operator()(T* ptr)
    {
        for (size_t i = 0; i < m_count; ++i) {
            ptr[i].~T();
        }
        m_allocator->free(ptr, m_count * sizeof(T));
    }
};

/// make_unique with custom allocator (non-array version)
template <class T, class Allocator = DefaultAllocator, class... Args>
auto make_unique(Allocator& allocator, Args&&... args)
    -> std::enable_if_t<!std::is_array<T>::value, std::unique_ptr<T, STLDeleter<T, Allocator>>>
{
    void* memory = allocator.allocate(sizeof(T), alignof(T)); // Throws
    T* ptr;
    try {
        ptr = new (memory) T(std::forward<Args>(args)...); // Throws
    }
    catch (...) {
        allocator.free(memory, sizeof(T));
        throw;
    }
    std::unique_ptr<T, STLDeleter<T, Allocator>> result{ptr, STLDeleter<T, Allocator>{sizeof(T), allocator}};
    return result;
}

/// make_unique with custom allocator (array version)
template <class Tv, class Allocator = DefaultAllocator>
auto make_unique(Allocator& allocator, size_t count)
    -> std::enable_if_t<std::is_array<Tv>::value, std::unique_ptr<Tv, STLDeleter<Tv, Allocator>>>
{
    using T = std::remove_extent_t<Tv>;
    void* memory = allocator.allocate(sizeof(T) * count, alignof(T)); // Throws
    T* ptr;
    try {
        ptr = new (memory) T[count]; // Throws
    }
    catch (...) {
        allocator.free(memory, sizeof(T) * count);
        throw;
    }
    std::unique_ptr<T[], STLDeleter<T[], Allocator>> result{ptr, STLDeleter<T[], Allocator>{count, allocator}};
    return result;
}


// Implementation:

inline DefaultAllocator& DefaultAllocator::get_default() noexcept
{
    return g_instance;
}

inline void* DefaultAllocator::allocate(std::size_t size, std::size_t)
{
    void* ptr = std::malloc(size);
    if (ptr == nullptr)
        throw util::bad_alloc{};
    return ptr;
}

inline void DefaultAllocator::free(void* ptr, std::size_t) noexcept
{
    std::free(ptr);
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_ALLOCATOR_HPP
