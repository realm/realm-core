/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#ifndef REALM_UTIL_BIND_PTR_HPP
#define REALM_UTIL_BIND_PTR_HPP

#include <algorithm>
#include <memory>
#include <atomic>
#include <ostream>
#include <utility>

#include <realm/util/features.h>
#include <realm/util/assert.hpp>


namespace realm {
namespace util {

struct bind_ptr_adopt_tag {};


/// A generic intrusive smart pointer that binds itself explicitly to
/// the target object.
///
/// This class is agnostic towards what 'binding' means for the target
/// object, but a common use is 'reference counting'. See RefCountBase
/// for an example of that.
///
/// This smart pointer implementation assumes that the target object
/// destructor never throws.
template <class T>
class bind_ptr {
public:
    constexpr bind_ptr() noexcept = default;
    ~bind_ptr() noexcept
    {
        unbind();
    }

    constexpr explicit bind_ptr(T* p) noexcept
    {
        bind(p);
    }
    template <class U>
    constexpr explicit bind_ptr(U* p) noexcept
    {
        bind(p);
    }

    constexpr bind_ptr(T* p, bind_ptr_adopt_tag) noexcept
    {
        m_ptr = p;
    }
    template <class U>
    constexpr bind_ptr(U* p, bind_ptr_adopt_tag) noexcept
    {
        m_ptr = p;
    }

    // Copy construct
    constexpr bind_ptr(const bind_ptr& p) noexcept
    {
        bind(p.m_ptr);
    }
    template <class U>
    constexpr bind_ptr(const bind_ptr<U>& p) noexcept
    {
        bind(p.m_ptr);
    }

    // Copy assign
    constexpr bind_ptr& operator=(const bind_ptr& p) noexcept
    {
        bind_ptr(p).swap(*this);
        return *this;
    }
    template <class U>
    constexpr bind_ptr& operator=(const bind_ptr<U>& p) noexcept
    {
        bind_ptr(p).swap(*this);
        return *this;
    }

    // Move construct
    constexpr bind_ptr(bind_ptr&& p) noexcept
        : m_ptr(p.release())
    {
    }
    template <class U>
    constexpr bind_ptr(bind_ptr<U>&& p) noexcept
        : m_ptr(p.release())
    {
    }

    // Move from std::unique_ptr
    constexpr bind_ptr(std::unique_ptr<T>&& p) noexcept
    {
        bind(p.release());
    }

    // Move assign
    constexpr bind_ptr& operator=(bind_ptr&& p) noexcept
    {
        bind_ptr(std::move(p)).swap(*this);
        return *this;
    }
    template <class U>
    constexpr bind_ptr& operator=(bind_ptr<U>&& p) noexcept
    {
        bind_ptr(std::move(p)).swap(*this);
        return *this;
    }

    //@{
    // Comparison
    template <class U>
    constexpr bool operator==(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator==(U*) const noexcept;

    template <class U>
    constexpr bool operator!=(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator!=(U*) const noexcept;

    template <class U>
    constexpr bool operator<(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator<(U*) const noexcept;

    template <class U>
    constexpr bool operator>(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator>(U*) const noexcept;

    template <class U>
    constexpr bool operator<=(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator<=(U*) const noexcept;

    template <class U>
    constexpr bool operator>=(const bind_ptr<U>&) const noexcept;

    template <class U>
    constexpr bool operator>=(U*) const noexcept;
    //@}

    // Dereference
    constexpr T& operator*() const noexcept
    {
        return *m_ptr;
    }
    constexpr T* operator->() const noexcept
    {
        return m_ptr;
    }

    constexpr explicit operator bool() const noexcept
    {
        return m_ptr != 0;
    }

    constexpr T* get() const noexcept
    {
        return m_ptr;
    }
    constexpr void reset() noexcept
    {
        bind_ptr().swap(*this);
    }
    constexpr void reset(T* p) noexcept
    {
        bind_ptr(p).swap(*this);
    }
    template <class U>
    constexpr void reset(U* p) noexcept
    {
        bind_ptr(p).swap(*this);
    }

    constexpr void reset(T* p, bind_ptr_adopt_tag) noexcept
    {
        bind_ptr(p, bind_ptr_adopt_tag{}).swap(*this);
    }

    template <class U>
    constexpr void reset(U* p, bind_ptr_adopt_tag) noexcept
    {
        bind_ptr(p, bind_ptr_adopt_tag{}).swap(*this);
    }

    constexpr T* release() noexcept
    {
        T* const p = m_ptr;
        m_ptr = nullptr;
        return p;
    }

    constexpr void swap(bind_ptr& p) noexcept
    {
        std::swap(m_ptr, p.m_ptr);
    }
    constexpr friend void swap(bind_ptr& a, bind_ptr& b) noexcept
    {
        a.swap(b);
    }

private:
    T* m_ptr = nullptr;

    constexpr void bind(T* p) noexcept
    {
        if (p)
            p->bind_ptr();
        m_ptr = p;
    }
    constexpr void unbind() noexcept
    {
        if (m_ptr)
            m_ptr->unbind_ptr();
    }

    template <class>
    friend class bind_ptr;
};

// Deduction guides
template <class T>
bind_ptr(T*) -> bind_ptr<T>;
template <class T>
bind_ptr(T*, bind_ptr_adopt_tag) -> bind_ptr<T>;

template <class T, typename... Args>
bind_ptr<T> make_bind(Args&&... __args)
{
    return bind_ptr<T>(new T(std::forward<Args>(__args)...));
}


template <class C, class T, class U>
inline std::basic_ostream<C, T>& operator<<(std::basic_ostream<C, T>& out, const bind_ptr<U>& p)
{
    out << static_cast<const void*>(p.get());
    return out;
}


//@{
// Comparison
template <class T, class U>
constexpr bool operator==(T*, const bind_ptr<U>&) noexcept;
template <class T, class U>
constexpr bool operator!=(T*, const bind_ptr<U>&) noexcept;
template <class T, class U>
constexpr bool operator<(T*, const bind_ptr<U>&) noexcept;
template <class T, class U>
constexpr bool operator>(T*, const bind_ptr<U>&) noexcept;
template <class T, class U>
constexpr bool operator<=(T*, const bind_ptr<U>&) noexcept;
template <class T, class U>
constexpr bool operator>=(T*, const bind_ptr<U>&) noexcept;
//@}


/// Polymorphic convenience base class for reference counting objects.
///
/// Together with bind_ptr, this class delivers simple instrusive
/// reference counting.
///
/// \sa bind_ptr
class RefCountBase {
public:
    RefCountBase() noexcept
        : m_ref_count(0)
    {
    }
    virtual ~RefCountBase() noexcept
    {
        REALM_ASSERT(m_ref_count == 0);
    }

    RefCountBase(const RefCountBase&)
        : m_ref_count(0)
    {
    }
    void operator=(const RefCountBase&) {}

protected:
    void bind_ptr() const noexcept
    {
        ++m_ref_count;
    }
    void unbind_ptr() const noexcept
    {
        if (--m_ref_count == 0)
            delete this;
    }

private:
    mutable unsigned long m_ref_count;

    template <class>
    friend class bind_ptr;
};


/// Same as RefCountBase, but this one makes the copying of, and the
/// destruction of counted references thread-safe.
///
/// \sa RefCountBase
/// \sa bind_ptr
class AtomicRefCountBase {
public:
    AtomicRefCountBase() noexcept
        : m_ref_count(0)
    {
    }
    virtual ~AtomicRefCountBase() noexcept
    {
        REALM_ASSERT(m_ref_count == 0);
    }

    AtomicRefCountBase(const AtomicRefCountBase&)
        : m_ref_count(0)
    {
    }
    void operator=(const AtomicRefCountBase&) {}

protected:
    // FIXME: Operators ++ and -- as used below use
    // std::memory_order_seq_cst. This can be optimized.
    void bind_ptr() const noexcept
    {
        ++m_ref_count;
    }
    void unbind_ptr() const noexcept
    {
        if (--m_ref_count == 0) {
            delete this;
        }
    }

private:
    mutable std::atomic<unsigned long> m_ref_count;

    template <class>
    friend class bind_ptr;
};


// Implementation:

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator==(const bind_ptr<U>& p) const noexcept
{
    return m_ptr == p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator==(U* p) const noexcept
{
    return m_ptr == p;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator!=(const bind_ptr<U>& p) const noexcept
{
    return m_ptr != p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator!=(U* p) const noexcept
{
    return m_ptr != p;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator<(const bind_ptr<U>& p) const noexcept
{
    return m_ptr < p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator<(U* p) const noexcept
{
    return m_ptr < p;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator>(const bind_ptr<U>& p) const noexcept
{
    return m_ptr > p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator>(U* p) const noexcept
{
    return m_ptr > p;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator<=(const bind_ptr<U>& p) const noexcept
{
    return m_ptr <= p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator<=(U* p) const noexcept
{
    return m_ptr <= p;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator>=(const bind_ptr<U>& p) const noexcept
{
    return m_ptr >= p.m_ptr;
}

template <class T>
template <class U>
constexpr bool bind_ptr<T>::operator>=(U* p) const noexcept
{
    return m_ptr >= p;
}

template <class T, class U>
constexpr bool operator==(T* a, const bind_ptr<U>& b) noexcept
{
    return b == a;
}

template <class T, class U>
constexpr bool operator!=(T* a, const bind_ptr<U>& b) noexcept
{
    return b != a;
}

template <class T, class U>
constexpr bool operator<(T* a, const bind_ptr<U>& b) noexcept
{
    return b > a;
}

template <class T, class U>
constexpr bool operator>(T* a, const bind_ptr<U>& b) noexcept
{
    return b < a;
}

template <class T, class U>
constexpr bool operator<=(T* a, const bind_ptr<U>& b) noexcept
{
    return b >= a;
}

template <class T, class U>
constexpr bool operator>=(T* a, const bind_ptr<U>& b) noexcept
{
    return b <= a;
}


} // namespace util
} // namespace realm

#endif // REALM_UTIL_BIND_PTR_HPP
