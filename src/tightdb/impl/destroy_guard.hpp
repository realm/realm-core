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

#ifndef TIGHTDB_IMPL_DESTROY_GUARD_HPP
#define TIGHTDB_IMPL_DESTROY_GUARD_HPP

#include <tightdb/util/features.h>
#include <tightdb/array.hpp>

namespace tightdb {
namespace _impl {


template<class T> class DestroyGuard {
public:
    DestroyGuard() TIGHTDB_NOEXCEPT;

    DestroyGuard(T*) TIGHTDB_NOEXCEPT;

    ~DestroyGuard() TIGHTDB_NOEXCEPT;

    void reset(T*) TIGHTDB_NOEXCEPT;

    T* get() const TIGHTDB_NOEXCEPT;

    T* release() TIGHTDB_NOEXCEPT;

private:
    T* m_ptr;
};


class RefDestroyGuard {
public:
    RefDestroyGuard(Allocator&) TIGHTDB_NOEXCEPT;

    RefDestroyGuard(ref_type, Allocator&) TIGHTDB_NOEXCEPT;

    ~RefDestroyGuard() TIGHTDB_NOEXCEPT;

    void reset(ref_type) TIGHTDB_NOEXCEPT;

    ref_type get() const TIGHTDB_NOEXCEPT;

    ref_type release() TIGHTDB_NOEXCEPT;

private:
    ref_type m_ref;
    Allocator& m_alloc;
};





// Implementation:

template<class T> inline DestroyGuard<T>::DestroyGuard() TIGHTDB_NOEXCEPT:
    m_ptr(0)
{
}

template<class T> inline DestroyGuard<T>::DestroyGuard(T* ptr) TIGHTDB_NOEXCEPT:
    m_ptr(ptr)
{
}

template<class T> inline DestroyGuard<T>::~DestroyGuard() TIGHTDB_NOEXCEPT
{
    if (m_ptr)
        m_ptr->destroy();
}

template<class T> inline void DestroyGuard<T>::reset(T* ptr) TIGHTDB_NOEXCEPT
{
    if (m_ptr)
        m_ptr->destroy();
    m_ptr = ptr;
}

template<class T> inline T* DestroyGuard<T>::get() const TIGHTDB_NOEXCEPT
{
    return m_ptr;
}

template<class T> inline T* DestroyGuard<T>::release() TIGHTDB_NOEXCEPT
{
    T* ptr = m_ptr;
    m_ptr = 0;
    return ptr;
}


inline RefDestroyGuard::RefDestroyGuard(Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_ref(0),
    m_alloc(alloc)
{
}

inline RefDestroyGuard::RefDestroyGuard(ref_type ref, Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_ref(ref),
    m_alloc(alloc)
{
}

inline RefDestroyGuard::~RefDestroyGuard() TIGHTDB_NOEXCEPT
{
    if (m_ref)
        Array::destroy(m_ref, m_alloc);
}

inline void RefDestroyGuard::reset(ref_type ref) TIGHTDB_NOEXCEPT
{
    if (m_ref)
        Array::destroy(m_ref, m_alloc);
    m_ref = ref;
}

inline ref_type RefDestroyGuard::get() const TIGHTDB_NOEXCEPT
{
    return m_ref;
}

inline ref_type RefDestroyGuard::release() TIGHTDB_NOEXCEPT
{
    ref_type ref = m_ref;
    m_ref = 0;
    return ref;
}


} // namespace _impl
} // namespace tightdb

#endif // TIGHTDB_IMPL_DESTROY_GUARD_HPP
