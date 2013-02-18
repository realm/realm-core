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
#ifndef TIGHTDB_ALLOC_HPP
#define TIGHTDB_ALLOC_HPP

#include <cstddef>

namespace tightdb {

#ifdef TIGHTDB_ENABLE_REPLICATION
struct Replication;
#endif

struct MemRef {
    MemRef(): pointer(0), ref(0) {}
    MemRef(void* p, std::size_t r): pointer(p), ref(r) {}
    void* pointer;
    std::size_t ref;
};

// FIXME: Casting a pointer to std::size_t is inherently nonportable
// (see the default definition of Allocator::Alloc()). For example,
// systems exist where pointers are 64 bits and std::size_t is 32. One
// idea would be to use a different type for refs such as
// std::uintptr_t, the problem with this one is that while it is
// described by the C++11 standard it is not required to be
// present. C++03 does not even mention it. A real working solution
// will be to introduce a new name for the type of refs. The typedef
// can then be made as complex as required to pick out an appropriate
// type on any supported platform.

class Allocator {
public:
    /// \throw std::bad_alloc If insufficient memory was available.
    virtual MemRef Alloc(std::size_t size);

    /// \throw std::bad_alloc If insufficient memory was available.
    virtual MemRef ReAlloc(std::size_t ref, void* addr, std::size_t size);

    // FIXME: SlabAlloc::Free() should be modified such than this method never throws.
    virtual void Free(std::size_t, void* addr);

    virtual void* Translate(std::size_t ref) const TIGHTDB_NOEXCEPT;
    virtual bool IsReadOnly(std::size_t) const TIGHTDB_NOEXCEPT;

    static Allocator& get_default() TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_ENABLE_REPLICATION
    Allocator() TIGHTDB_NOEXCEPT: m_replication(0) {}
#endif
    virtual ~Allocator() {}

#ifdef TIGHTDB_ENABLE_REPLICATION
    Replication* get_replication() TIGHTDB_NOEXCEPT { return m_replication; }
#endif

#ifdef TIGHTDB_DEBUG
    virtual void Verify() const {}
#endif // TIGHTDB_DEBUG

#ifdef TIGHTDB_ENABLE_REPLICATION
protected:
    Replication* m_replication;
#endif
};


} // namespace tightdb

#endif // TIGHTDB_ALLOC_HPP
