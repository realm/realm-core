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

#include <stdint.h>
#include <cstddef>

#include <tightdb/config.h>
#include <tightdb/assert.hpp>
#include <tightdb/safe_int_ops.hpp>

namespace tightdb {

#ifdef TIGHTDB_ENABLE_REPLICATION
class Replication;
#endif

typedef std::size_t ref_type;

inline ref_type to_ref(int64_t v) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(!int_cast_has_overflow<ref_type>(v));
    // Check that v is divisible by 8 (64-bit aligned).
    TIGHTDB_ASSERT(v % 8 == 0);
    return ref_type(v);
}

struct MemRef {
    MemRef(): m_addr(0), m_ref(0) {}
    MemRef(char* addr, ref_type ref): m_addr(addr), m_ref(ref) {}
    char* m_addr;
    ref_type m_ref;
};


/// The common interface for TightDB allocators.
///
/// A TightDB allocator must associate a 'ref' to each allocated
/// object and be able to efficiently map any 'ref' to the
/// corresponding memory address. The 'ref' is an integer and it must
/// always be divisible by 8. Also, a value of zero is used to
/// indicate a null-reference, and must therefore never be returned by
/// Allocator::alloc().
///
/// The purpose of the 'refs' is to decouple the memory reference from
/// the actual address and thereby allowing objects to be relocated in
/// memory without having to modify stored references.
///
/// \sa SlabAlloc
class Allocator {
public:
    /// The specified size must not be zero.
    ///
    /// \throw std::bad_alloc If insufficient memory was available.
    virtual MemRef alloc(std::size_t size) = 0;

    /// The specified size must not be zero.
    ///
    /// \throw std::bad_alloc If insufficient memory was available.
    ///
    /// Note: The underscore has been added because the name `realloc`
    /// would conflict with a macro on the Windows platform.
    virtual MemRef realloc_(ref_type ref, const char* addr, std::size_t old_size,
                            std::size_t new_size) = 0;

    /// Release the specified chunk of memory.
    ///
    /// Note: The underscore has been added because the name `free
    /// would conflict with a macro on the Windows platform.
    virtual void free_(ref_type, const char* addr) TIGHTDB_NOEXCEPT = 0;

    /// Map the specified \a ref to the corresponding memory
    /// address. Note that if is_read_only(ref) returns true, then the
    /// referenced object is to be considered immutable, and it is
    /// then entirely the responsibility of the caller that the memory
    /// is not modified by way of the returned memory pointer.
    virtual char* translate(ref_type ref) const TIGHTDB_NOEXCEPT = 0;

    /// Returns true if, and only if the object at the specified 'ref'
    /// is in the immutable part of the memory managed by this
    /// allocator. The method by which some objects become part of the
    /// immuatble part is entirely up to the class that implements
    /// this interface.
    virtual bool is_read_only(ref_type) const TIGHTDB_NOEXCEPT = 0;

    /// Returns a simple allocator that can be used with free-standing
    /// TightDB objects (such as a free-standing table). A
    /// free-standing object is one that is not part of a Group, and
    /// therefore, is not part of an actual database.
    static Allocator& get_default() TIGHTDB_NOEXCEPT;

    virtual ~Allocator() TIGHTDB_NOEXCEPT {}

#ifdef TIGHTDB_DEBUG
    virtual void Verify() const = 0;
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
    Allocator() TIGHTDB_NOEXCEPT: m_replication(0) {}
    Replication* get_replication() TIGHTDB_NOEXCEPT { return m_replication; }

protected:
    Replication* m_replication;
#endif
};


} // namespace tightdb

#endif // TIGHTDB_ALLOC_HPP
