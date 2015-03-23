#include <cerrno>
#include <cstdlib>
#include <stdexcept>
#include <algorithm>

#include <tightdb/alloc_slab.hpp>

using namespace std;
using namespace tightdb;


// FIXME: Casting a pointers to std::size_t is inherently
// nonportable. For example, systems exist where pointers are 64 bits
// and std::size_t is 32. One idea would be to use a different type
// for refs such as std::uintptr_t, the problem with this one is that
// while it is described by the C++11 standard it is not required to
// be present. C++03 does not even mention it. A real working solution
// will be to introduce a new name for the type of refs. The typedef
// can then be made as complex as required to pick out an appropriate
// type on any supported platform.
//
// A better solution may be to use an instance of SlabAlloc. The main
// problem is that SlabAlloc is not thread-safe. Another problem is
// that its free-list management is currently exceedingly slow do to
// linear searches. Another problem is that it is prone to general
// memory corruption due to lack of exception safety when upding
// free-lists. But these problems must be fixed anyway.


namespace {

/// For use with free-standing objects (objects that are not part of a
/// TightDB group)
///
/// Note that it is essential that this class is stateless as it may
/// be used by multiple threads. Although it has m_replication, this
/// is not a problem, as there is no way to modify it, so it will
/// remain zero.
class DefaultAllocator: public tightdb::Allocator {
public:
    DefaultAllocator()
    {
        m_baseline = 1; // Zero is not available
    }

    MemRef do_alloc(size_t size) REALM_OVERRIDE
    {
        char* addr = static_cast<char*>(::malloc(size));
        if (REALM_UNLIKELY(!addr)) {
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw bad_alloc();
        }
#ifdef REALM_ENABLE_ALLOC_SET_ZERO
        fill(addr, addr+size, 0);
#endif
        return MemRef(addr, reinterpret_cast<size_t>(addr));
    }

    MemRef do_realloc(ref_type, const char* addr, size_t old_size,
                      size_t new_size) REALM_OVERRIDE
    {
        char* new_addr = static_cast<char*>(::realloc(const_cast<char*>(addr), new_size));
        if (REALM_UNLIKELY(!new_addr)) {
            REALM_ASSERT_DEBUG(errno == ENOMEM);
            throw bad_alloc();
        }
#ifdef REALM_ENABLE_ALLOC_SET_ZERO
        fill(new_addr+old_size, new_addr+new_size, 0);
#else
        static_cast<void>(old_size);
#endif
        return MemRef(new_addr, reinterpret_cast<size_t>(new_addr));
    }

    void do_free(ref_type, const char* addr) REALM_NOEXCEPT REALM_OVERRIDE
    {
        ::free(const_cast<char*>(addr));
    }

    char* do_translate(ref_type ref) const REALM_NOEXCEPT REALM_OVERRIDE
    {
        return reinterpret_cast<char*>(ref);
    }

#ifdef REALM_DEBUG
    void Verify() const REALM_OVERRIDE {}
#endif
};

} // anonymous namespace



Allocator& Allocator::get_default() REALM_NOEXCEPT
{
    static DefaultAllocator default_alloc;
    return default_alloc;
}

MemRef Allocator::do_realloc(ref_type ref, const char* addr, size_t old_size,
                             size_t new_size)
{
    // Allocate new space
    MemRef new_mem = do_alloc(new_size); // Throws

    // Copy existing contents
    char* new_addr = new_mem.m_addr;
    copy(addr, addr+old_size, new_addr);

    // Free old chunk
    do_free(ref, addr);

    return new_mem;
}
