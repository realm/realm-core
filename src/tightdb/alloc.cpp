#include <cerrno>
#include <cstdlib>
#include <stdexcept>

#include <tightdb/alloc_slab.hpp>

using namespace std;

namespace tightdb {

MemRef Allocator::Alloc(size_t size)
{
    void* addr = malloc(size);
    if (TIGHTDB_LIKELY(addr)) return MemRef(addr, reinterpret_cast<size_t>(addr));
    TIGHTDB_ASSERT(errno == ENOMEM);
    throw bad_alloc();
}

MemRef Allocator::ReAlloc(size_t, const void* addr, size_t size)
{
    void* new_addr = realloc(const_cast<void*>(addr), size);
    if (TIGHTDB_LIKELY(new_addr)) return MemRef(new_addr, reinterpret_cast<size_t>(new_addr));
    TIGHTDB_ASSERT(errno == ENOMEM);
    throw bad_alloc();
}

void Allocator::Free(size_t, const void* addr)
{
    free(const_cast<void*>(addr));
}

void* Allocator::Translate(size_t ref) const TIGHTDB_NOEXCEPT
{
    return reinterpret_cast<void*>(ref);
}

bool Allocator::IsReadOnly(size_t) const TIGHTDB_NOEXCEPT
{
    return false;
}

Allocator& Allocator::get_default() TIGHTDB_NOEXCEPT
{
    static Allocator default_alloc;
    return default_alloc;
}

} //namespace tightdb
