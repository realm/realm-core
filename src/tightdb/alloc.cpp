#include <cerrno>
#include <cstdlib>
#include <stdexcept>

#include <tightdb/alloc_slab.hpp>

using namespace std;

namespace tightdb {

MemRef Allocator::alloc(size_t size)
{
    char* addr = static_cast<char*>(malloc(size));
    if (TIGHTDB_LIKELY(addr)) return MemRef(addr, reinterpret_cast<size_t>(addr));
    TIGHTDB_ASSERT(errno == ENOMEM);
    throw bad_alloc();
}

MemRef Allocator::realloc_(ref_type, const char* addr, size_t size)
{
    char* new_addr = static_cast<char*>(realloc(const_cast<char*>(addr), size));
    if (TIGHTDB_LIKELY(new_addr)) return MemRef(new_addr, reinterpret_cast<size_t>(new_addr));
    TIGHTDB_ASSERT(errno == ENOMEM);
    throw bad_alloc();
}

void Allocator::free_(ref_type, const char* addr)
{
    free(const_cast<char*>(addr));
}

char* Allocator::translate(ref_type ref) const TIGHTDB_NOEXCEPT
{
    return reinterpret_cast<char*>(ref);
}

bool Allocator::is_read_only(ref_type) const TIGHTDB_NOEXCEPT
{
    return false;
}

Allocator& Allocator::get_default() TIGHTDB_NOEXCEPT
{
    static Allocator default_alloc;
    return default_alloc;
}

} //namespace tightdb
