#include <cerrno>
#include <cstdlib>
#include <stdexcept>

#include <tightdb/alloc_slab.hpp>

using namespace std;
using namespace tightdb;


namespace {

// For use with free-standing objects (objects that are not part of a
// TightDB group)
class DefaultAllocator: public tightdb::Allocator {
public:
    MemRef alloc(size_t size) TIGHTDB_OVERRIDE
    {
        char* addr = static_cast<char*>(malloc(size));
        if (TIGHTDB_LIKELY(addr))
            return MemRef(addr, reinterpret_cast<size_t>(addr));
        TIGHTDB_ASSERT(errno == ENOMEM);
        throw bad_alloc();
    }

    MemRef realloc_(ref_type, const char* addr, size_t size) TIGHTDB_OVERRIDE
    {
        char* new_addr = static_cast<char*>(realloc(const_cast<char*>(addr), size));
        if (TIGHTDB_LIKELY(new_addr))
            return MemRef(new_addr, reinterpret_cast<size_t>(new_addr));
        TIGHTDB_ASSERT(errno == ENOMEM);
        throw bad_alloc();
    }

    void free_(ref_type, const char* addr) TIGHTDB_OVERRIDE
    {
        free(const_cast<char*>(addr));
    }

    char* translate(ref_type ref) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        return reinterpret_cast<char*>(ref);
    }

    bool is_read_only(ref_type) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        return false;
    }

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE {}
#endif
};

} // anonymous namespace



Allocator& Allocator::get_default() TIGHTDB_NOEXCEPT
{
    static DefaultAllocator alloc;
    return alloc;
}
