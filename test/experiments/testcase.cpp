#include <cstring>
#include <limits>
#include <vector>
#include <map>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>

#include <tightdb.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>

#include "../util/thread_wrapper.hpp"
#include "unit_test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::_impl;

namespace {

class FooAlloc: public Allocator {
public:
    FooAlloc():
        m_offset(8)
    {
        m_baseline = 8;
    }

    ~FooAlloc() TIGHTDB_NOEXCEPT
    {
    }

    MemRef alloc(size_t size) TIGHTDB_OVERRIDE
    {
        ref_type ref = m_offset;
        char*& addr = m_map[ref]; // Throws
        TIGHTDB_ASSERT(!addr);
        addr = new char[size]; // Throws
        m_offset += size;
        return MemRef(addr, ref);
    }

    MemRef realloc_(ref_type ref, const char* addr, size_t old_size,
                    size_t new_size) TIGHTDB_OVERRIDE
    {
        static_cast<void>(old_size);
        free_(ref, addr);
        return alloc(new_size);
    }

    void free_(ref_type ref, const char* addr) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        typedef map<ref_type, char*>::iterator iter;
        iter i = m_map.find(ref);
        TIGHTDB_ASSERT(i != m_map.end());
        char* addr_2 = i->second;
        TIGHTDB_ASSERT(addr_2 == addr);
        m_map.erase(i);
        delete[] addr;
    }

    char* translate(ref_type ref) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        typedef map<ref_type, char*>::const_iterator iter;
        iter i = m_map.find(ref);
        TIGHTDB_ASSERT(i != m_map.end());
        char* addr = i->second;
        return addr;
    }

    bool empty()
    {
        return m_map.empty();
    }

    void clear()
    {
        typedef map<ref_type, char*>::const_iterator iter;
        iter end = m_map.end();
        for (iter i = m_map.begin(); i != end; ++i) {
            char* addr = i->second;
            delete[] addr;
        }
        m_map.clear();
    }

#ifdef TIGHTDB_DEBUG

    void Verify() const TIGHTDB_OVERRIDE
    {
    }

#endif

private:
    ref_type m_offset;
    map<ref_type, char*> m_map;
};

} // anonymous namespace



TEST(RefDestroyGuard)
{
    // Destroy
    {
        FooAlloc alloc;
        {
            ref_type ref = Array::create_empty_array(Array::type_Normal, alloc);
            RefDestroyGuard dg(ref, alloc);
            CHECK_EQUAL(ref, dg.get());
        }
        CHECK(alloc.empty());
    }
    // Release
    {
        FooAlloc alloc;
        {
            ref_type ref = Array::create_empty_array(Array::type_Normal, alloc);
            RefDestroyGuard dg(ref, alloc);
            CHECK_EQUAL(ref, dg.release());
        }
        CHECK(!alloc.empty());
        alloc.clear();
    }
    // Reset
    {
        FooAlloc alloc;
        {
            RefDestroyGuard dg(alloc);
            ref_type ref_1 = Array::create_empty_array(Array::type_Normal, alloc);
            dg.reset(ref_1);
            ref_type ref_2 = Array::create_empty_array(Array::type_Normal, alloc);
            dg.reset(ref_2);
        }
        CHECK(alloc.empty());
    }
}
