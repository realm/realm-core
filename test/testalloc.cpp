#include <UnitTest++.h>
#include <tightdb/alloc_slab.hpp>

using namespace tightdb;

namespace {

void set_capacity(char* addr, size_t size)
{
    unsigned char* header = reinterpret_cast<unsigned char*>(addr);
    header[4] = (size >> 16) & 0x000000FF;
    header[5] = (size >> 8) & 0x000000FF;
    header[6] = size & 0x000000FF;
}

} // anonymous namespace


TEST(Alloc1)
{
    SlabAlloc alloc;

    MemRef mr1 = alloc.alloc(8);
    MemRef mr2 = alloc.alloc(16);
    MemRef mr3 = alloc.alloc(256);

    // Set size in headers (needed for Alloc::free())
    set_capacity(mr1.m_addr, 8);
    set_capacity(mr2.m_addr, 16);
    set_capacity(mr3.m_addr, 256);

    // Are pointers 64bit aligned
    CHECK_EQUAL(0, intptr_t(mr1.m_addr) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr2.m_addr) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr3.m_addr) & 0x7);

    // Do refs translate correctly
    CHECK_EQUAL(static_cast<void*>(mr1.m_addr), alloc.translate(mr1.m_ref));
    CHECK_EQUAL(static_cast<void*>(mr2.m_addr), alloc.translate(mr2.m_ref));
    CHECK_EQUAL(static_cast<void*>(mr3.m_addr), alloc.translate(mr3.m_ref));

    alloc.free_(mr3.m_ref, mr3.m_addr);
    alloc.free_(mr2.m_ref, mr2.m_addr);
    alloc.free_(mr1.m_ref, mr1.m_addr);

    // SlabAlloc destructor will verify that all is free'd
}
