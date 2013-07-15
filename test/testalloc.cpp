#include <UnitTest++.h>
#include <tightdb/alloc_slab.hpp>

using namespace tightdb;

namespace {

void set_capacity(void* p, size_t size)
{
    uint8_t* header = static_cast<uint8_t*>(p);
    header[4] = (size >> 16) & 0x000000FF;
    header[5] = (size >> 8) & 0x000000FF;
    header[6] = size & 0x000000FF;
}

} // anonymous namespace


TEST(Alloc1)
{
    SlabAlloc alloc;

    MemRef mr1 = alloc.Alloc(8);
    MemRef mr2 = alloc.Alloc(16);
    MemRef mr3 = alloc.Alloc(256);

    // Set size in headers (needed for Alloc::Free)
    set_capacity(mr1.pointer, 8);
    set_capacity(mr2.pointer, 16);
    set_capacity(mr3.pointer, 256);

    // Are pointers 64bit aligned
    CHECK_EQUAL(0, intptr_t(mr1.pointer) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr2.pointer) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr3.pointer) & 0x7);

    // Do refs translate correctly
    CHECK_EQUAL(mr1.pointer, alloc.translate(mr1.ref));
    CHECK_EQUAL(mr2.pointer, alloc.translate(mr2.ref));
    CHECK_EQUAL(mr3.pointer, alloc.translate(mr3.ref));

    alloc.Free(mr3.ref, mr3.pointer);
    alloc.Free(mr2.ref, mr2.pointer);
    alloc.Free(mr1.ref, mr1.pointer);

    // SlabAlloc destructor will verify that all is free'd
}
