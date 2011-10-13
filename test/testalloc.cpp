#include <UnitTest++.h>
#include "AllocSlab.h"

// Pre-declare local functions
void SetCapacity(void* p, size_t size);


void SetCapacity(void* p, size_t size) {
	uint8_t* header = (uint8_t*)p;
	header[4] = (size >> 16) & 0x000000FF;
	header[5] = (size >> 8) & 0x000000FF;
	header[6] = size & 0x000000FF;
}

TEST(Alloc1) {
	SlabAlloc alloc;

	const MemRef mr1 = alloc.Alloc(8);
	const MemRef mr2 = alloc.Alloc(16);
	const MemRef mr3 = alloc.Alloc(256);

	// Set size in headers (needed for Alloc::Free)
	SetCapacity(mr1.pointer, 8);
	SetCapacity(mr2.pointer, 16);
	SetCapacity(mr3.pointer, 256);

	// Are pointers 64bit aligned
	CHECK_EQUAL(0, (intptr_t)mr1.pointer & 0x7);
	CHECK_EQUAL(0, (intptr_t)mr2.pointer & 0x7);
	CHECK_EQUAL(0, (intptr_t)mr3.pointer & 0x7);

	// Do refs translate correctly
	CHECK_EQUAL(mr1.pointer, alloc.Translate(mr1.ref));
	CHECK_EQUAL(mr2.pointer, alloc.Translate(mr2.ref));
	CHECK_EQUAL(mr3.pointer, alloc.Translate(mr3.ref));

	alloc.Free(mr3.ref, mr3.pointer);
	alloc.Free(mr2.ref, mr2.pointer);
	alloc.Free(mr1.ref, mr1.pointer);

	// SlabAlloc destructor will verify that all is free'd
}
