#include "alloc.h"
#include <assert.h>

SlabAlloc::SlabAlloc() : m_shared(NULL), m_baseline(0) {}

SlabAlloc::~SlabAlloc() {
	// Release all allocated memory
	for (size_t i = 0; i < m_slabs.GetSize(); ++i) {
		void* p = (void*)(int)m_slabs[i].pointer;
		free(p);
	}
}

MemRef SlabAlloc::Alloc(size_t size) {
	// Do we have a free space we can reuse?
	for (size_t i = 0; i < m_freeSpace.GetSize(); ++i) {
		FreeSpace::Cursor r = m_freeSpace[i];
		if (r.size >= (int)size) {
			const size_t location = r.ref;
			const size_t rest = r.size - size;

			// Update free list
			if (rest == 0) m_freeSpace.DeleteRow(i);
			else {
				r.size = rest;
				r.ref += size;
			}

			return MemRef(Translate(location), location);
		}
	}

	// Else, allocate new slab
	const size_t multible = 256 * ((size % 256) + 1);
	const size_t slabsBack = m_slabs.IsEmpty() ? 0 : m_slabs.Back().offset;
	const size_t doubleLast = m_slabs.IsEmpty() ? 0 :
		                                          (slabsBack - (m_slabs.GetSize() == 1) ? 0 : m_slabs[-2].offset) * 2;
	const size_t newsize = multible > doubleLast ? multible : doubleLast;

	// Allocate memory 
	void* slab = malloc(newsize);
	if (!slab) return MemRef(NULL, 0);

	// Add to slab table
	Slabs::Cursor s = m_slabs.Add();
	s.offset = slabsBack + newsize;
	s.pointer = (int)slab;

	// Update free list
	const size_t rest = newsize - size;
	FreeSpace::Cursor f = m_freeSpace.Add();
	f.ref = slabsBack + newsize;
	f.size = rest;

	return MemRef((void*)slab, slabsBack);
}

// Support function
size_t GetSizeFromHeader(void* p) {
	// parse the capacity part of 8byte header
	const uint8_t* const header = (uint8_t*)p;
	return (header[4] << 16) + (header[5] << 8) + header[6];
}

void SlabAlloc::Free(size_t ref, void* p) {
	// Get size from segment
	const size_t size = GetSizeFromHeader(p);

	// Add to freelist
	m_freeSpace.Add(ref, size);

	// Consolidate freelist
}

MemRef SlabAlloc::ReAlloc(size_t ref, void* p, size_t size, bool doCopy=true) {
	//TODO: Check if we can extend current space

	// Allocate new space
	const MemRef space = Alloc(size);
	if (!space.pointer) return space;

	if (doCopy) {
		// Get size of old segment
		const size_t oldsize = GetSizeFromHeader(p);

		// Copy existing segment
		memcpy(space.pointer, p, oldsize);

		// Add old segment to freelist
		Free(ref, p);
	}

	return space;
}

void* SlabAlloc::Translate(size_t ref) const {
	if (ref < m_baseline) return m_shared + ref;
	else {
		const size_t ndx = m_slabs.offset.Find(ref); //FindPos
		assert(ndx != -1);

		const size_t offset = ndx ? m_slabs[ndx-1].offset : 0;
		return (int64_t*)(int)m_slabs[ndx].pointer + (ref - offset);
	}
}
