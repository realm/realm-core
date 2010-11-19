#include "alloc.h"

//TODO: what about returning pos zero vs error reporting?
void* SlabAlloc::Alloc(size_t size) {
	// Do we have a free space we can reuse?
	if (!m_freeSpace.IsEmpty()) {
		FreeSpace::Cursor r = m_freeSpace.Back();
		if (r.size >= (int)size) {
			void* location = (void*)(int)r.pos;
			const size_t rest = r.size - size;

			// Update free list
			// TODO: sorted list
			if (rest == 0) m_freeSpace.PopBack();
			else {
				r.size = (int)rest;
				r.pos += (int)size;
			}

			return location;
		}
	}

	// Else, allocate new slab
	const size_t multible = 256 * ((size % 256) + 1);
	const size_t slabsBack = m_slabs.IsEmpty() ? 0 : m_slabs.Back().end;
	const size_t doubleLast = m_slabs.IsEmpty() ? 0 :
		                                          (slabsBack - (m_slabs.GetSize() == 1) ? 0 : m_slabs[-2].end) * 2;
	const size_t newsize = multible > doubleLast ? multible : doubleLast;

	// Alloc and add to slab table
	void* slab = malloc(newsize);
	if (!slab) return NULL;
	Slabs::Cursor s = m_slabs.Add();
	s.end = (int)(slabsBack + newsize) + (slabsBack ? 0 : 1); // slabs count from 1
	s.pointer = (int)slab;

	// Update free list
	const size_t rest = newsize - size;
	FreeSpace::Cursor f = m_freeSpace.Add();
	f.size = (int)rest;
	f.pos = (int)(slabsBack + newsize);

	return (void*)slabsBack;
}

void SlabAlloc::Free(void* p) {
	// Get size from segment

	// Add to freelist

	// Consolidate freelist
}

void* SlabAlloc::ReAlloc(void* p, size_t size) {
	// Allocate new space
	void* space = Alloc(size);
	if (!space) return NULL;

	// Copy existing segment
	//memcpy(space, p, oldsize);

	// Add old segment to freelist
	Free(p);

	return space;
}