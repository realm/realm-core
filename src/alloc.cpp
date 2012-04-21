
// Memory Mapping includes
#ifdef _MSC_VER
#include <windows.h>
#include <stdio.h>
#include <conio.h>
#include <stdio.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#endif

#include <cassert>
#include <iostream>
#include "AllocSlab.hpp"
#include "Array.hpp"

#ifdef _DEBUG
#include <cstdio>
#endif //_DEBUG

namespace {

using namespace tightdb;

// Support function
// todo, fixme: use header function in array instead!
size_t GetSizeFromHeader(void* p) {
	// parse the capacity part of 8byte header
	const uint8_t* const header = (uint8_t*)p;
	return (header[4] << 16) + (header[5] << 8) + header[6];
}

}


namespace tightdb {

Allocator& GetDefaultAllocator() {
	static Allocator DefaultAllocator;
	return DefaultAllocator;
}

SlabAlloc::SlabAlloc() : m_shared(NULL), m_owned(false), m_baseline(8) {
#ifdef _DEBUG
	m_debugOut = false;
#endif //_DEBUG
}

SlabAlloc::~SlabAlloc() {
#ifdef _DEBUG
	if (!IsAllFree()) {
		m_slabs.Print();
		m_freeSpace.Print();
		assert(false);
	}
#endif //_DEBUG

	// Release all allocated memory
	for (size_t i = 0; i < m_slabs.GetSize(); ++i) {
		void* p = (void*)(intptr_t)m_slabs[i].pointer;
		free(p);
	}

	// Release any shared memory
	if (m_shared) {
		if (m_owned) {
			free(m_shared);
		}
		else {
#ifdef _MSC_VER
			UnmapViewOfFile(m_shared);
			CloseHandle(m_fd);
			CloseHandle(m_mapfile);
#else
			munmap(m_shared, m_baseline);
			close(m_fd);
#endif
		}
	}
}

MemRef SlabAlloc::Alloc(size_t size) {
	assert((size & 0x7) == 0); // only allow sizes that are multibles of 8

	// Do we have a free space we can reuse?
	for (size_t i = 0; i < m_freeSpace.GetSize(); ++i) {
		FreeSpace::Cursor r = m_freeSpace[i];
		if (r.size >= (int)size) {
			const size_t location = (size_t)r.ref;
			const size_t rest = (size_t)r.size - size;

			// Update free list
			if (rest == 0) m_freeSpace.DeleteRow(i);
			else {
				r.size = rest;
				r.ref += (unsigned int)size;
			}

#ifdef _DEBUG
			if (m_debugOut) {
				printf("Alloc ref: %lu size: %lu\n", location, size);
			}
#endif //_DEBUG

			void* pointer = Translate(location);
			return MemRef(pointer, location);
		}
	}

	// Else, allocate new slab
	const size_t multible = 256 * ((size / 256) + 1);
	const size_t slabsBack = m_slabs.IsEmpty() ? m_baseline : m_slabs.Back().offset;
	const size_t doubleLast = m_slabs.IsEmpty() ? 0 :
		                                          (slabsBack - ((m_slabs.GetSize() == 1) ? (size_t)0 : m_slabs[-2].offset)) * 2;
	const size_t newsize = multible > doubleLast ? multible : doubleLast;

	// Allocate memory 
	void* slab = malloc(newsize);
	if (!slab) return MemRef(NULL, 0);

	// Add to slab table
	Slabs::Cursor s = m_slabs.Add();
	s.offset = slabsBack + newsize;
	s.pointer = (intptr_t)slab;

	// Update free list
	const size_t rest = newsize - size;
	FreeSpace::Cursor f = m_freeSpace.Add();
	f.ref = slabsBack + size;
	f.size = rest;

#ifdef _DEBUG
	if (m_debugOut) {
		printf("Alloc ref: %lu size: %lu\n", slabsBack, size);
	}
#endif //_DEBUG

	return MemRef(slab, slabsBack);
}

void SlabAlloc::Free(size_t ref, void* p) {
	if (IsReadOnly(ref)) return;

	// Get size from segment
	const size_t size = GetSizeFromHeader(p);
	const size_t refEnd = ref + size;
	bool isMerged = false;

#ifdef _DEBUG
	if (m_debugOut) {
		printf("Free ref: %lu size: %lu\n", ref, size);
	}
#endif //_DEBUG

	// Check if we can merge with start of free block
	size_t n = m_freeSpace.ref.Find(refEnd);
	if (n != (size_t)-1) {
		// No consolidation over slab borders
		if (m_slabs.offset.Find(refEnd) == (size_t)-1) {
			m_freeSpace[n].ref = ref;
			m_freeSpace[n].size += size;
			isMerged = true;
		}
	}

	// Check if we can merge with end of free block
	if (m_slabs.offset.Find(ref) == (size_t)-1) { // avoid slab borders
		const size_t count = m_freeSpace.GetSize();
		for (size_t i = 0; i < count; ++i) {
			FreeSpace::Cursor c = m_freeSpace[i];

		//	printf("%d %d", c.ref, c.size);

			const size_t end = TO_REF(c.ref + c.size);
			if (ref == end) {
				if (isMerged) {
					c.size += m_freeSpace[n].size;
					m_freeSpace.DeleteRow(n);
				}
				else c.size += size;

				return;
			}
		}
	}

	// Else just add to freelist
	if (!isMerged) m_freeSpace.Add(ref, size);
}

MemRef SlabAlloc::ReAlloc(size_t ref, void* p, size_t size) {
	assert((size & 0x7) == 0); // only allow sizes that are multibles of 8

	//TODO: Check if we can extend current space

	// Allocate new space
	const MemRef space = Alloc(size);
	if (!space.pointer) return space;

	/*if (doCopy) {*/  //TODO: allow realloc without copying
		// Get size of old segment
		const size_t oldsize = GetSizeFromHeader(p);

		// Copy existing segment
		memcpy(space.pointer, p, oldsize);

		// Add old segment to freelist
		Free(ref, p);
	//}

#ifdef _DEBUG
	if (m_debugOut) {
		printf("ReAlloc origref: %lu oldsize: %lu newref: %lu newsize: %lu\n", ref, oldsize, space.ref, size);
	}
#endif //_DEBUG

	return space;
}

void* SlabAlloc::Translate(size_t ref) const {
	if (ref < m_baseline) return m_shared + ref;
	else {
		const size_t ndx = m_slabs.offset.FindPos(ref);
		assert(ndx != (size_t)-1);

		const size_t offset = ndx ? m_slabs[ndx-1].offset : m_baseline;
		return (char*)(intptr_t)m_slabs[ndx].pointer + (ref - offset);
	}
}

bool SlabAlloc::IsReadOnly(size_t ref) const {
	return ref < m_baseline;
}

bool SlabAlloc::SetSharedBuffer(const char* buffer, size_t len) {
	// Verify that the topref points to a location within buffer.
	// This is currently the only integrity check we make
	size_t ref = (size_t)(*(uint64_t*)buffer);
	if (ref > len) return false;
	
	// There is a unit test that calls this function with an invalid buffer
	// so we can't size_t-test range with TO_REF until now
	ref = TO_REF(*(uint64_t*)buffer);

	m_shared = (char*)buffer;
	m_baseline = len;
	m_owned = true; // we now own the buffer
	return true;
}

bool SlabAlloc::SetShared(const char* path) {
#ifdef _MSC_VER
	// Open file
	m_fd = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_ALWAYS, NULL, NULL);

	// Map to memory (read only)
	const HANDLE hMapFile = CreateFileMapping(m_fd, NULL, PAGE_WRITECOPY, 0, 0, 0);
	if (hMapFile == NULL || hMapFile == INVALID_HANDLE_VALUE) {
		CloseHandle(m_fd);
		return false;
	}
	const LPCTSTR pBuf = (LPTSTR) MapViewOfFile(hMapFile, FILE_MAP_COPY, 0, 0, 0);
	if (pBuf == NULL) {
		return false;
	}

	// Get Size
	LARGE_INTEGER size;
	GetFileSizeEx(m_fd, &size);
	m_baseline = TO_REF(size.QuadPart);

	m_shared = (char *)pBuf;
	m_mapfile = hMapFile;
#else
	// Open file
	m_fd = open(path, O_RDONLY);
	if (m_fd < 0) return false;

	// Get size
	struct stat statbuf;
	if (fstat(m_fd, &statbuf) < 0) {
		close(m_fd);
		return false;
	}

	// Verify that data is 64bit aligned
	if ((statbuf.st_size & 0x7) != 0) return false;

	// Map to memory (read only)
	void* p = mmap(0, statbuf.st_size, PROT_READ, MAP_SHARED, m_fd, 0);
	if (p == (void*)-1) {
		close(m_fd);
		return false;
	}

	//TODO: Verify the data structures

	m_shared = (char*)p;
	m_baseline = statbuf.st_size;
#endif

	return true;
}

size_t SlabAlloc::GetTopRef() const {
	assert(m_shared && m_baseline > 0);

	const size_t ref = TO_REF(*(uint64_t*)m_shared);
	assert(ref < m_baseline);

	return ref;
}

size_t SlabAlloc::GetTotalSize() const {
	if (m_slabs.IsEmpty()) {
		return m_baseline;
	}
	else {
		return TO_REF(m_slabs.Back().offset);
	}
}

#ifdef _DEBUG

bool SlabAlloc::IsAllFree() const {
	if (m_freeSpace.GetSize() != m_slabs.GetSize()) return false;

	// Verify that free space matches slabs
	size_t ref = m_baseline;
	for (size_t i = 0; i < m_slabs.GetSize(); ++i) {
		const Slabs::Cursor c = m_slabs[i];
		const size_t size = TO_REF(c.offset) - ref;

		const size_t r = m_freeSpace.ref.Find(ref);
		if (r == (size_t)-1) return false;
		if (size != (size_t)m_freeSpace[r].size) return false;

		ref = TO_REF(c.offset);
	}
	return true;
}

void SlabAlloc::Verify() const {
	// Make sure that all free blocks fit within a slab
	for (size_t i = 0; i < m_freeSpace.GetSize(); ++i) {
		const FreeSpace::Cursor c = m_freeSpace[i];
		const size_t ref = TO_REF(c.ref);

		const size_t ndx = m_slabs.offset.FindPos(ref);
		assert(ndx != size_t(-1));

		const size_t slab_end = TO_REF(m_slabs[ndx].offset);
		const size_t free_end = ref + TO_REF(c.size);

		assert(free_end <= slab_end);
	}
}

void SlabAlloc::Print() const {
	const size_t allocated = m_slabs.IsEmpty() ? 0 : m_slabs[m_slabs.GetSize()-1].offset;

	size_t free = 0;
	for (size_t i = 0; i < m_freeSpace.GetSize(); ++i) {
		free += TO_REF(m_freeSpace[i].size);
	}

	cout << "Base: " << (m_shared ? m_baseline : 0) << " Allocated: " << (allocated - free) << "\n";
}

#endif //_DEBUG

}
