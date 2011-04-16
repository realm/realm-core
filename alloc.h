#ifndef __TDB_ALLOC__
#define __TDB_ALLOC__

#include <stdlib.h>
#include "tightdb.h"
#include "stdint.h"

struct MemRef {
	void* pointer;
	size_t ref;
	MemRef(void* p, size_t r) : pointer(p), ref(r) {}
};

class Allocator {
public:
	MemRef Alloc(size_t size) {void* p = malloc(size); return MemRef(p,(size_t)p);}
	MemRef ReAlloc(void* p, size_t size) {void* p2 = realloc(p, size); return MemRef(p2,(size_t)p2);}
	void Free(void* p) {return free(p);}
	void* Translate(size_t ref) const {return (void*)ref;}
};

class SlabAlloc : public Allocator {
public:
	SlabAlloc();
	~SlabAlloc();

	MemRef Alloc(size_t size);
	MemRef ReAlloc(size_t ref, void* p, size_t size, bool doCopy);
	void Free(size_t ref, void* p);
	void* Translate(size_t ref) const;

private:
	// Define internal tables
	TDB_TABLE_2(Slabs,
				Int, offset,
				Int, pointer)
	TDB_TABLE_2(FreeSpace,
				Int, ref,
				Int, size)

	// Member variables
	int64_t* m_shared;
	size_t m_baseline;
	Slabs m_slabs;
	FreeSpace m_freeSpace;
};

#endif //__TDB_ALLOC__
