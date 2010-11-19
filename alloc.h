#ifndef __TDB_ALLOC__
#define __TDB_ALLOC__

#include <stdlib.h>
#include "tightdb.h"

class Allocator {
public:
	void* Alloc(size_t size) {return malloc(size);}
	void* ReAlloc(void* p, size_t size) {return realloc(p, size);}
	void Free(void* p) {return free(p);}
	void* Translate(void* p) {return p;}
};

class SlabAlloc : public Allocator {
public:
	void* Alloc(size_t size);
	void* ReAlloc(void* p, size_t size);
	void Free(void* p);

private:
	// Define internal tables
	TDB_TABLE_2(Slabs,
				Int, end,
				Int, pointer)
	TDB_TABLE_2(FreeSpace,
				Int, size,
				Int, pos)

	// Member variables
	Slabs m_slabs;
	FreeSpace m_freeSpace;
};

#endif //__TDB_ALLOC__
