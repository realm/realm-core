#ifndef __TDB_ALLOC__
#define __TDB_ALLOC__

#include <stdlib.h>

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <cstdint> // unint8_t etc
#endif

struct MemRef {
	MemRef() : pointer(NULL), ref(0) {}
	MemRef(void* p, size_t r) : pointer(p), ref(r) {}
	void* pointer;
	size_t ref;
};

class Allocator {
public:
	MemRef Alloc(size_t size) {void* p = malloc(size); return MemRef(p,(size_t)p);}
	MemRef ReAlloc(void* p, size_t size) {void* p2 = realloc(p, size); return MemRef(p2,(size_t)p2);}
	void Free(void* p) {return free(p);}
	void* Translate(size_t ref) const {return (void*)ref;}
};

static Allocator DefaultAllocator;

#endif //__TDB_ALLOC__
