#ifndef __TDB_ALLOC__
#define __TDB_ALLOC__

#include <stdlib.h>

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

struct MemRef {
	MemRef() : pointer(NULL), ref(0) {}
	MemRef(void* p, size_t r) : pointer(p), ref(r) {}
	void* pointer;
	size_t ref;
};

class Allocator {
public:
	virtual MemRef Alloc(size_t size) {void* p = malloc(size); return MemRef(p,(size_t)p);}
	virtual MemRef ReAlloc(void* p, size_t size) {void* p2 = realloc(p, size); return MemRef(p2,(size_t)p2);}
	virtual void Free(size_t, void* p) {return free(p);}
	virtual void* Translate(size_t ref) const {return (void*)ref;}

#ifdef _DEBUG
	virtual void Verify() const {};
#endif //_DEBUG
};

static Allocator DefaultAllocator;

#endif //__TDB_ALLOC__
