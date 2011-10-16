#ifndef __TDB_ALLOC__
#define __TDB_ALLOC__

#include <stdlib.h>

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

#define MEMREF_HEADER_SIZE    sizeof(MemRef::Header)
#define MEMREF_GET_HEADER(p) ((MemRef::Header*)((uint8_t*)(p)-MEMREF_HEADER_SIZE))

struct MemRef {
	MemRef() : pointer(NULL), ref(0) {}
	MemRef(void* p, size_t r) : pointer(p), ref(r) {}
	void* pointer;
	size_t ref;
    
    struct Header { // 64 bit
        uint32_t width    : 3;
        uint32_t unused1  : 3;
        uint32_t hasRefs  : 1;
        uint32_t isNode   : 1;
        uint32_t length   : 24;
        uint32_t capacity : 24;
        uint32_t unused2  : 8;
    };
};

class Allocator {
public:
      
	virtual MemRef Alloc(size_t size) {
        void* p = malloc(size); return MemRef(p,(size_t)p);
    }
    virtual MemRef ReAlloc(MemRef::Header* header, size_t size) {
        void* p = realloc(header, size); return MemRef(p,(size_t)p);
    }
	virtual void Free(size_t, MemRef::Header* header) {
        return free(header);
    }
	virtual void* Translate(size_t ref) const {
        return (void*)ref;
    }

#ifdef _DEBUG
	virtual void Verify() const {};
#endif //_DEBUG
};

static Allocator DefaultAllocator;

#endif //__TDB_ALLOC__
