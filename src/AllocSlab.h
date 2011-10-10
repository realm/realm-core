#ifndef __TDB_ALLOC_SLAB__
#define __TDB_ALLOC_SLAB__

#include "tightdb.h"

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

class SlabAlloc : public Allocator {
public:
	SlabAlloc() : m_shared(NULL), m_baseline(10) {}
	~SlabAlloc();

	MemRef Alloc(size_t size);
	MemRef ReAlloc(size_t ref, MemRef::Header* header, size_t size, bool doCopy);
	void Free(size_t ref, MemRef::Header* header);
	void* Translate(size_t ref) const;

#ifdef _DEBUG
	void Verify() const;
	bool IsAllFree() const;
#endif //_DEBUG

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

#endif //__TDB_ALLOC_SLAB__
