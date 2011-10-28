#ifndef __TDB_ARRAY_BLOB__
#define __TDB_ARRAY_BLOB__

#include "Array.h"

class ArrayBlob : public Array {
public:
	ArrayBlob(Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ArrayBlob(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	ArrayBlob(Allocator& alloc);
	~ArrayBlob();

	const uint8_t* Get(size_t pos) const;

	void Add(void* data, size_t len);
	void Insert(size_t pos, void* data, size_t len);
	void Replace(size_t start, size_t end, void* data, size_t len);
	void Delete(size_t start, size_t end);
	void Clear();

private:
	virtual size_t CalcByteLen(size_t count, size_t width) const;
};

#endif //__TDB_ARRAY_BLOB__