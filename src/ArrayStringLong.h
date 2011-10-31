#ifndef __TDB_ARRAY_STRING_LONG__
#define __TDB_ARRAY_STRING_LONG__

#include "ArrayBlob.h"

class ArrayStringLong : public Array {
public:
	ArrayStringLong(Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ArrayStringLong(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	//ArrayStringLong(Allocator& alloc);
	~ArrayStringLong();

	bool IsEmpty() const;
	size_t Size() const;

	const char* Get(size_t ndx) const;
	void Add(const char* value);
	void Add(const char* value, size_t len);
	void Set(size_t ndx, const char* value);
	void Set(size_t ndx, const char* value, size_t len);
	void Insert(size_t ndx, const char* value);
	void Insert(size_t ndx, const char* value, size_t len);
	void Delete(size_t ndx);
	void Clear();

private:
	Array m_offsets;
	ArrayBlob m_blob;
};

#endif //__TDB_ARRAY_STRING_LONG__
