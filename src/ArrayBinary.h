#ifndef __TDB_ARRAY_BINARY__
#define __TDB_ARRAY_BINARY__

#include "ArrayBlob.h"

class ArrayBinary : public Array {
public:
	ArrayBinary(Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ArrayBinary(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	//ArrayBinary(Allocator& alloc);
	~ArrayBinary();

	bool IsEmpty() const;
	size_t Size() const;

	const void* Get(size_t ndx) const;
	size_t GetLen(size_t ndx) const;

	void Add(const void* value, size_t len);
	void Set(size_t ndx, const void* value, size_t len);
	void Insert(size_t ndx, const void* value, size_t len);
	void Delete(size_t ndx);
	void Clear();

	size_t Write(std::ostream& out, size_t& pos) const;

private:
	Array m_offsets;
	ArrayBlob m_blob;
};

#endif //__TDB_ARRAY_BINARY__
