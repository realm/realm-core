#ifndef __TDB_ARRAY_STRING__
#define __TDB_ARRAY_STRING__

#include "Array.h"

class ArrayString : public Array {
public:
	ArrayString(Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ArrayString(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	ArrayString(Allocator& alloc);
	~ArrayString();

	const char* Get(size_t ndx) const;
	bool Add();
	bool Add(const char* value);
	bool Set(size_t ndx, const char* value);
	bool Set(size_t ndx, const char* value, size_t len);
	bool Insert(size_t ndx, const char* value);
	bool Insert(size_t ndx, const char* value, size_t len);
	void Delete(size_t ndx);

	size_t Find(const char* value, size_t start=0 , size_t end=-1) const;

	size_t Write(std::ostream& out) const;

#ifdef _DEBUG
	bool Compare(const ArrayString& c) const;
	void Stats() const;
	void ToDot(FILE* f) const;
#endif //_DEBUG

private:
	virtual size_t CalcByteLen(size_t count, size_t width) const;
};

#endif //__TDB_ARRAY__