#ifndef __TDB_ARRAY_STRING__
#define __TDB_ARRAY_STRING__

#include "Array.h"

class ArrayString : public Array {
public:
	ArrayString(Allocator& alloc=DefaultAllocator);
	~ArrayString();
	
	const char* Get(size_t ndx) const;
	bool Add();
	bool Add(const char* value);
	bool Set(size_t ndx, const char* value);
	bool Set(size_t ndx, const char* value, size_t len);
	bool Insert(size_t ndx, const char* value, size_t len);
	void Delete(size_t ndx);

	size_t Find(const char* value) const;
	size_t Find(const char* value, size_t len) const;

#ifdef _DEBUG
	void Stats() const;
	void ToDot(FILE* f) const;
#endif //_DEBUG

private:
	bool Alloc(size_t count, size_t width);
};

#endif //__TDB_ARRAY__