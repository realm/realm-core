#ifndef __TDB_ARRAY_STRING__
#define __TDB_ARRAY_STRING__

#include "Array.h"

class ArrayString : public Array {
public:
	ArrayString(Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
	ArrayString(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=GetDefaultAllocator());
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
	void FindAll(Array& result, const char* value, size_t add_offset = 0, size_t start = 0, size_t end = -1);

	template<class S> size_t Write(S& out) const;

#ifdef _DEBUG
	bool Compare(const ArrayString& c) const;
	void StringStats() const;
	void ToDot(FILE* f) const;
#endif //_DEBUG

private:
	size_t FindWithLen(const char* value, size_t len, size_t start , size_t end) const;
	virtual size_t CalcByteLen(size_t count, size_t width) const;
	virtual size_t CalcItemCount(size_t bytes, size_t width) const;
};

// Templates

template<class S>
size_t ArrayString::Write(S& out) const {
	// Calculate how many bytes the array takes up
	const size_t len = 8 + (m_len * m_width);

	// Write header first
	// TODO: replace capacity with checksum
	out.write((const char*)m_data-8, 8);

	// Write array
	const size_t arrayByteLen = len - 8;
	if (arrayByteLen) out.write((const char*)m_data, arrayByteLen);

	// Pad so next block will be 64bit aligned
	const char pad[8] = {0,0,0,0,0,0,0,0};
	const size_t rest = (~len & 0x7)+1;

	if (rest < 8) {
		out.write(pad, rest);
		return len + rest;
	}
	else return len; // Return number of bytes written
}

#endif //__TDB_ARRAY__