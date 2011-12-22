#ifndef __TDB_ARRAY_BLOB__
#define __TDB_ARRAY_BLOB__

#include "Array.h"

class ArrayBlob : public Array {
public:
	ArrayBlob(Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
	ArrayBlob(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=GetDefaultAllocator());
	ArrayBlob(Allocator& alloc);
	~ArrayBlob();

	const uint8_t* Get(size_t pos) const;

	void Add(void* data, size_t len);
	void Insert(size_t pos, void* data, size_t len);
	void Replace(size_t start, size_t end, void* data, size_t len);
	void Delete(size_t start, size_t end);
	void Clear();

	template<class S> size_t Write(S& out) const;

private:
	virtual size_t CalcByteLen(size_t count, size_t width) const;
};

// Templates

template<class S>
size_t ArrayBlob::Write(S& out) const {
	// Calculate how many bytes the array takes up
	const size_t len = 8 + m_len;

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

#endif //__TDB_ARRAY_BLOB__