#ifndef __TDB_ARRAY_STRING_LONG__
#define __TDB_ARRAY_STRING_LONG__

#include "ArrayBlob.h"

class ArrayStringLong : public Array {
public:
	ArrayStringLong(Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
	ArrayStringLong(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=GetDefaultAllocator());
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

	size_t Find(const char* value, size_t start=0 , size_t end=-1) const;
	void FindAll(Array &result, const char* value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

	template<class S> size_t Write(S& out, size_t& pos) const;

private:
	size_t FindWithLen(const char* value, size_t len, size_t start , size_t end) const;

	// Member variables
	Array m_offsets;
	ArrayBlob m_blob;
};

// Templates

template<class S>
size_t ArrayStringLong::Write(S& out, size_t& pos) const{
	// Write out offsets
	const size_t offsets_pos = pos;
	pos += m_offsets.Write(out);

	// Write out data
	const size_t blob_pos = pos;
	pos += m_blob.Write(out);

	// Write new array with node info
	const size_t node_pos = pos;
	Array node(COLUMN_HASREFS);
	node.Add(offsets_pos);
	node.Add(blob_pos);
	pos += node.Write(out);

	// Clean-up
	node.SetType(COLUMN_NORMAL); // avoid recursive del
	node.Destroy();

	return node_pos;
}

#endif //__TDB_ARRAY_STRING_LONG__
