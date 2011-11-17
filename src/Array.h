#ifndef __TDB_ARRAY__
#define __TDB_ARRAY__

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif
//#include <climits> // size_t
#include <cstdlib> // size_t
#include <cstring> // memmove
#include "alloc.h"
#include <iostream>

#ifdef _DEBUG
#include <stdio.h>
#endif

// Pre-definitions
class Column;

#ifdef _DEBUG
class MemStats {
public:
	MemStats() : allocated(0), used(0), array_count(0) {}
	MemStats(size_t allocated, size_t used, size_t array_count)
	: allocated(allocated), used(used), array_count(array_count) {}
	MemStats(const MemStats& m) {
		allocated = m.allocated;
		used = m.used;
		array_count = m.array_count;
	}
	void Add(const MemStats& m) {
		allocated += m.allocated;
		used += m.used;
		array_count += m.array_count;
	}
	size_t allocated;
	size_t used;
	size_t array_count;
};
#endif

enum ColumnDef {
	COLUMN_NORMAL,
	COLUMN_NODE,
	COLUMN_HASREFS
};

class Array {
public:
	Array(size_t ref, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	Array(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	Array(ColumnDef type=COLUMN_NORMAL, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	Array(Allocator& alloc);
	Array(const Array& a);

	bool operator==(const Array& a) const;

	void SetType(ColumnDef type);
	void SetParent(Array* parent, size_t pndx);
	void UpdateParentNdx(int diff) {m_parentNdx += diff;}
	Array* GetParent() const {return m_parent;}
	size_t GetParentNdx() const {return m_parentNdx;}
	void UpdateRef(size_t ref);

	bool IsValid() const {return m_data != NULL;}
	void Invalidate() {m_data = NULL;}

	size_t Size() const {return m_len;}
	bool IsEmpty() const {return m_len == 0;}

	bool Insert(size_t ndx, int64_t value);
	bool Add(int64_t value);
	bool Set(size_t ndx, int64_t value);
	int64_t Get(size_t ndx) const;
	int64_t operator[](size_t ndx) const {return Get(ndx);}
	int64_t Back() const;
	void Delete(size_t ndx);
	void Clear();

	bool Increment(int64_t value, size_t start=0, size_t end=(size_t)-1);
	bool IncrementIf(int64_t limit, int64_t value);
	void Adjust(size_t start, int64_t diff);

	size_t FindPos(int64_t value) const;
	size_t FindPos2(int64_t value) const;
	size_t Find(int64_t value, size_t start=0, size_t end=(size_t)-1) const;
	void FindAll(Column& result, int64_t value, size_t offset=0,
				 size_t start=0, size_t end=(size_t)-1) const;
	void FindAllHamming(Column& result, uint64_t value, size_t maxdist, size_t offset=0) const;

	void Sort();

	void Resize(size_t count);

	bool IsNode() const {return m_isNode;}
	bool HasRefs() const {return m_hasRefs;}
	Array GetSubArray(size_t ndx);
	const Array GetSubArray(size_t ndx) const;
	size_t GetRef() const {return m_ref;};
	void Destroy();

	Allocator& GetAllocator() const {return m_alloc;}

	// Serialization
	template<class S> size_t Write(S& target) const;

	// Debug
	size_t GetBitWidth() const {return m_width;}
#ifdef _DEBUG
	bool Compare(const Array& c) const;
	void Print() const;
	void Verify() const;
	void ToDot(FILE* f, bool horizontal=false) const;
	MemStats Stats() const;
#endif //_DEBUG

private:
	Array& operator=(const Array&) {return *this;} // not allowed

protected:
	void Create(size_t ref);

	void DoSort(size_t lo, size_t hi);

	// Getters and Setters for adaptive-packed arrays
	typedef int64_t(Array::*Getter)(size_t) const;
	typedef void(Array::*Setter)(size_t, int64_t);
	int64_t Get_0b(size_t ndx) const;
	int64_t Get_1b(size_t ndx) const;
	int64_t Get_2b(size_t ndx) const;
	int64_t Get_4b(size_t ndx) const;
	int64_t Get_8b(size_t ndx) const;
	int64_t Get_16b(size_t ndx) const;
	int64_t Get_32b(size_t ndx) const;
	int64_t Get_64b(size_t ndx) const;
	void Set_0b(size_t ndx, int64_t value);
	void Set_1b(size_t ndx, int64_t value);
	void Set_2b(size_t ndx, int64_t value);
	void Set_4b(size_t ndx, int64_t value);
	void Set_8b(size_t ndx, int64_t value);
	void Set_16b(size_t ndx, int64_t value);
	void Set_32b(size_t ndx, int64_t value);
	void Set_64b(size_t ndx, int64_t value);

	virtual size_t CalcByteLen(size_t count, size_t width) const;

	void set_header_isnode(bool value, void* header=NULL);
	void set_header_hasrefs(bool value, void* header=NULL);
	void set_header_width(size_t value, void* header=NULL);
	void set_header_len(size_t value, void* header=NULL);
	void set_header_capacity(size_t value, void* header=NULL);
	bool get_header_isnode(const void* header=NULL);
	bool get_header_hasrefs(const void* header=NULL);
	size_t get_header_width(const void* header=NULL);
	size_t get_header_len(const void* header=NULL);
	size_t get_header_capacity(const void* header=NULL);

	void SetWidth(size_t width);
	bool Alloc(size_t count, size_t width);
	bool CopyOnWrite();

	// Member variables
	Getter m_getter;
	Setter m_setter;
	size_t m_ref;
	unsigned char* m_data;
	size_t m_len;
	size_t m_capacity;
	size_t m_width;
	bool m_isNode;
	bool m_hasRefs;
	Array* m_parent;
	size_t m_parentNdx;
	Allocator& m_alloc;
};

// Templates

template<class S>
size_t Array::Write(S& out) const {
	// Calculate who many bytes the array takes up
	const size_t len = CalcByteLen(m_len, m_width);

	// Write header first
	// TODO: replace capacity with checksum
	out.write((const char*)m_data-8, 8);

	// Write array
	const size_t arrayByteLen = len - 8;
	if (arrayByteLen) out.write((const char*)m_data, arrayByteLen);

	// Pad so next block will be 64bit aligned
	const char pad[8] = {0,0,0,0,0,0,0,0};
	const size_t rest = (~len & 0x7)+1; // CHECK
	if (rest < 8) {
		out.write(pad, rest);
		return len + rest;
	}
	else return len; // Return number of bytes written
}

#endif //__TDB_ARRAY__
