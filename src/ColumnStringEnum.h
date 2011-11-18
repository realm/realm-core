#ifndef __TDB_COLUMN_STRING_ENUM__
#define __TDB_COLUMN_STRING_ENUM__

#include "ColumnString.h"

class ColumnStringEnum {
public:
	ColumnStringEnum(size_t ref_keys, size_t ref_values, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ColumnStringEnum(size_t ref_keys, size_t ref_values, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	~ColumnStringEnum();
	void Destroy();

	size_t Size() const;
	bool IsEmpty() const;

	const char* Get(size_t ndx) const;
	bool Add(const char* value);
	bool Set(size_t ndx, const char* value);
	bool Insert(size_t ndx, const char* value);
	void Delete(size_t ndx);
	void Clear();

	size_t Find(const char* value, size_t start=0, size_t end=-1) const;

	void UpdateParentNdx(int diff);

	// Serialization
	template<class S> void Write(S& out, size_t& pos, size_t& ref_keys, size_t& ref_values) const;

#ifdef _DEBUG
	bool Compare(const ColumnStringEnum& c) const;
	void Verify() const;
	MemStats Stats() const;
#endif // _DEBUG

private:
	size_t GetKeyNdx(const char* value);

	// Member variables
	AdaptiveStringColumn m_keys;
	Column m_values;
};

// Templates

template<class S>
void ColumnStringEnum::Write(S& out, size_t& pos, size_t& ref_keys, size_t& ref_values) const {
	ref_keys = m_keys.Write(out, pos);
	ref_values = m_values.Write(out, pos);
}

#endif //__TDB_COLUMN_STRING_ENUM__
