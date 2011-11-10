#ifndef __TDB_COLUMN_STRING__
#define __TDB_COLUMN_STRING__

#include "Column.h"
#include "ArrayString.h"
#include "ArrayStringLong.h"

class AdaptiveStringColumn : public ColumnBase {
public:
	AdaptiveStringColumn(Allocator& alloc=DefaultAllocator);
	AdaptiveStringColumn(size_t ref, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	AdaptiveStringColumn(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	~AdaptiveStringColumn();

	void Destroy();

	bool IsStringColumn() const {return true;}

	size_t Size() const;
	bool IsEmpty() const;

	const char* Get(size_t ndx) const;
	bool Add() {return Add("");}
	bool Add(const char* value);
	bool Set(size_t ndx, const char* value);
	bool Insert(size_t ndx, const char* value);
	void Delete(size_t ndx);
	void Clear();

	size_t Find(const char* value, size_t start=0 , size_t end=-1) const;

	// Index
	bool HasIndex() const {return false;}
	void BuildIndex(Index&) {}
	void ClearIndex() {}
	size_t FindWithIndex(int64_t) const {return (size_t)-1;}

	size_t GetRef() const {return m_array->GetRef();}
	void SetParent(Array* parent, size_t pndx) {m_array->SetParent(parent, pndx);}

	// Serialization
	template<class S> size_t Write(S& out, size_t& pos) const;

#ifdef _DEBUG
	bool Compare(const AdaptiveStringColumn& c) const;
	void Verify() const {};
	void ToDot(FILE* f, bool isTop=true) const;
#endif //_DEBUG

protected:
	friend class ColumnBase;
	void UpdateRef(size_t ref);

	const char* LeafGet(size_t ndx) const;
	bool LeafSet(size_t ndx, const char* value);
	bool LeafInsert(size_t ndx, const char* value);
	size_t LeafFind(const char* value, size_t start, size_t end) const {return ((ArrayString*)m_array)->Find(value, start, end);}
	void LeafDelete(size_t ndx);

	template<class S> size_t LeafWrite(S& out, size_t& pos) const;

	bool IsLongStrings() const {return m_array->HasRefs();} // HasRefs indicates long string array
};

// Templates

template<class S>
size_t AdaptiveStringColumn::Write(S& out, size_t& pos) const {
	return TreeWrite<const char*, AdaptiveStringColumn>(out, pos);
}

template<class S>
size_t AdaptiveStringColumn::LeafWrite(S& out, size_t& pos) const {
	if (IsLongStrings()) {
		return ((ArrayStringLong*)m_array)->Write(out, pos);
	}
	else {
		const size_t leaf_pos = pos;
		pos += ((ArrayString*)m_array)->Write(out);
		return leaf_pos;
	}
}

#endif //__TDB_COLUMN_STRING__