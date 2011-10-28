#ifndef __TDB_COLUMN__
#define __TDB_COLUMN__

#include "Array.h"

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif
//#include <climits> // size_t
#include <cstdlib> // size_t

// Pre-definitions
class Column;
class Index;

class ColumnBase {
public:
	virtual ~ColumnBase() {};

	virtual bool IsIntColumn() const {return false;}
	virtual bool IsStringColumn() const {return false;}

	virtual bool Add() = 0;
	virtual void Clear() = 0;
	virtual void Delete(size_t ndx) = 0;

	// Indexing
	virtual bool HasIndex() const = 0;
	//virtual Index& GetIndex() = 0;
	virtual void BuildIndex(Index& index) = 0;
	virtual void ClearIndex() = 0;

	virtual size_t GetRef() const = 0;

#ifdef _DEBUG
	virtual void Verify() const = 0;
#endif //_DEBUG

protected:
	struct NodeChange {
		size_t ref1;
		size_t ref2;
		enum ChangeType {
			CT_ERROR,
			CT_NONE,
			CT_INSERT_BEFORE,
			CT_INSERT_AFTER,
			CT_SPLIT
		} type;
		NodeChange(ChangeType t, size_t r1=0, size_t r2=0) : ref1(r1), ref2(r2), type(t) {}
		NodeChange(bool success) : ref1(0), ref2(0), type(success ? CT_NONE : CT_ERROR) {}
	};

	// Tree functions
	template<typename T, class C> T TreeGet(size_t ndx) const;
	template<typename T, class C> bool TreeSet(size_t ndx, T value);
	template<typename T, class C> bool TreeInsert(size_t ndx, T value);
	template<typename T, class C> NodeChange DoInsert(size_t ndx, T value);
	template<typename T, class C> void TreeDelete(size_t ndx);
	template<typename T, class C> size_t TreeFind(T value, size_t start, size_t end) const;
	template<typename T, class C> size_t TreeWrite(std::ostream& out, size_t& pos) const;

	// Node functions
	bool IsNode() const {return m_array->IsNode();}
	const Array NodeGetOffsets() const;
	const Array NodeGetRefs() const;
	Array NodeGetOffsets();
	Array NodeGetRefs();
	template<class C> bool NodeInsert(size_t ndx, size_t ref);
	template<class C> bool NodeAdd(size_t ref);
	bool NodeUpdateOffsets(size_t ndx);
	template<class C> bool NodeInsertSplit(size_t ndx, size_t newRef);
	size_t GetRefSize(size_t ref) const;

	// Member variables
	Array* m_array;
};

class Column : public ColumnBase {
public:
	Column(Allocator& alloc);
	Column(ColumnDef type, Allocator& alloc);
	Column(ColumnDef type=COLUMN_NORMAL, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	Column(size_t ref, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	Column(size_t ref, const Array* parent, size_t pndx, Allocator& alloc=DefaultAllocator);
	Column(const Column& column);
	~Column();

	void Destroy();

	bool IsIntColumn() const {return true;}

	bool operator==(const Column& column) const;

	void SetParent(Array* parent, size_t pndx);

	size_t Size() const;
	bool IsEmpty() const;

	// Overloads for setting values (should catch most integer types)
	int Get(size_t ndx) const {return (int)Get64(ndx);}
	bool Set(size_t ndx, int32_t value) {return Set64(ndx, value);}
	bool Set(size_t ndx, uint32_t value) {return Set64(ndx, value);}
	bool Set(size_t ndx, int64_t value) {return Set64(ndx, value);}
	bool Set(size_t ndx, uint64_t value) {return Set64(ndx, value);}
	bool Insert(size_t ndx, int32_t value) {return Insert64(ndx, value);}
	bool Insert(size_t ndx, uint32_t value) {return Insert64(ndx, value);}
	bool Insert(size_t ndx, int64_t value) {return Insert64(ndx, value);}
	bool Insert(size_t ndx, uint64_t value) {return Insert64(ndx, value);}
	bool Add() {return Add64(0);}
	//bool Add(int32_t value) {return Add64(value);}
	//bool Add(uint32_t value) {return Add64(value);}
	bool Add(int64_t value) {return Add64(value);}
	//bool Add(uint64_t value) {return Add64(value);}
	
	// At the core all integers are 64bit
	int64_t Get64(size_t ndx) const;
	bool Set64(size_t ndx, int64_t value);
	bool Insert64(size_t ndx, int64_t value);
	bool Add64(int64_t value);

	intptr_t GetPtr(size_t ndx) const {return (intptr_t)Get64(ndx);}
	
	void Clear();
	void Delete(size_t ndx);
	//void Resize(size_t len);
	bool Reserve(size_t len, size_t width=8);

	bool Increment64(int64_t value, size_t start=0, size_t end=-1);
	size_t Find(int64_t value, size_t start=0, size_t end=-1) const;
	void FindAll(Column& result, int64_t value, size_t offset=0,
				 size_t start=0, size_t end=-1) const;
	void FindAllHamming(Column& result, uint64_t value, size_t maxdist, size_t offset=0) const;
	size_t FindPos(int64_t value) const;

	// Index
	bool HasIndex() const {return m_index != NULL;}
	Index& GetIndex();
	void BuildIndex(Index& index);
	void ClearIndex();
	size_t FindWithIndex(int64_t value) const;

	size_t GetRef() const {return m_array->GetRef();}

	void Sort();

	// Serialization
	size_t Write(std::ostream& out, size_t& pos) const;

	// Debug
#ifdef _DEBUG
	bool Compare(const Column& c) const;
	void Print() const;
	void Verify() const;
	void ToDot(FILE* f, bool isTop=true) const;
#endif //_DEBUG

private:
	Column& operator=(const Column&) {return *this;} // not allowed

protected:
	friend class ColumnBase;
	void Create();
	void UpdateRef(size_t ref);
	
	// Node functions
	int64_t LeafGet(size_t ndx) const {return m_array->Get(ndx);}
	bool LeafSet(size_t ndx, int64_t value) {return m_array->Set(ndx, value);}
	bool LeafInsert(size_t ndx, int64_t value) {return m_array->Insert(ndx, value);}
	void LeafDelete(size_t ndx) {m_array->Delete(ndx);}
	size_t LeafFind(int64_t value, size_t start, size_t end) const {return m_array->Find(value, start, end);}
	size_t LeafWrite(std::ostream& out) const {return m_array->Write(out);}

	void DoSort(size_t lo, size_t hi);

	// Member variables
	Index* m_index;
};

#include "ArrayString.h"

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
	size_t Write(std::ostream& out, size_t& pos) const;

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
	size_t LeafWrite(std::ostream& out) const {return ((ArrayString*)m_array)->Write(out);}

	bool IsLongStrings() const {return m_array->HasRefs();} // HasRefs indicates long string array
};

// Templates
#include "Column_tpl.h"

#endif //__TDB_COLUMN__
