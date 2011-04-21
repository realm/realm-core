#ifndef __TDB_COLUMN__
#define __TDB_COLUMN__

#include "stdint.h"

enum ColumnDef {
	COLUMN_NORMAL,
	COLUMN_NODE,
	COLUMN_HASREFS
};

class ColumnBase {
public:
	virtual ~ColumnBase() {};

	virtual bool IsIntColumn() const {return false;}
	virtual bool IsStringColumn() const {return false;}

	virtual bool Add() = 0;
	virtual void Clear() = 0;
	virtual void Delete(size_t ndx) = 0;
};

class Column : public ColumnBase {
public:
	Column();
	Column(ColumnDef type, Column* parent=NULL, size_t pndx=0);
	Column(void* ref);
	Column(void* ref, Column* parent, size_t pndx);
	Column(void* ref, const Column* parent, size_t pndx);
	Column(const Column& column);
	~Column();

	bool IsIntColumn() const {return true;}

	Column& operator=(const Column& column);
	bool operator==(const Column& column) const;

	void Create(void* ref);
	void SetParent(Column* column, size_t pndx);

	size_t Size() const;
	bool IsEmpty() const;

	int Get(size_t ndx) const {return (int)Get64(ndx);}
	bool Set(size_t ndx, int value) {return Set64(ndx, value);}
	bool Insert(size_t ndx, int value) {return Insert64(ndx, value);}
	bool Add() {return Add64(0);}
	bool Add(int value) {return Add64(value);}
	
	int64_t Get64(size_t ndx) const;
	bool Set64(size_t ndx, int64_t value);
	bool Insert64(size_t ndx, int64_t value);
	bool Add64(int64_t value);
	
	void Clear();
	void Delete(size_t ndx);
	//void Resize(size_t len);
	bool Reserve(size_t len, size_t width=8);

	bool Increment64(int64_t value, size_t start=0, size_t end=-1);
	size_t Find(int64_t value, size_t start=0, size_t end=-1) const;

	Column GetSubColumn(size_t ndx);
	const Column GetSubColumn(size_t ndx) const;
	void* GetRef() const {return m_data-8;};
	void Destroy();

	void Print() const;
	void Verify() const;

protected:
	// List functions
	size_t ListSize() const {return m_len;}
	bool ListInsert(size_t ndx, int64_t value);
	bool ListAdd(int64_t value);
	bool ListSet(size_t ndx, int64_t value);
	int64_t ListGet(size_t ndx) const;
	int64_t ListBack() const;
	void ListResize(size_t count);
	void ListDelete(size_t ndx);
	size_t ListFindPos(int64_t value) const;
	bool ListIncrement(int64_t value, size_t start=0, size_t end=-1);
	size_t ListFind(int64_t value, size_t start=0, size_t end=-1) const;

	// Node functions
	bool IsNode() const {return m_isNode;}
	bool NodeInsert(size_t ndx, void* ref);
	bool NodeAdd(void* ref);
	bool NodeUpdateOffsets(size_t ndx);
	bool NodeInsertSplit(size_t ndx, void* newRef);
	void UpdateParent(int newRef) {if (m_parent) m_parent->ListSet(m_parentNdx, newRef);}
	
	struct NodeChange {
		void* ref1;
		void* ref2;
		enum ChangeType {
			ERROR,
			NONE,
			INSERT_BEFORE,
			INSERT_AFTER,
			SPLIT
		} type;
		NodeChange(ChangeType t, void* r1=0, void* r2=0) : ref1(r1), ref2(r2), type(t) {}
		NodeChange(bool success) : ref1(NULL), ref2(NULL), type(success ? NONE : ERROR) {}
	};

	// BTree function
	void UpdateRef(void* ref);
	NodeChange DoInsert(size_t ndx, int64_t value);
	
	// Getters and Setters for adaptive-packed lists
	typedef int64_t(Column::*Getter)(size_t) const;
    typedef void(Column::*Setter)(size_t, int64_t);
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

	bool Alloc(size_t count, size_t width);
	void SetWidth(size_t width);

	// Member variables
	Getter m_getter;
	Setter m_setter;
	unsigned char* m_data;
	Column* m_parent;
	size_t m_parentNdx;
	size_t m_len;
	size_t m_capacity;
	size_t m_width;
	bool m_isNode;
	bool m_hasRefs;
};

class StringColumn : public ColumnBase {
public:
	StringColumn(Column& refs, Column& lenghts);
	~StringColumn();

	bool IsStringColumn() const {return true;}

	size_t Size() const {return m_refs.Size();}

	bool Add();
	const char* Get(size_t ndx) const;
	bool Set(size_t ndx, const char* value);
	bool Set(size_t ndx, const char* value, size_t len);
	bool Insert(size_t ndx, const char* value, size_t len);

	void Clear();
	void Delete(size_t ndx);

	size_t Find(const char* value) const;
	size_t Find(const char* value, size_t len) const;

private:
	void* Alloc(const char* value, size_t len);
	void Free(size_t ndx);

	Column m_refs;
	Column m_lengths;
};

class AdaptiveStringColumn : public Column {
public:
	AdaptiveStringColumn();
	~AdaptiveStringColumn();

	bool IsStringColumn() const {return true;}

	const char* Get(size_t ndx) const;
	bool Add();
	bool Add(const char* value);
	bool Set(size_t ndx, const char* value);
	bool Set(size_t ndx, const char* value, size_t len);
	bool Insert(size_t ndx, const char* value, size_t len);
	void Delete(size_t ndx);

	size_t Find(const char* value) const;
	size_t Find(const char* value, size_t len) const;

	void Stats() const;

private:
	bool Alloc(size_t count, size_t width);
};

#endif //__TDB_COLUMN__