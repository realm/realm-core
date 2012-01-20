#ifndef __TDB_COLUMN_MIXED__
#define __TDB_COLUMN_MIXED__

#include "Column.h"
#include "ColumnType.h"
#include "Table.h"

// Pre-declarations
class ColumnBinary;

class ColumnMixed : public ColumnBase {
public:
	ColumnMixed(Allocator& alloc=GetDefaultAllocator());
	ColumnMixed(size_t ref, Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
	~ColumnMixed();
	void Destroy();
	
	void SetParent(Array* parent, size_t pndx);
	
	ColumnType GetType(size_t ndx) const;
	size_t Size() const {return m_types->Size();}
	bool IsEmpty() const {return m_types->IsEmpty();}
	
	int64_t GetInt(size_t ndx) const;
	bool GetBool(size_t ndx) const;
	time_t GetDate(size_t ndx) const;
	const char* GetString(size_t ndx) const;
	BinaryData GetBinary(size_t ndx) const;
	Table* GetTablePtr(size_t ndx);
	
	void SetInt(size_t ndx, int64_t value);
	void SetBool(size_t ndx, bool value);
	void SetDate(size_t ndx, time_t value);
	void SetString(size_t ndx, const char* value);
	void SetBinary(size_t ndx, const char* value, size_t len);
	void SetTable(size_t ndx);
	
	void InsertInt(size_t ndx, int64_t value);
	void InsertBool(size_t ndx, bool value);
	void InsertDate(size_t ndx, time_t value);
	void InsertString(size_t ndx, const char* value);
	void InsertBinary(size_t ndx, const char* value, size_t len);
	void InsertTable(size_t ndx);
	
	bool Add();
	void Clear(){}
	void Delete(size_t ndx);
	
	// Indexing
	bool HasIndex() const {return false;}
	void BuildIndex(Index& index) {}
	void ClearIndex() {}
	
	size_t GetRef() const {return m_array->GetRef();}
	
#ifdef _DEBUG
	void Verify() const {}
#endif //_DEBUG
	
private:
	void Create(size_t ref, Array* parent, size_t pndx, Allocator& alloc);
	void InitDataColumn();
	
	void ClearValue(size_t ndx, ColumnType newtype);
	
	// Member variables
	Column*       m_types;
	Column*       m_refs;
	ColumnBinary* m_data;
};

#endif //__TDB_COLUMN_MIXED__
