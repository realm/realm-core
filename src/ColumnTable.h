#ifndef __TDB_COLUMN_TABLE__
#define __TDB_COLUMN_TABLE__

#include "Column.h"

class Table;

class ColumnTable {
public:
	ColumnTable(size_t ref_specSet, Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
	ColumnTable(size_t ref_column, size_t ref_specSet, Array* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());

	size_t Size() const {return m_table_refs.Size();}
	bool IsEmpty() const {return m_table_refs.IsEmpty();}

	Table GetTable(size_t ndx);
	const Table GetTable(size_t ndx) const;
	Table* GetTablePtr(size_t ndx);
	size_t GetTableSize(size_t ndx) const;

	void Add();
	void Insert(size_t ndx);
	void Delete(size_t ndx);
	void Clear(size_t ndx);

	size_t GetRef() const {return m_table_refs.GetRef();}
	void SetParent(Array* parent, size_t pndx) {m_table_refs.SetParent(parent, pndx);}

private:
	Column m_table_refs;
	size_t m_ref_specSet;
};

#endif //__TDB_COLUMN_TABLE__
