#ifndef __TDB_COLUMN_TABLE__
#define __TDB_COLUMN_TABLE__

#include "Column.h"

class Table;

class ColumnTable {
public:
	ColumnTable(size_t ref_specSet, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);
	ColumnTable(size_t ref_column, size_t ref_specSet, Array* parent=NULL, size_t pndx=0, Allocator& alloc=DefaultAllocator);

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

	// Serialization
	template<class S> size_t Write(S& out, size_t& pos) const;

private:
	Column m_table_refs;
	size_t m_ref_specSet;
};

// Templates

#include "Table.h"

template<class S>
size_t ColumnTable::Write(S& out, size_t& pos) const {
	Column newrefs;
	const size_t count = m_table_refs.Size();

	// Write sub-tables
	for (size_t i = 0; i < count; ++i) {
		const size_t ref = m_table_refs.Get(i);
		if (ref == 0) {
			newrefs.Add(0);
		}
		else {
			const Table subtable = GetTable(i);
			const size_t subtablepos = subtable.Write(out, pos);
			newrefs.Add(subtablepos);
		}
	}

	// Write refs
	const size_t columnpos = newrefs.Write(out, pos);

	// Clean--up
	newrefs.Destroy();

	return columnpos;
}


#endif //__TDB_COLUMN_TABLE__
