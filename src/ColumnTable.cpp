#include "ColumnTable.h"


ColumnTable::ColumnTable(size_t ref_specSet, Array* parent, size_t pndx, Allocator& alloc)
: m_table_refs(COLUMN_HASREFS, parent, pndx, alloc), m_ref_specSet(ref_specSet) {}

ColumnTable::ColumnTable(size_t ref_column, size_t ref_specSet, Array* parent, size_t pndx, Allocator& alloc)
: m_table_refs(ref_column, parent, pndx, alloc), m_ref_specSet(ref_specSet) {}

Table ColumnTable::GetTable(size_t ndx) {
	assert(ndx < m_table_refs.Size());

	const size_t ref_columns = m_table_refs.Get(ndx);
	Allocator& alloc = m_table_refs.GetAllocator();

	// Get parent info for subtable
	Array* parent = NULL;
	size_t pndx   = 0;
	m_table_refs.GetParentInfo(ndx, parent, pndx);

	return Table(alloc, m_ref_specSet, ref_columns, parent, pndx);
}

Table* ColumnTable::GetTablePtr(size_t ndx) {
	assert(ndx < m_table_refs.Size());

	const size_t ref_columns = m_table_refs.Get(ndx);
	Allocator& alloc = m_table_refs.GetAllocator();

	// Get parent info for subtable
	Array* parent = NULL;
	size_t pndx   = 0;
	m_table_refs.GetParentInfo(ndx, parent, pndx);

	return new Table(alloc, m_ref_specSet, ref_columns, parent, pndx);
}

size_t ColumnTable::GetTableSize(size_t ndx) const {
	assert(ndx < m_table_refs.Size());

	const size_t ref_columns = m_table_refs.Get(ndx);

	if (ref_columns == 0) return 0;
	else {
		Allocator& alloc = m_table_refs.GetAllocator();
		const Table table(alloc, m_ref_specSet, ref_columns, NULL, 0);
		return table.GetSize();
	}
}

void ColumnTable::Add() {
	m_table_refs.Add(0);
}

void ColumnTable::Insert(size_t ndx) {
	assert(ndx <= m_table_refs.Size());
	m_table_refs.Insert(ndx, 0);
}

void ColumnTable::Delete(size_t ndx) {
	assert(ndx < m_table_refs.Size());

	const size_t ref_columns = m_table_refs.Get(ndx);

	// Delete sub-tree
	if (ref_columns != 0) {
		Allocator& alloc = m_table_refs.GetAllocator();
		Array columns(ref_columns, (Array*)NULL, 0, alloc);
		columns.Destroy();
	}

	m_table_refs.Delete(ndx);
}
