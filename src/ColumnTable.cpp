#include "ColumnTable.h"
#include "Table.h"


ColumnTable::ColumnTable(size_t ref_specSet, Array* parent, size_t pndx, Allocator& alloc)
: Column(COLUMN_HASREFS, parent, pndx, alloc), m_ref_specSet(ref_specSet) {}

ColumnTable::ColumnTable(size_t ref_column, size_t ref_specSet, Array* parent, size_t pndx, Allocator& alloc)
: Column(ref_column, parent, pndx, alloc), m_ref_specSet(ref_specSet) {}

Table *ColumnTable::get_subtable_ptr(size_t ndx, Table const *parent) const {
	assert(ndx < Size());
	assert(parent);

	// FIXME: Must search local cache for table instance!

	const size_t ref_columns = GetAsRef(ndx);
	Allocator& alloc = GetAllocator();

	return new Table(Table::SubtableTag(), alloc, m_ref_specSet, ref_columns, m_array, ndx, parent);
}

size_t ColumnTable::GetTableSize(size_t ndx) const {
	assert(ndx < Size());

	const size_t ref_columns = GetAsRef(ndx);

	if (ref_columns == 0) return 0;
	else {
		Allocator &alloc = GetAllocator();
		// FIXME: This should be done in a leaner way that avoids
		// instantiation of a Table object.
		// OK to fake that this is not a subtable table, because the
		// operation is read-only.
		return Table(alloc, m_ref_specSet, ref_columns, NULL, 0).GetSize();
	}
}

bool ColumnTable::Add() {
	Insert(Size()); // zero-ref indicates empty table
	return true;
}

void ColumnTable::Insert(size_t ndx) {
	assert(ndx <= Size());
	
	// zero-ref indicates empty table
	Column::Insert(ndx, 0);
}

void ColumnTable::Delete(size_t ndx) {
	assert(ndx < Size());

	const size_t ref_columns = GetAsRef(ndx);

	// Delete sub-tree
	if (ref_columns != 0) {
		Allocator& alloc = GetAllocator();
		Array columns(ref_columns, (Array*)NULL, 0, alloc);
		columns.Destroy();
	}

	Column::Delete(ndx);
}

void ColumnTable::Clear(size_t ndx) {
	assert(ndx < Size());

	const size_t ref_columns = GetAsRef(ndx);
	if (ref_columns == 0) return; // already empty

	// Delete sub-tree
	Allocator& alloc = GetAllocator();
	Array columns(ref_columns, (Array*)NULL, 0, alloc);
	columns.Destroy();

	// Mark as empty table
	Set(ndx, 0);
}

#ifdef _DEBUG

void ColumnTable::Verify() const {
	Column::Verify();

	// Verify each sub-table
	Allocator &alloc = GetAllocator();
	const size_t count = Size();
	for (size_t i = 0; i < count; ++i) {
		const size_t tref = GetAsRef(i);
		if (tref == 0) continue;

		// OK to fake that this is not a subtable table, because the
		// operation is read-only.
		Table(alloc, m_ref_specSet, tref, NULL, 0).Verify();
	}
}

void ColumnTable::LeafToDot(std::ostream& out, const Array& array) const {
	array.ToDot(out);

	Allocator &alloc = GetAllocator();
	const size_t count = array.Size();

	for (size_t i = 0; i < count; ++i) {
		const size_t tref = array.GetAsRef(i);
		if (tref == 0) continue;

		// OK to fake that this is not a subtable table, because the
		// operation is read-only.
		Table(alloc, m_ref_specSet, tref, NULL, 0).ToDot(out);
	}
}

#endif //_DEBUG
