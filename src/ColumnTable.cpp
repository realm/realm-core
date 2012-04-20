#include "ColumnTable.h"

using namespace std;

namespace {

using namespace tightdb;

struct FakeParent: Table::Parent
{
	virtual void update_child_ref(size_t, size_t) {} // Ignore
	virtual void child_destroyed(size_t) {} // Ignore
#ifdef _DEBUG
	virtual size_t get_child_ref_for_verify(size_t) const { return 0; }
#endif
};

}


namespace tightdb {

void ColumnSubtableParent::child_destroyed(size_t subtable_ndx)
{
	m_subtable_map.remove(subtable_ndx);
	// Note that this column instance may be destroyed upon return
	// from Table::unbind_ref().
	if (m_table && m_subtable_map.empty()) m_table->unbind_ref();
}

void ColumnSubtableParent::register_subtable(size_t subtable_ndx, Table *subtable) const
{
	bool const was_empty = m_subtable_map.empty();
	m_subtable_map.insert(subtable_ndx, subtable);
	if (m_table && was_empty) m_table->bind_ref();
}


ColumnTable::ColumnTable(size_t ref_specSet, ArrayParent *parent, size_t pndx,
						 Allocator &alloc, Table const *tab):
	ColumnSubtableParent(parent, pndx, alloc, tab), m_ref_specSet(ref_specSet) {}

ColumnTable::ColumnTable(size_t ref_column, size_t ref_specSet, ArrayParent *parent, size_t pndx,
						 Allocator& alloc, Table const *tab):
	ColumnSubtableParent(ref_column, parent, pndx, alloc, tab), m_ref_specSet(ref_specSet) {}

Table *ColumnTable::get_subtable_ptr(size_t ndx) const {
	assert(ndx < Size());

	Table *subtable = m_subtable_map.find(ndx);
	if (subtable) return subtable;

	size_t const ref_columns = GetAsRef(ndx);
	Allocator& alloc = GetAllocator();

	subtable = new Table(Table::SubtableTag(), alloc, m_ref_specSet, ref_columns,
						 const_cast<ColumnTable *>(this), ndx);
	register_subtable(ndx, subtable);
	return subtable;
}

size_t ColumnTable::GetTableSize(size_t ndx) const {
	assert(ndx < Size());

	const size_t ref_columns = GetAsRef(ndx);

	if (ref_columns == 0) return 0;
	else {
		Allocator &alloc = GetAllocator();
		// FIXME: This should be done in a leaner way that avoids
		// instantiation of a Table object.
		// OK to use a fake parent, because the operation is read-only.
		FakeParent fake_parent;
		return Table(alloc, m_ref_specSet, ref_columns, &fake_parent, 0).GetSize();
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

		// OK to fake that this is not a subtable, because the
		// operation is read-only.
		Table t(alloc, m_ref_specSet, tref, const_cast<ColumnTable *>(this), i);
		register_subtable(i, &t);
		t.Verify();
	}
}

void ColumnTable::LeafToDot(std::ostream& out, const Array& array) const {
	array.ToDot(out);

	Allocator &alloc = GetAllocator();
	const size_t count = array.Size();

	for (size_t i = 0; i < count; ++i) {
		const size_t tref = array.GetAsRef(i);
		if (tref == 0) continue;

		// OK to use a fake parent, because the operation is read-only.
		FakeParent fake_parent;
		Table(alloc, m_ref_specSet, tref, &fake_parent, 0).ToDot(out);
	}
}

#endif //_DEBUG

}
