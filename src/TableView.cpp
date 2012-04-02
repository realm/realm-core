#include "Table.h"
#include "Column.h"
#include <assert.h>

TableView::TableView(Table& source) : m_table(source) {
}

TableView::TableView(const TableView& v) : m_table(v.m_table), m_refs(v.m_refs) {
}

TableView::~TableView() {
	m_refs.Destroy();
}

Table *TableView::GetTable(void) {
	return &m_table;
}

// Searching
size_t TableView::Find(size_t column_id, int64_t value) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	
	for(size_t i = 0; i < m_refs.Size(); i++)
		if(Get(column_id, i) == value)
			return i;

	return (size_t)-1;
}

void TableView::FindAll(TableView& tv, size_t column_id, int64_t value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	
	for(size_t i = 0; i < m_refs.Size(); i++)
		if(Get(column_id, i) == value)			
			tv.GetRefColumn().Add(i);
}

size_t TableView::FindString(size_t column_id, const char* value) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);

	for(size_t i = 0; i < m_refs.Size(); i++)
	if(strcmp(GetString(column_id, i), value) == 0)
		return i;

	return (size_t)-1;
}


void TableView::FindAllString(TableView& tv, size_t column_id, const char *value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);

	for(size_t i = 0; i < m_refs.Size(); i++)
	if(strcmp(GetString(column_id, i), value) == 0)
		tv.GetRefColumn().Add(i);
}

int64_t TableView::Sum(size_t column_id) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	int64_t sum = 0;

	for(size_t i = 0; i < m_refs.Size(); i++)
		sum += Get(column_id, i);

	return sum;
}

int64_t TableView::Max(size_t column_id) const {
	if (IsEmpty()) return 0;
	if (m_refs.Size() == 0) return 0;

	int64_t mv = Get(column_id, 0);
	for (size_t i = 1; i < m_refs.Size(); ++i) {
		const int64_t v = Get(column_id, i);
		if (v > mv) {
			mv = v;
		}
	}
	return mv;
}

int64_t TableView::Min(size_t column_id) const {
	if (IsEmpty()) return 0;
	if (m_refs.Size() == 0) return 0;

	int64_t mv = Get(column_id, 0);
	for (size_t i = 1; i < m_refs.Size(); ++i) {
		const int64_t v = Get(column_id, i);
		if (v < mv) {
			mv = v;
		}
	}
	return mv;
}

int64_t TableView::Get(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	return m_table.Get(column_id, real_ndx);
}

bool TableView::GetBool(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	return m_table.GetBool(column_id, real_ndx);
}

time_t TableView::GetDate(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	return m_table.GetDate(column_id, real_ndx);
}

const char* TableView::GetString(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	return m_table.GetString(column_id, real_ndx);
}

TableRef TableView::GetTable(size_t column_id, size_t ndx) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_TABLE);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	return m_table.GetTable(column_id, real_ndx);
}

void TableView::Set(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	m_table.Set(column_id, real_ndx, value);
}

void TableView::SetBool(size_t column_id, size_t ndx, bool value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	m_table.SetBool(column_id, real_ndx, value);
}

void TableView::SetDate(size_t column_id, size_t ndx, time_t value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	m_table.SetDate(column_id, real_ndx, value);
}

void TableView::SetString(size_t column_id, size_t ndx, const char* value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.GetAsRef(ndx);
	m_table.SetString(column_id, real_ndx, value);
}


void TableView::Sort(size_t column, bool Ascending) {
	assert(m_table.GetColumnType(column) == COLUMN_TYPE_INT || m_table.GetColumnType(column) == COLUMN_TYPE_DATE || m_table.GetColumnType(column) == COLUMN_TYPE_BOOL);

	if(m_refs.Size() == 0)
		return;

	Array vals;
	Array ref;
	Array result;

	//ref.Preset(0, m_refs.Size() - 1, m_refs.Size());
	for(size_t t = 0; t < m_refs.Size(); t++)
		ref.Add(t);
	
	// Extract all values from the Column and put them in an Array because Array is much faster to operate on
	// with rand access (we have ~log(n) accesses to each element, so using 1 additional read to speed up the rest is faster)
	if(m_table.GetColumnType(column) == COLUMN_TYPE_INT) {
		for(size_t t = 0; t < m_refs.Size(); t++) {
			int64_t v = m_table.Get(column, m_refs.GetAsRef(t));
			vals.Add(v);
		}
	}
	else if(m_table.GetColumnType(column) == COLUMN_TYPE_DATE) {
		for(size_t t = 0; t < m_refs.Size(); t++) {
			size_t idx = m_refs.GetAsRef(t);
			int64_t v = (int64_t)m_table.GetDate(column, idx);
			vals.Add(v);
		}	
	}
	else if(m_table.GetColumnType(column) == COLUMN_TYPE_BOOL) {
		for(size_t t = 0; t < m_refs.Size(); t++) {
			size_t idx = m_refs.GetAsRef(t);
			int64_t v = (int64_t)m_table.GetBool(column, idx);
			vals.Add(v);
		}		
	}

	vals.ReferenceSort(ref);
	vals.Destroy();

	for(size_t t = 0; t < m_refs.Size(); t++) {
		size_t r = ref.GetAsRef(t);
		size_t rr = m_refs.GetAsRef(r);
		result.Add(rr);
	}

	ref.Destroy();

	// Copy result to m_refs (todo, there might be a shortcut)
	m_refs.Clear();
	if(Ascending) {
		for(size_t t = 0; t < ref.Size(); t++) {
			size_t v = result.GetAsRef(t);
			m_refs.Add(v);
		}
	} 
	else {
		for(size_t t = 0; t < ref.Size(); t++) {
			size_t v = result.GetAsRef(ref.Size() - t - 1);
				m_refs.Add(v);
		}
	}
	result.Destroy();
}

void TableView::Delete(size_t ndx) {
	assert(ndx < m_refs.Size());
	
	// Delete row in source table
	const size_t real_ndx = m_refs.GetAsRef(ndx);
	m_table.DeleteRow(real_ndx);
	
	// Update refs
	m_refs.Delete(ndx);
	m_refs.IncrementIf(ndx, -1);
}

void TableView::Clear() {
	m_refs.Sort();
	
	// Delete all referenced rows in source table
	// (in reverse order to avoid index drift)
	const size_t count = m_refs.Size();
	for (size_t i = count; i; --i) {
		const size_t ndx = m_refs.GetAsRef(i-1);
		m_table.DeleteRow(ndx);
	}
	
	m_refs.Clear();
}
