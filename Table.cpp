#include "Table.h"
#include <assert.h>

Table::Table(const char* name) : m_name(name), m_size(0), m_columns(COLUMN_HASREFS), m_columnNames(COLUMN_NORMAL) {
}

Table::~Table() {
	m_columns.Destroy();
	m_columnNames.Destroy();

	//TODO: free cached columns
}

void Table::RegisterColumn(const char* name) {
	Column* newColumn = new Column(COLUMN_NORMAL);
	
	m_columnNames.Add((int)name);

	m_columns.Add((int)newColumn->GetRef());
	newColumn->SetParent(&m_columns, m_columns.Size()-1);

	m_cols.Add((int)newColumn);
}

Column& Table::GetColumn(size_t ndx) {
	assert(ndx < m_cols.Size());
	//TODO: verify that it is an int column

	return *(Column*)m_cols.Get(ndx);
}

const Column& Table::GetColumn(size_t ndx) const {
	assert(ndx < m_cols.Size());
	//TODO: verify that it is an int column

	return *(const Column*)m_cols.Get(ndx);
}

size_t Table::AddRow() {
	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		Column& column = GetColumn(i);
		column.Add(0);
	}

	return m_size++;
}

void Table::Clear() {
	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		Column& column = GetColumn(i);
		column.Clear();
	}
	m_size = 0;
}

void Table::DeleteRow(size_t ndx) {
	assert(ndx < m_size);

	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		Column& column = GetColumn(i);
		column.Delete(ndx);
	}
}

int Table::Get(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return column.Get(ndx);
}

void Table::Set(size_t column_id, size_t ndx, int value) {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set(ndx, value);
}
