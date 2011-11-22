#include "Table.h"
#include <assert.h>

TableView::TableView(Table& source) : m_table(source) {
}

TableView::TableView(const TableView& v) : m_table(v.m_table), m_refs(v.m_refs) {
}

TableView::~TableView() {
	m_refs.Destroy();
}

int64_t TableView::Get(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	assert(ndx < m_refs.Size());


	printf("HEJ\n");

	const size_t real_ndx = m_refs.Get(ndx);
	return m_table.Get(column_id, real_ndx);
}

bool TableView::GetBool(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	return m_table.GetBool(column_id, real_ndx);
}

time_t TableView::GetDate(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	return m_table.GetDate(column_id, real_ndx);
}

const char* TableView::GetString(size_t column_id, size_t ndx) const {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	return m_table.GetString(column_id, real_ndx);
}

void TableView::Set(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_INT);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	m_table.Set(column_id, real_ndx, value);
}

void TableView::SetBool(size_t column_id, size_t ndx, bool value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	m_table.SetBool(column_id, real_ndx, value);
}

void TableView::SetDate(size_t column_id, size_t ndx, time_t value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	m_table.SetDate(column_id, real_ndx, value);
}


void TableView::SetString(size_t column_id, size_t ndx, const char* value) {
	assert(column_id < m_table.GetColumnCount());
	assert(m_table.GetColumnType(column_id) == COLUMN_TYPE_STRING);
	assert(ndx < m_refs.Size());

	const size_t real_ndx = m_refs.Get(ndx);
	m_table.SetString(column_id, real_ndx, value);
}
