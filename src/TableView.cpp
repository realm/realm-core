#include "Table.h"

TableView::TableView(Table& source) : m_table(source) {
}

TableView::TableView(const TableView& v) : m_table(v.m_table), m_refs(v.m_refs) {
}

int TableView::Get(size_t column_id, size_t ndx) const {
	return (int)Get64(column_id, ndx);
}

void TableView::Set(size_t column_id, size_t ndx, int value) {
	Set64(column_id, ndx, value);
}

int64_t TableView::Get64(size_t column_id, size_t ndx) const {
	return m_table.Get64(column_id, (size_t)m_refs.Get(ndx));
}

void TableView::Set64(size_t column_id, size_t ndx, int64_t value) {
	m_table.Set64(column_id, (size_t)m_refs.Get(ndx), value);
}

const char* TableView::GetString(size_t column_id, size_t ndx) const {
	return m_table.GetString(column_id, (size_t)m_refs.Get(ndx));
}

void TableView::SetString(size_t column_id, size_t ndx, const char* value) {
	m_table.SetString(column_id, (size_t)m_refs.Get(ndx), value);
}