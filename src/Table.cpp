#include "Table.h"
#include <assert.h>
#include "Index.h"

const ColumnType Accessor::type = COLUMN_TYPE_INT;
const ColumnType AccessorBool::type = COLUMN_TYPE_BOOL;
const ColumnType AccessorString::type = COLUMN_TYPE_STRING;

Table::Table(const char* name) : m_name(name), m_size(0), m_columns(COLUMN_HASREFS), m_columnNames(COLUMN_NORMAL) {
}

Table::Table(const Table&) {
	//TODO: copy-constructor (ref-counting?)
	assert(false);
}

Table& Table::operator=(const Table&) {
	//TODO: assignment operator (ref-counting?)
	assert(false);
	return *this;
}

Table::~Table() {
	m_columns.Destroy();
	m_columnNames.Destroy();

	// free cached columns
	for (size_t i = 0; i < m_cols.Size(); ++i) {
		ColumnBase* const column = (ColumnBase* const)m_cols.Get(i);
		delete(column);
	}
}

void Table::RegisterColumn(ColumnType type, const char* name) {
	switch (type) {
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_BOOL:
		{
			Column* newColumn = new Column(COLUMN_NORMAL);
			
			m_columnNames.Add((intptr_t)name);

			m_columns.Add((intptr_t)newColumn->GetRef());
			newColumn->SetParent(&m_columns, m_columns.Size()-1);

			m_cols.Add((intptr_t)newColumn);
		}
		break;
	case COLUMN_TYPE_STRING:
		{
			/*Column refs(COLUMN_NORMAL);
			Column lengths(COLUMN_NORMAL);
			
			m_columnNames.Add((int)name);

			const size_t pos = m_columns.Size();
			m_columns.Add((int)refs.GetRef());
			m_columns.Add((int)lengths.GetRef());
			refs.SetParent(&m_columns, pos);
			lengths.SetParent(&m_columns, pos+1);

			StringColumn* newColumn = new StringColumn(refs, lengths);
			m_cols.Add((int)newColumn);*/

			AdaptiveStringColumn* newColumn = new AdaptiveStringColumn();
			
			m_columnNames.Add((intptr_t)name);

			m_columns.Add((intptr_t)newColumn->GetRef());
			newColumn->SetParent(&m_columns, m_columns.Size()-1);

			m_cols.Add((intptr_t)newColumn);
		}
		break;
	default:
		assert(false);
	}
}

bool Table::HasIndex(size_t column_id) const {
	assert(column_id < m_cols.Size());
	const ColumnBase& col = GetColumnBase(column_id);
	return col.HasIndex();
}

void Table::SetIndex(size_t column_id) {
	assert(column_id < m_cols.Size());
	if (HasIndex(column_id)) return;

	ColumnBase& col = GetColumnBase(column_id);
	
	if (col.IsIntColumn()) {
		Column& c = static_cast<Column&>(col);
		Index* index = new Index();
		c.BuildIndex(*index);
		m_columns.Add((intptr_t)index->GetRef());
	}
	else {
		assert(false);
	}
}

ColumnBase& Table::GetColumnBase(size_t ndx) {
	assert(ndx < m_cols.Size());
	return *(ColumnBase* const)m_cols.Get(ndx);
}

const ColumnBase& Table::GetColumnBase(size_t ndx) const {
	assert(ndx < m_cols.Size());
	return *(const ColumnBase* const)m_cols.Get(ndx);
}

Column& Table::GetColumn(size_t ndx) {
	ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsIntColumn());
	return static_cast<Column&>(column);
}

const Column& Table::GetColumn(size_t ndx) const {
	const ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsIntColumn());
	return static_cast<const Column&>(column);
}

AdaptiveStringColumn& Table::GetColumnString(size_t ndx) {
	ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsStringColumn());
	return static_cast<AdaptiveStringColumn&>(column);
}

const AdaptiveStringColumn& Table::GetColumnString(size_t ndx) const {
	const ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsStringColumn());
	return static_cast<const AdaptiveStringColumn&>(column);
}

size_t Table::AddRow() {
	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		ColumnBase& column = GetColumnBase(i);
		column.Add();
	}

	return m_size++;
}

void Table::Clear() {
	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		ColumnBase& column = GetColumnBase(i);
		column.Clear();
	}
	m_size = 0;
}

void Table::DeleteRow(size_t ndx) {
	assert(ndx < m_size);

	const size_t count = m_cols.Size();
	for (size_t i = 0; i < count; ++i) {
		ColumnBase& column = GetColumnBase(i);
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

const char* Table::GetString(size_t column_id, size_t ndx) const {
	assert(column_id < m_columns.Size());
	assert(ndx < m_size);

	const AdaptiveStringColumn& column = GetColumnString(column_id);
	return column.Get(ndx);
}

void Table::SetString(size_t column_id, size_t ndx, const char* value) {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	AdaptiveStringColumn& column = GetColumnString(column_id);
	column.Set(ndx, value);
}

TableView Table::FindAll(size_t column_id, int64_t value) {
	assert(column_id < m_columns.Size());

	const Column& column = GetColumn(column_id);

	TableView view(*this);
	column.FindAll(view.GetRefColumn(), value);

	return view;
}