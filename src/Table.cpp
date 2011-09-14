#include "Table.h"
#include <assert.h>
#include "Index.h"

const ColumnType Accessor::type = COLUMN_TYPE_INT;
const ColumnType AccessorBool::type = COLUMN_TYPE_BOOL;
const ColumnType AccessorString::type = COLUMN_TYPE_STRING;
const ColumnType AccessorDate::type = COLUMN_TYPE_DATE;

Table::Table(const char* name, Allocator& alloc) : m_name(name), m_size(0), m_columns(COLUMN_HASREFS, NULL, 0, alloc), m_alloc(alloc) {
}

Table::Table(const Table& t) : m_alloc(t.m_alloc) {
	//TODO: copy-constructor (ref-counting?)
	assert(false);
}

Table& Table::operator=(const Table&) {
	//TODO: assignment operator (ref-counting?)
	assert(false);
	return *this;
}

Table::~Table() {
	m_spec.Destroy();
	m_columns.Destroy();
	m_columnNames.Destroy();

	// free cached columns
	for (size_t i = 0; i < m_cols.Size(); ++i) {
		ColumnBase* const column = (ColumnBase* const)m_cols.Get(i);
		delete(column);
	}
	m_cols.Destroy();
}

size_t Table::GetColumnCount() const {
	return m_cols.Size();
}

const char* Table::GetColumnName(size_t ndx) const {
	assert(ndx < GetColumnCount());
	return m_columnNames.Get(ndx);
}

size_t Table::GetColumnIndex(const char* name) const {
	return m_columnNames.Find(name);
}

ColumnType Table::GetColumnType(size_t ndx) const {
	assert(ndx < GetColumnCount());
	return (ColumnType)m_spec.Get(ndx);
}

size_t Table::RegisterColumn(ColumnType type, const char* name) {
	const size_t column_ndx = m_cols.Size();

	switch (type) {
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_BOOL:
		{
			Column* newColumn = new Column(COLUMN_NORMAL, m_alloc);
			
			m_columnNames.Add(name);
			m_spec.Add(type);

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

			AdaptiveStringColumn* newColumn = new AdaptiveStringColumn(m_alloc);
			
			m_columnNames.Add(name);
			m_spec.Add(type);

			m_columns.Add((intptr_t)newColumn->GetRef());
			newColumn->SetParent(&m_columns, m_columns.Size()-1);

			m_cols.Add((intptr_t)newColumn);
		}
		break;
	default:
		assert(false);
	}

	return column_ndx;
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
	--m_size;
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

int64_t Table::Get64(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return column.Get64(ndx);
}

void Table::Set64(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set64(ndx, value);
}

bool Table::GetBool(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return column.Get64(ndx) != 0;
}

void Table::SetBool(size_t column_id, size_t ndx, bool value) {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set64(ndx, value ? 1 : 0);
}

time_t Table::GetDate(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return (time_t)column.Get64(ndx) != 0;
}

void Table::SetDate(size_t column_id, size_t ndx, time_t value) {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set64(ndx, (int64_t)value);
}

void Table::InsertInt(size_t column_id, size_t ndx, int value) {
	assert(column_id < m_cols.Size());
	assert(ndx <= m_size);

	Column& column = GetColumn(column_id);
	column.Insert(ndx, value);
}

void Table::InsertInt(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_cols.Size());
	assert(ndx <= m_size);

	Column& column = GetColumn(column_id);
	column.Insert64(ndx, value);
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

void Table::InsertString(size_t column_id, size_t ndx, const char* value) {
	assert(column_id < m_cols.Size());
	assert(ndx <= m_size);

	AdaptiveStringColumn& column = GetColumnString(column_id);
	column.Insert(ndx, value, strlen(value));
}

void Table::InsertDone() {
	++m_size;

#ifdef _DEBUG
	Verify();
#endif //_DEBUG
}

size_t Table::Find(size_t column_id, int64_t value) const {
	assert(column_id < m_columns.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_INT);
	const Column& column = GetColumn(column_id);

	return column.Find(value);
}

size_t Table::FindBool(size_t column_id, bool value) const {
	assert(column_id < m_columns.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	const Column& column = GetColumn(column_id);

	return column.Find(value ? 1 : 0);
}

size_t Table::FindDate(size_t column_id, time_t value) const {
	assert(column_id < m_columns.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_DATE);
	const Column& column = GetColumn(column_id);

	return column.Find((int64_t)value);
}

size_t Table::FindString(size_t column_id, const char* value) const {
	assert(column_id < m_columns.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_STRING);
	const AdaptiveStringColumn& column = GetColumnString(column_id);

	return column.Find(value);
}

void Table::FindAll(TableView& tv, size_t column_id, int64_t value) {
	assert(column_id < m_columns.Size());
	assert(&tv.GetParent() == this);

	const Column& column = GetColumn(column_id);

	column.FindAll(tv.GetRefColumn(), value);
}

void Table::FindAllHamming(TableView& tv, size_t column_id, uint64_t value, size_t max) {
	assert(column_id < m_columns.Size());
	assert(&tv.GetParent() == this);

	const Column& column = GetColumn(column_id);

	column.FindAllHamming(tv.GetRefColumn(), value, max);
}

#ifdef _DEBUG
#include "stdio.h"

void Table::Verify() const {
	const size_t column_count = GetColumnCount();
	assert(column_count == m_cols.Size());
	assert(column_count == m_columnNames.Size());
	assert(column_count == m_spec.Size());

	for (size_t i = 0; i < column_count; ++i) {
		const ColumnType type = GetColumnType(i);
		switch (type) {
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_BOOL:
			{
				const Column& column = GetColumn(i);
				assert(column.Size() == m_size);
				column.Verify();
			}
			break;
		case COLUMN_TYPE_STRING:
			{
				const AdaptiveStringColumn& column = GetColumnString(i);
				assert(column.Size() == m_size);
				column.Verify();
			}
			break;
		default:
			assert(false);
		}
	}

	m_alloc.Verify();
}

void Table::ToDot(const char* filename) const {
	FILE* f = NULL;
	if (fopen_s(&f, filename, "w") !=0) return;

	fprintf(f, "digraph G {\n");
	fprintf(f, "node [shape=record];\n");

	// Table header
	fprintf(f, "table [label=\"{");
	const size_t column_count = GetColumnCount();
	for (size_t i = 0; i < column_count; ++i) {
		if (i > 0) fprintf(f, "} | {");
		fprintf(f, "%s | ", m_columnNames.Get(i));
	
		const ColumnType type = GetColumnType(i);
		switch (type) {
		case COLUMN_TYPE_INT:
			fprintf(f, "Int"); break;
		case COLUMN_TYPE_BOOL:
			fprintf(f, "Bool"); break;
		case COLUMN_TYPE_STRING:
			fprintf(f, "String"); break;
		default:
			assert(false);
		}

		fprintf(f, "| <%zu>", i);
	}
	fprintf(f, "}\"];\n");

	// Refs
	for (size_t i = 0; i < column_count; ++i) {
		const ColumnBase& column = GetColumnBase(i);
		const size_t ref = column.GetRef();
		fprintf(f, "table:%zu -> n%zx\n", i, ref);
	}


	// Columns
	for (size_t i = 0; i < column_count; ++i) {
		const ColumnType type = GetColumnType(i);
		switch (type) {
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_BOOL:
			{
				const Column& column = GetColumn(i);
				column.ToDot(f);
			}
			break;
		case COLUMN_TYPE_STRING:
			{
				const AdaptiveStringColumn& column = GetColumnString(i);
				column.ToDot(f);
			}
			break;
		default:
			assert(false);
		}
	}

	fprintf(f, "}\n");

	fclose(f);
}

void Table::Print() const {

	// Table header
	printf("Table \"%s\" len(%zu)\n    ", m_name, m_size);
	const size_t column_count = GetColumnCount();
	for (size_t i = 0; i < column_count; ++i) {
		printf("%-10s ", m_columnNames.Get(i));
	}

	// Types
	printf("\n    ");
	for (size_t i = 0; i < column_count; ++i) {
		const ColumnType type = GetColumnType(i);
		switch (type) {
		case COLUMN_TYPE_INT:
			printf("Int        "); break;
		case COLUMN_TYPE_BOOL:
			printf("Bool       "); break;
		case COLUMN_TYPE_STRING:
			printf("String     "); break;
		default:
			assert(false);
		}
	}
	printf("\n");

	// Columns
	for (size_t i = 0; i < m_size; ++i) {
		printf("%3zu ", i);
		for (size_t n = 0; n < column_count; ++n) {
			const ColumnType type = GetColumnType(n);
			switch (type) {
			case COLUMN_TYPE_INT:
				{
					const Column& column = GetColumn(n);
					printf("%10d ", column.Get(i));
				}
				break;
			case COLUMN_TYPE_BOOL:
				{
					const Column& column = GetColumn(n);
					printf(column.Get(i) == 0 ? "     false " : "      true ");
				}
				break;
			case COLUMN_TYPE_STRING:
				{
					const AdaptiveStringColumn& column = GetColumnString(n);
					printf("%10s ", column.Get(i));
				}
				break;
			default:
				assert(false);
			}
		}
		printf("\n");
	}
	printf("\n");
}

#endif //_DEBUG