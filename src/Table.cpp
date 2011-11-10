#include "Table.h"
#include <assert.h>
#include "Index.h"
#include <iostream>
#include <fstream>
#include "AllocSlab.h"

const ColumnType Accessor::type = COLUMN_TYPE_INT;
const ColumnType AccessorBool::type = COLUMN_TYPE_BOOL;
const ColumnType AccessorString::type = COLUMN_TYPE_STRING;
const ColumnType AccessorDate::type = COLUMN_TYPE_DATE;

Table::Table(Allocator& alloc)
: m_size(0),  m_top(COLUMN_HASREFS, NULL, 0, alloc), m_spec(COLUMN_NORMAL, NULL, 0, alloc), m_columnNames(NULL, 0, alloc), m_columns(COLUMN_HASREFS, NULL, 0, alloc), m_alloc(alloc)
{
    m_top.Add(m_spec.GetRef());
    m_top.Add(m_columnNames.GetRef());
    m_top.Add(m_columns.GetRef());

    m_spec.SetParent(&m_top, 0);
    m_columnNames.SetParent(&m_top, 1);
    m_columns.SetParent(&m_top, 2);
}


Table::Table(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : m_size(0), m_top(alloc), m_spec(alloc), m_columnNames(alloc), m_columns(alloc), m_alloc(alloc)
{
    // Load from allocated memory
    m_top.UpdateRef(ref);
    m_top.SetParent(parent, pndx);
    assert(m_top.Size() == 3);

    m_spec.UpdateRef(m_top.Get(0));
    m_spec.SetParent(&m_top, 0);
    m_columnNames.UpdateRef(m_top.Get(1));
    m_columnNames.SetParent(&m_top, 1);
    m_columns.UpdateRef(m_top.Get(2));
    m_columns.SetParent(&m_top, 2);

    // Cache columns
    size_t size = (size_t)-1;
    for (size_t i = 0; i < m_spec.Size(); ++i) {
        const ColumnType type = (ColumnType)m_spec.Get(i);
        const size_t ref = m_columns.Get(i);

        ColumnBase* newColumn = NULL;
		size_t colsize = (size_t)-1;
		switch (type) {
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOL:
			case COLUMN_TYPE_DATE:
				newColumn = new Column(ref, &m_columns, i, m_alloc);
				colsize = ((Column*)newColumn)->Size();
                break;
            case COLUMN_TYPE_STRING:
                newColumn = new AdaptiveStringColumn(ref, &m_columns, i, m_alloc);
				colsize = ((AdaptiveStringColumn*)newColumn)->Size();
                break;
			case COLUMN_TYPE_BINARY:
				newColumn = new ColumnBinary(ref, &m_columns, i, m_alloc);
				colsize = ((ColumnBinary*)newColumn)->Size();
                break;
            default:
                assert(false);
        }

		m_cols.Add((intptr_t)newColumn);

		// Set table size
		// (and verify that all column are same length)
		if (size == (size_t)-1) size = colsize;
		else assert(size == colsize);
    }

    if (size != (size_t)-1) m_size = size;
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
	// Destroying m_top will also destroy
	// m_spec, m_columns and m_columnNames
	m_top.Destroy();

	// free cached columns
	for (size_t i = 0; i < m_cols.Size(); ++i) {
		ColumnBase* const column = (ColumnBase* const)m_cols.Get(i);
		delete(column);
	}
	m_cols.Destroy();
}

void Table::SetParent(Array* parent, size_t pndx) {
    m_top.SetParent(parent, pndx);
}

size_t Table::GetRef() const {
    return m_top.GetRef();
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

	ColumnBase* newColumn = NULL;

	switch (type) {
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_BOOL:
	case COLUMN_TYPE_DATE:
		newColumn = new Column(COLUMN_NORMAL, m_alloc);
		m_columns.Add(((Column*)newColumn)->GetRef());
		((Column*)newColumn)->SetParent(&m_columns, m_columns.Size()-1);
		break;
	case COLUMN_TYPE_STRING:
		newColumn = new AdaptiveStringColumn(m_alloc);
		m_columns.Add(((AdaptiveStringColumn*)newColumn)->GetRef());
		((Column*)newColumn)->SetParent(&m_columns, m_columns.Size()-1);
		break;
	case COLUMN_TYPE_BINARY:
		newColumn = new ColumnBinary(m_alloc);
		m_columns.Add(((ColumnBinary*)newColumn)->GetRef());
		((ColumnBinary*)newColumn)->SetParent(&m_columns, m_columns.Size()-1);
		break;
	default:
		assert(false);
	}

	m_columnNames.Add(name);
	m_spec.Add(type);
	m_cols.Add((intptr_t)newColumn);

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

ColumnBinary& Table::GetColumnBinary(size_t ndx) {
	ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsBinaryColumn());
	return static_cast<ColumnBinary&>(column);
}

const ColumnBinary& Table::GetColumnBinary(size_t ndx) const {
	const ColumnBase& column = GetColumnBase(ndx);
	assert(column.IsBinaryColumn());
	return static_cast<const ColumnBinary&>(column);
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

int64_t Table::Get(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return column.Get(ndx);
}

void Table::Set(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set(ndx, value);
}

bool Table::GetBool(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return column.Get(ndx) != 0;
}

void Table::SetBool(size_t column_id, size_t ndx, bool value) {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_BOOL);
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set(ndx, value ? 1 : 0);
}

time_t Table::GetDate(size_t column_id, size_t ndx) const {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_size);

	const Column& column = GetColumn(column_id);
	return (time_t)column.Get(ndx) != 0;
}

void Table::SetDate(size_t column_id, size_t ndx, time_t value) {
	assert(column_id < m_cols.Size());
	assert(GetColumnType(column_id) == COLUMN_TYPE_DATE);
	assert(ndx < m_size);

	Column& column = GetColumn(column_id);
	column.Set(ndx, (int64_t)value);
}

void Table::InsertInt(size_t column_id, size_t ndx, int64_t value) {
	assert(column_id < m_cols.Size());
	assert(ndx <= m_size);

	Column& column = GetColumn(column_id);
	column.Insert(ndx, value);
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
	column.Insert(ndx, value);
}

BinaryData Table::GetBinary(size_t column_id, size_t ndx) const {
	assert(column_id < m_columns.Size());
	assert(ndx < m_size);

	const ColumnBinary& column = GetColumnBinary(column_id);
	return column.Get(ndx);
}

void Table::SetBinary(size_t column_id, size_t ndx, const void* value, size_t len) {
	assert(column_id < m_cols.Size());
	assert(ndx < m_size);

	ColumnBinary& column = GetColumnBinary(column_id);
	column.Set(ndx, value, len);
}

void Table::InsertBinary(size_t column_id, size_t ndx, const void* value, size_t len) {
	assert(column_id < m_cols.Size());
	assert(ndx <= m_size);

	ColumnBinary& column = GetColumnBinary(column_id);
	column.Insert(ndx, value, len);
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

bool Table::Compare(const Table& c) const {
    if (!m_spec.Compare(c.m_spec)) return false;
    if (!m_columnNames.Compare(c.m_columnNames)) return false;

    const size_t column_count = GetColumnCount();
    if (column_count != c.GetColumnCount()) return false;

    for (size_t i = 0; i < column_count; ++i) {
		const ColumnType type = GetColumnType(i);

        switch (type) {
            case COLUMN_TYPE_INT:
            case COLUMN_TYPE_BOOL:
                {
                    const Column& column1 = GetColumn(i);
                    const Column& column2 = c.GetColumn(i);
                    if (!column1.Compare(column2)) return false;
                }
                break;
            case COLUMN_TYPE_STRING:
                {
                    const AdaptiveStringColumn& column1 = GetColumnString(i);
                    const AdaptiveStringColumn& column2 = c.GetColumnString(i);
                    if (!column1.Compare(column2)) return false;
                }
                break;
            default:
                assert(false);
		}
    }
    return true;
}

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
	FILE* f = fopen(filename, "w");
	if (!f) return;

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
	printf("Table: len(%zu)\n    ", m_size);
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
					printf("%10lld ", column.Get(i));
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