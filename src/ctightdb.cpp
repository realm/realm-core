#include "ctightdb.h"
#include "Table.h"
#include <cstdarg>
#include <assert.h>

extern "C" {

// Pre-declare local functions
void table_insert_impl(Table* t, size_t ndx, va_list ap);
	
Table* table_new() {
	return new Table();
}

void table_delete(Table* t) {
	delete t;
}

size_t table_register_column(Table* t, ColumnType type, const char* name) {
	return t->RegisterColumn(type, name);
}

size_t table_get_column_count(const Table* t) {
	return t->GetColumnCount();
}

const char* table_get_column_name(const Table* t, size_t ndx) {
	return t->GetColumnName(ndx);
}

size_t table_get_column_index(const Table* t, const char* name) {
	return t->GetColumnIndex(name);
}

ColumnType table_get_column_type(const Table* t, size_t ndx) {
	return t->GetColumnType(ndx);
}

bool table_is_empty(const Table* t) {
	return t->IsEmpty();
}

size_t table_get_size(const Table* t) {
	return t->GetSize();
}

void table_clear(Table* t) {
	t->Clear();
}

void table_delete_row(Table* t, size_t ndx) {
	t->DeleteRow(ndx);
}

int table_get_int(const Table* t, size_t column_id, size_t ndx) {
	return t->Get(column_id, ndx);
}

int64_t table_get_int64(const Table* t, size_t column_id, size_t ndx) {
	return t->Get64(column_id, ndx);
}

bool table_get_bool(const Table* t, size_t column_id, size_t ndx) {
	return t->GetBool(column_id, ndx);
}

time_t table_get_date(const Table* t, size_t column_id, size_t ndx) {
	return t->GetDate(column_id, ndx);
}

const char* table_get_string(const Table* t, size_t column_id, size_t ndx) {
	return t->GetString(column_id, ndx);
}

void table_set_int(Table* t, size_t column_id, size_t ndx, int value) {
	t->Set(column_id, ndx, value);
}

void table_set_int64(Table* t, size_t column_id, size_t ndx, int64_t value) {
	t->Set64(column_id, ndx, value);
}

void table_set_bool(Table* t, size_t column_id, size_t ndx, bool value) {
	t->SetBool(column_id, ndx, value);
}

void table_set_date(Table* t, size_t column_id, size_t ndx, time_t value) {
	t->SetDate(column_id, ndx, value);
}

void table_set_string(Table* t, size_t column_id, size_t ndx, const char* value) {
	t->SetString(column_id, ndx, value);
}

void table_insert_impl(Table* t, size_t ndx, va_list ap) {
	assert(ndx <= t->GetSize());

	const size_t count = t->GetColumnCount();
	for (size_t i = 0; i < count; ++i) {
		const ColumnType type = t->GetColumnType(i);
		switch (type) {
		case COLUMN_TYPE_INT:
			{
				// int values should always be cast to 64bit in args
				const int64_t v = va_arg(ap, int64_t);
				t->InsertInt(i, ndx, v);
			}
			break;
		case COLUMN_TYPE_BOOL:
			{
				const int v = va_arg(ap, int);
				t->InsertBool(i, ndx, v);
			}
			break;
		case COLUMN_TYPE_DATE:
			{
				const time_t v = va_arg(ap, time_t);
				t->InsertDate(i, ndx, v);
			}
			break;
		case COLUMN_TYPE_STRING:
			{
				const char* v = va_arg(ap, const char*);
				t->InsertString(i, ndx, v);
			}
			break;
		default:
			assert(false);
		}
	}

	t->InsertDone();
}

void table_add(Table* t,  ...) {
	// initialize varable length arg list
	va_list ap;
	va_start(ap, t);

	const size_t ndx = t->GetSize();
	table_insert_impl(t, ndx, ap);

	va_end(ap);
}

void table_insert(Table* t, size_t ndx, ...) {
	// initialize varable length arg list
	va_list ap;
	va_start(ap, ndx);

	table_insert_impl(t, ndx, ap);

	va_end(ap);
}

void table_insert_int(Table* t, size_t column_id, size_t ndx, int value) {
	t->InsertInt(column_id, ndx, value);
}

void table_insert_int64(Table* t, size_t column_id, size_t ndx, int64_t value) {
	t->InsertInt(column_id, ndx, value);
}

void table_insert_bool(Table* t, size_t column_id, size_t ndx, bool value) {
	t->InsertBool(column_id, ndx, value);
}

void table_insert_date(Table* t, size_t column_id, size_t ndx, time_t value) {
	t->InsertDate(column_id, ndx, value);
}

void table_insert_string(Table* t, size_t column_id, size_t ndx, const char* value) {
	t->InsertString(column_id, ndx, value);
}

void table_insert_done(Table* t) {
	t->InsertDone();
}

bool table_has_index(const Table* t, size_t column_id) {
	return t->HasIndex(column_id);
}

void table_set_index(Table* t, size_t column_id) {
	return t->SetIndex(column_id);
}

size_t table_find_int(const Table* t, size_t column_id, int value) {
	return t->Find(column_id, value);
}

size_t table_find_int64(const Table* t, size_t column_id, int64_t value) {
	return t->Find(column_id, value);
}

size_t table_find_bool(const Table* t, size_t column_id, bool value) {
	return t->FindBool(column_id, value);
}

size_t table_find_date(const Table* t, size_t column_id, time_t value) {
	return t->FindDate(column_id, value);
}

size_t table_find_string(const Table* t, size_t column_id, const char* value) {
	return t->FindString(column_id, value);
}

TableView* table_find_all_int64(Table* t, size_t column_id, int64_t value) {
	TableView* tv = new TableView(*t);
	t->FindAll(*tv, column_id, value);
	return tv;
}

TableView* table_find_all_hamming(Table* t, size_t column_id, uint64_t value, size_t max) {
	TableView* tv = new TableView(*t);
	t->FindAllHamming(*tv, column_id, value, max);
	return tv;
}


// *** TableView ************************************

void tableview_delete(TableView* t) {
	delete t;
}

bool tableview_is_empty(const TableView* tv) {
	return tv->IsEmpty();
}

size_t tableview_get_size(const TableView* tv) {
	return tv->GetSize();
}

int tableview_get_int(const TableView* t, size_t column_id, size_t ndx) {
	return t->Get(column_id, ndx);
}

int64_t tableview_get_int64(const TableView* t, size_t column_id, size_t ndx) {
	return t->Get64(column_id, ndx);
}

bool tableview_get_bool(const TableView* t, size_t column_id, size_t ndx) {
	return t->GetBool(column_id, ndx);
}

time_t tableview_get_date(const TableView* t, size_t column_id, size_t ndx) {
	return t->GetDate(column_id, ndx);
}

const char* tableview_get_string(const TableView* t, size_t column_id, size_t ndx) {
	return t->GetString(column_id, ndx);
}

void tableview_set_int(TableView* t, size_t column_id, size_t ndx, int value) {
	t->Set(column_id, ndx, value);
}

void tableview_set_int64(TableView* t, size_t column_id, size_t ndx, int64_t value) {
	t->Set64(column_id, ndx, value);
}

void tableview_set_bool(TableView* t, size_t column_id, size_t ndx, bool value) {
	t->SetBool(column_id, ndx, value);
}

void tableview_set_date(TableView* t, size_t column_id, size_t ndx, time_t value) {
	t->SetDate(column_id, ndx, value);
}

void tableview_set_string(TableView* t, size_t column_id, size_t ndx, const char* value) {
	t->SetString(column_id, ndx, value);
}


} //extern "C"
