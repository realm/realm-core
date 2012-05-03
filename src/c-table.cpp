#include "c-table.h"

#include "table.hpp"
#include "group.hpp"
#include "query.hpp"
#include "date.hpp"
#include <cstdarg>
#include <assert.h>

using tightdb::Date;

/*
C1X will be getting support for type generic expressions they look like this:
#define cbrt(X) _Generic((X), long double: cbrtl, \
                              default: cbrt, \
                              float: cbrtf)(X)
*/

// Internal helper functions to gain access to protected/private methods in Table:
namespace tightdb {
void TableHelper_unbind(Table* t) {
   t->unbind_ref();
}
Table* TableHelper_get_subtable_ptr(Table* t, std::size_t col_idx, std::size_t row_idx) {
    return t->get_subtable_ptr(col_idx, row_idx);
}
const Table* TableHelper_get_const_subtable_ptr(const Table* t, std::size_t col_idx, std::size_t row_idx) {
    return t->get_subtable_ptr(col_idx, row_idx);
}

}

extern "C" {

/*** Mixed ************************************/

Mixed *mixed_new_bool(bool value) {
    return new Mixed(value);
}
Mixed *mixed_new_date(time_t value) {
    return new Mixed(Date(value));
}
Mixed *mixed_new_int(int64_t value) {
    return new Mixed(value);
}
Mixed *mixed_new_string(const char* value) {
    return new Mixed(value);
}
Mixed *mixed_new_binary(const char* value, size_t len) {
    return new Mixed((const char*)value, len);
}
Mixed *mixed_new_table(void) {
    return new Mixed(COLUMN_TYPE_TABLE);
}
void mixed_delete(Mixed *mixed) {
    delete mixed;
}

int64_t mixed_get_int(Mixed *mixed) {
    return mixed->get_int(); 
}
bool mixed_get_bool(Mixed *mixed) {
    return mixed->get_bool(); 
}
time_t mixed_get_date(Mixed *mixed) {
    return mixed->get_date(); 
}
const char* mixed_get_string(Mixed *mixed) {
    return mixed->get_string(); 
}
BinaryData mixed_get_binary(Mixed *mixed) {
    return mixed->get_binary();
}


/*** Spec ************************************/

void spec_delete(Spec* spec) {
    delete spec;
}

void spec_add_column(Spec* spec, ColumnType type, const char* name) {
    spec->add_column(type, name);
}

Spec* spec_add_column_table(Spec* spec, const char* name) {
    return new Spec(spec->add_subtable_column(name));
}

Spec* spec_get_spec(Spec* spec, size_t column_id) {
    return new Spec(spec->get_subspec(column_id));
}

size_t spec_get_column_count(Spec* spec) {
    return spec->get_column_count();
}

ColumnType spec_get_column_type(Spec* spec, size_t column_id) {
    return spec->get_column_type(column_id);
}

const char* spec_get_column_name(Spec* spec, size_t column_id) {
    return spec->get_column_name(column_id);
}

size_t spec_get_column_index(Spec* spec, const char* name) {
    return spec->get_column_index(name);
}

// ??? get_subspec_ref ??

/*** Table ************************************/


// Pre-declare local functions
void table_insert_impl(Table* t, size_t ndx, va_list ap);
	
Table* table_new() {
	return new Table();
}

void table_delete(Table* t) {
	delete t;
}

void table_unbind(Table* t) {
    TableHelper_unbind(t);
}

Spec* table_get_spec(Table* t) {
    return new Spec(t->get_spec());
}

void table_update_from_spec(Table* t, size_t ref_specSet) {
    t->update_from_spec();
}

size_t table_register_column(Table* t, ColumnType type, const char* name) {
	return t->register_column(type, name);
}

size_t table_get_column_count(const Table* t) {
	return t->get_column_count();
}

const char* table_get_column_name(const Table* t, size_t ndx) {
	return t->get_column_name(ndx);
}

size_t table_get_column_index(const Table* t, const char* name) {
	return t->get_column_index(name);
}

ColumnType table_get_column_type(const Table* t, size_t ndx) {
	return t->get_column_type(ndx);
}

bool table_is_empty(const Table* t) {
	return t->is_empty();
}

size_t table_get_size(const Table* t) {
	return t->size();
}

void table_clear(Table* t) {
	t->clear();
}

void table_optimize(Table* t) {
    t->optimize();
}

void table_delete_row(Table* t, size_t ndx) {
	t->remove(ndx);
}

void table_pop_back(Table* t) {
    t->pop_back();
}


/*** Getters *******/


int64_t table_get_int(const Table* t, size_t column_id, size_t ndx) {
	return t->Get(column_id, ndx);
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

BinaryData table_get_binary(const Table* t, size_t column_id, size_t ndx) {
	return t->GetBinary(column_id, ndx);
}

Mixed* table_get_mixed(const Table* t, size_t column_id, size_t ndx) {
	return new Mixed(t->GetMixed(column_id, ndx));
}

ColumnType table_get_mixed_type(const Table* t, size_t column_id, size_t ndx) {
    return t->GetMixedType(column_id, ndx);
}

Table* table_get_table(Table* t, size_t column_id, size_t ndx) {
    return TableHelper_get_subtable_ptr(t, column_id, ndx); 
}

const Table* table_get_ctable(const Table* t, size_t column_id, size_t ndx) {
    return TableHelper_get_const_subtable_ptr(t, column_id, ndx); 
}

/*** Setters *******/


void table_set_int(Table* t, size_t column_id, size_t ndx, int64_t value) {
	t->Set(column_id, ndx, value);
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

void table_set_binary(Table* t, size_t column_id, size_t ndx, const char *value, size_t len) {
	t->SetBinary(column_id, ndx, value, len);
}

void table_set_mixed(Table* t, size_t column_id, size_t ndx, Mixed value) {
	t->SetMixed(column_id, ndx, value);
}

void table_clear_table(Table* t, size_t column_id, size_t ndx) {
    t->ClearTable(column_id, ndx);
}


void table_insert_impl(Table* t, size_t ndx, va_list ap) {
	assert(ndx <= t->size());

	const size_t count = t->get_column_count();
	for (size_t i = 0; i < count; ++i) {
		const ColumnType type = t->get_column_type(i);
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
				t->InsertBool(i, ndx, v != 0);
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
		case COLUMN_TYPE_MIXED:
			{
				Mixed* const v = va_arg(ap, Mixed*);
				t->InsertMixed(i, ndx, v);
			}
			break;
		case COLUMN_TYPE_BINARY:
			{
				const char* ptr = va_arg(ap, const char*);
                size_t      len = va_arg(ap, size_t);
				t->InsertBinary(i, ndx, ptr, len);
			}
			break;
        case COLUMN_TYPE_TABLE:
			{
				t->InsertTable(i, ndx);
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

	table_insert_impl(t, t->size(), ap);

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

void table_insert_binary(Table* t, size_t column_id, size_t ndx, const char* value, size_t len) {
	t->InsertBinary(column_id, ndx, value, len);
}

void table_insert_mixed(Table* t, size_t column_id, size_t ndx, Mixed value) {
	t->InsertMixed(column_id, ndx, value);
}

void table_insert_table(Table* t, size_t column_id, size_t ndx) {
	t->InsertTable(column_id, ndx);
}

void table_insert_done(Table* t) {
	t->InsertDone();
}


/******* Index, Searching ******************************/


bool table_has_index(const Table* t, size_t column_id) {
	return t->has_index(column_id);
}

void table_set_index(Table* t, size_t column_id) {
	return t->set_index(column_id);
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


// *** TableView *********************************************************************


void tableview_delete(TableView* tv) {
	delete tv;
}

bool tableview_is_empty(const TableView* tv) {
	return tv->is_empty();
}

size_t tableview_get_size(const TableView* tv) {
	return tv->size();
}
/* ??? Implement
size_t tableview_get_table_size(const TableView* tv, size_t column_id, size_t ndx) {
    return tv->GetTableSize();
*/


int64_t tableview_get_int(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->Get(column_id, ndx);
}

bool tableview_get_bool(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->GetBool(column_id, ndx);
}

time_t tableview_get_date(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->GetDate(column_id, ndx);
}

const char* tableview_get_string(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->GetString(column_id, ndx);
}

/* ??? Waiting for implementation
BinaryData tableview_get_binary(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->GetBinary(column_id, ndx);
}

Mixed tableview_get_mixed(const TableView* tv, size_t column_id, size_t ndx) {
	return tv->GetMixed(column_id, ndx);
}
*/


void tableview_set_int(TableView* tv, size_t column_id, size_t ndx, int64_t value) {
	tv->Set(column_id, ndx, value);
}

void tableview_set_bool(TableView* tv, size_t column_id, size_t ndx, bool value) {
	tv->SetBool(column_id, ndx, value);
}

void tableview_set_date(TableView* tv, size_t column_id, size_t ndx, time_t value) {
	tv->SetDate(column_id, ndx, value);
}

void tableview_set_string(TableView* tv, size_t column_id, size_t ndx, const char* value) {
	tv->SetString(column_id, ndx, value);
}

/*
//??? Waiting for implementation
void tableview_set_binary(TableView* tv, size_t column_id, size_t ndx, const char* value, size_t len) {
	tv->SetBinary(column_id, ndx, value, len);
}

void tableview_set_mixed(TableView* tv, size_t column_id, size_t ndx, Mixed value) {
	tv->SetMixed(column_id, ndx, value);
}

void tableview_clear_table(TableView* tv, size_t column_id, size_t ndx) {
    tv->ClearTable(column_id, ndx);
}
*/

/* Search and sort */

size_t tableview_find(TableView* tv, size_t column_id, int64_t value) {
    return tv->Find(column_id, value);
}

size_t tableview_find_string(TableView* tv, size_t column_id, const char* value) {
    return tv->FindString(column_id, value);
}

#if 0
//??? Waiting for implementation
void tableview_find_all(TableView* tv, size_t column_id, int64_t value) {
    // ??? waiting for implementation: tv->FindAll(*tv, column_id, value);
    assert(0);
}

void tableview_find_all_string(TableView* tv, size_t column_id, const char *value) {
    tv->FindAllString(*tv, column_id, value);
}
#endif

/* Aggregation */
int64_t tableview_sum(TableView* tv, size_t column_id) {
    return tv->sum(column_id);
}

int64_t tableview_min(TableView* tv, size_t column_id) {
    return tv->Min(column_id);
}

int64_t tableview_max(TableView* tv, size_t column_id) {
    return tv->Max(column_id);
}

void tableview_sort(TableView* tv, size_t column_id, bool ascending) {
    tv->Sort(column_id, ascending);
}


/**** Group *********************************************************************/

Group* group_new(void) {
    return new Group();
}

Group* group_new_file(const char* filename) {
    return new Group(filename);
}

Group* group_new_mem(const char* buffer, size_t len) {
    return new Group(buffer, len);
}

void group_delete(Group* group) {
    delete group;
}

bool group_is_valid(Group* group) {
    return group->is_valid();
}

size_t group_get_table_count(Group* group) {
    return group->get_table_count();
}

const char* group_get_table_name(Group* group, size_t table_ndx) {
    return group->get_table_name(table_ndx);
}

bool group_has_table(Group* group, const char* name) {
    return group->has_table(name);
}

#if 0
///???
Table* group_get_table(Group* group, const char* name) {
    /*??? Waiting for removal of TopLevelTable*/
    /* return group->GetTable(name); */
}
#endif	

/* Serialization */
void group_write(Group* group, const char* filepath) {
    group->write(filepath);
}

char* group_write_to_mem(Group* group, size_t* len) {
    return group->write_to_mem(*len);
}



/**** Query *********************************************************************/


Query* query_new() {
    return new Query();
}

void query_delete(Query* q) {
    delete q;
}

void query_group(Query* q) {
    q->RightParan();
}

void query_end_group(Query* q) {
    q->LeftParan();
}
void query_or(Query* q) {
    q->Or();
}
#if 1
void query_subtable(Query* q, size_t column_id) {
    q->Subtable(column_id);
}
#endif
void query_parent(Query* q) {
    q->Parent();
}

Query* query_bool_equal(Query* q, size_t column_id, bool value) {
    return new Query(q->Equal(column_id, value));
}

Query* query_int_equal(Query* q, size_t column_id, int64_t value) {
    return new Query(q->Equal(column_id, value));    
}

/* Integers */

Query*  query_int_not_equal(Query* q, size_t column_id, int64_t value) {
    return new Query(q->NotEqual(column_id, value));
}
Query*  query_int_greater(Query* q, size_t column_id, int64_t value) {
    return new Query(q->Greater(column_id, value));
}
Query*  query_int_greater_or_equal(Query* q, size_t column_id, int64_t value) {
    return new Query(q->GreaterEqual(column_id, value));
}
Query*  query_int_less(Query* q, size_t column_id, int64_t value) {
    return new Query(q->Less(column_id, value));
}
Query*  query_int_less_or_equal(Query* q, size_t column_id, int64_t value) {
    return new Query(q->LessEqual(column_id, value));
}
Query*  query_int_between(Query* q, size_t column_id, int64_t from, int64_t to) {
    return new Query(q->Between(column_id, from , to));
}

/* Strings */ 

Query*  query_string_equal(Query* q, size_t column_id, const char* value, CaseSensitivity_t case_sensitive) {
    return new Query(q->Equal(column_id, value, (case_sensitive == CASE_SENSITIVE)));
}
Query*  query_string_not_equal(Query* q, size_t column_id, const char* value, CaseSensitivity_t case_sensitive) {
    return new Query(q->NotEqual(column_id, value, (case_sensitive == CASE_SENSITIVE)));
}
Query*  query_string_begins_with(Query* q, size_t column_id, const char* value, CaseSensitivity_t case_sensitive) {
    return new Query(q->BeginsWith(column_id, value, (case_sensitive == CASE_SENSITIVE)));
}
Query*  query_string_ends_with(Query* q, size_t column_id, const char* value, CaseSensitivity_t case_sensitive) {
    return new Query(q->EndsWith(column_id, value, (case_sensitive == CASE_SENSITIVE)));
}
Query*  query_string_contains(Query* q, size_t column_id, const char* value, CaseSensitivity_t case_sensitive) {
    return new Query(q->Contains(column_id, value, (case_sensitive == CASE_SENSITIVE)));
}


/* ??? Currently missing support for Query on Mixed and Binary */


TableView*  query_find_all(Query* q, Table* t) {
    TableView* tv = new TableView(*t);
    q->FindAll(*t, *tv, 0, size_t(-1), size_t(-1));
    return tv;
}

TableView*  query_find_all_range(Query* q, Table* t, size_t start, size_t end, size_t limit) {
    TableView* tv = new TableView(*t);
    q->FindAll(*t, *tv, start, end, limit);
    return tv;
}

/* Aggregations */

size_t query_count(Query* q, const Table* t) {
    return q->Count(*t, 0U, (size_t)-1, (size_t)-1);
}

size_t query_count_range(Query* q, const Table* t, size_t start, size_t end, size_t limit) {
    return q->Count(*t, start, end, limit);
}

int64_t query_min(Query* q, const Table* t, size_t column_id, size_t* resultcount) {
    return q->Min(*t, column_id, resultcount, 0, size_t(-1), size_t(-1));
}

int64_t query_min_range(Query* q, const Table* t, size_t column_id, size_t* resultcount,
                         size_t start, size_t end, size_t limit) {
    return q->Min(*t, column_id, resultcount, start, end, limit);
}

int64_t  query_max(Query* q, const Table* t, size_t column_id, size_t* resultcount) {
    return q->Max(*t, column_id, resultcount, 0, size_t(-1), size_t(-1));
}

int64_t  query_max_range(Query* q, const Table* t, size_t column_id, size_t* resultcount,
                         size_t start, size_t end, size_t limit){
    return q->Max(*t, column_id, resultcount, start, end, limit);
}

int64_t  query_sum(Query* q, const Table* t, size_t column_id, size_t* resultcount) {
    return q->sum(*t, column_id, resultcount, 0, size_t(-1), size_t(-1));
}

int64_t  query_sum_range(Query* q, const Table* t, size_t column_id, size_t* resultcount,
                         size_t start, size_t end, size_t limit){
    return q->sum(*t, column_id, resultcount, start, end, limit);
}

double  query_avg(Query* q, const Table* t, size_t column_id, size_t* resultcount) {
    return q->Avg(*t, column_id, resultcount, 0, size_t(-1), size_t(-1));
}

double  query_avg_range(Query* q, const Table* t, size_t column_id, size_t* resultcount,
                         size_t start, size_t end, size_t limit) {
    return q->Avg(*t, column_id, resultcount, start, end, limit);
}


} //extern "C"
