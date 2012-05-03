#ifndef __C_TIGHTDB_H__
#define __C_TIGHTDB_H__

#include "c-table.h"
#include "query.hpp"


#define TIGHTDB_TABLE_1(TableName, CType0, CName0) \
\
Table* TableName##_new(void) { \
    Table *tbl = table_new(); \
    Spec* spec = table_get_spec(tbl); \
	spec_add_column(spec, COLUMN_TYPE_##CType0, #CName0); \
    table_update_from_spec(tbl, spec_get_ref(spec)); \
    spec_delete(spec); \
    return tbl; \
} \
\
void TableName##_add(Table* tbl, tdb_type_##CType0 value0) { \
	table_add(tbl, value0); \
} \
\
void TableName##_insert(Table* tbl, size_t row_ndx, tdb_type_##CType0 value0) { \
	table_insert(tbl, row_ndx, value0); \
} \
\
tdb_type_##CType0 TableName##_get_##CName0(Table* tbl, size_t row_ndx) { \
	return table_get_##CType0(tbl, 0, row_ndx); \
} \
void TableName##_set_##CName0(Table* tbl, size_t row_ndx, tdb_type_##CType0 value) { \
	return table_set_##CType0(tbl, 0, row_ndx, value); \
} \



#define TIGHTDB_TABLE_2(TableName, CType0, CName0, CType1, CName1) \
\
Table* TableName##_new(void) { \
    Table *tbl = table_new(); \
    Spec* spec = table_get_spec(tbl); \
	spec_add_column(spec, COLUMN_TYPE_##CType0, #CName0); \
	spec_add_column(spec, COLUMN_TYPE_##CType1, #CName1); \
    table_update_from_spec(tbl, spec_get_ref(spec)); \
    spec_delete(spec); \
    return tbl; \
} \
\
void TableName##_add(Table* tbl, tdb_type_##CType0 value0, tdb_type_##CType1 value1) { \
	table_add(tbl, value0, value1); \
} \
\
void TableName##_insert(Table* tbl, size_t row_ndx, tdb_type_##CType0 value0, tdb_type_##CType1 value1) { \
	table_insert(tbl, row_ndx, value0, value1); \
} \
\
tdb_type_##CType0 TableName##_get_##CName0(Table* tbl, size_t row_ndx) { \
	return table_get_##CType0(tbl, 0, row_ndx); \
} \
void TableName##_set_##CName0(Table* tbl, size_t row_ndx, tdb_type_##CType0 value) { \
	return table_set_##CType0(tbl, 0, row_ndx, value); \
} \
tdb_type_##CType1 TableName##_get_##CName1(Table* tbl, size_t row_ndx) { \
	return table_get_##CType1(tbl, 1, row_ndx); \
} \
void TableName##_set_##CName1(Table* tbl, size_t row_ndx, tdb_type_##CType1 value) { \
	return table_set_##CType1(tbl, 1, row_ndx, value); \
} \



#endif //__C_TIGHTDB_H__
