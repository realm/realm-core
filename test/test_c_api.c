#define _CRT_SECURE_NO_WARNINGS

#define TEST_PASSED 0
#define TEST_FAILED -1

#if 0

#include <string.h>
#include <cstdio>
#include <tightdb/c-tightdb.h>


#define TEST_DATA_ROWS 256     // Rows of test data
#define TEST_DATA_COLUMNS 3    // Amount of different column types

#define INT_COL 0
#define BOOL_COL 1
#define STRING_COL 2


/****************************/
// TODO Test all methods...

// get/set for bool, date, binary, mixed (table)

/***********************************************************/

// Test data
tdb_type_int int_data[TEST_DATA_ROWS];
tdb_type_bool bool_data[TEST_DATA_ROWS];
char str[20];


TIGHTDB_TABLE_2(mytable,
                MyInt, Int,
                MyStr, String)

int c_test_1() {
    bool passed = true;
    Table* tbl = mytable_new();

    for (size_t i = 0; i < TEST_DATA_ROWS; i++) {
        int_data[i] = i - (TEST_DATA_ROWS / 2);
        bool_data[i] = ((i % 2) == 1);
        sprintf(str, "hello string %d", i);

        mytable_add(tbl, int_data[i], str);
    }
    mytable_insert(tbl, 7, 11111, "hej");
    if (mytable_get_MyInt(tbl, 7) != 11111)
        passed = false;
    mytable_insert(tbl, 55, 123456789, "123456789");
    if (strcmp(mytable_get_MyStr(tbl, 55), "123456789") != 0)
        passed = false;

    table_delete_row(tbl, 55);
    table_delete_row(tbl, 7);
    if (table_get_size(tbl) != TEST_DATA_ROWS)
        passed = false;

    for (int64_t i = 0; i < TEST_DATA_ROWS; i++) {
        if (mytable_get_MyInt(tbl, i) != i - (TEST_DATA_ROWS / 2))
            passed = false;
        sprintf(str, "hello string %d", i);
        if (strcmp(mytable_get_MyStr(tbl, i), str) != 0)
            passed =false;
    }

    table_delete(tbl);

    printf("Tested %d rows\n", TEST_DATA_ROWS);
    return passed ? TEST_PASSED : TEST_FAILED;
}


int c_test_2()
{
    // Test data
    int int_data[TEST_DATA_ROWS];
    bool bool_data[TEST_DATA_ROWS];
    for (size_t i = 0; i < TEST_DATA_ROWS; i++) {
        int_data[i] = i - (TEST_DATA_ROWS / 2);
        bool_data[i] = ((i % 2) == 1);
    }
    const char string_data[] = "This is a test. Testing. 1, 2, 3 testing";

    size_t col_ids[TEST_DATA_COLUMNS];
    const char* col_names[] = {"Int_col", "Bool_col", "String_col"};

    Table* t = table_new();
    col_ids[INT_COL] = table_register_column(t, COLUMN_TYPE_INT, col_names[INT_COL]);
    col_ids[BOOL_COL] = table_register_column(t, COLUMN_TYPE_BOOL, col_names[BOOL_COL]);
    col_ids[STRING_COL] = table_register_column(t, COLUMN_TYPE_STRING, col_names[STRING_COL]);

    for (size_t i = 0; i < TEST_DATA_COLUMNS; i++) {
        if (col_ids[i] != table_get_column_index(t, col_names[i]))
            return TEST_FAILED;
    }

    for (size_t row = 0; row < TEST_DATA_ROWS; row++){
        table_insert_int(t, col_ids[INT_COL], row, int_data[row]);
        table_insert_bool(t, col_ids[BOOL_COL], row, bool_data[row]);
        table_insert_string(t, col_ids[STRING_COL], row, string_data);
        table_insert_done(t);
    }

    for (size_t row = 0; row < TEST_DATA_ROWS; row++){
        if (int_data[row] != table_get_int(t, col_ids[INT_COL], row))
           return TEST_FAILED;
        if (bool_data[row] != table_get_bool(t, col_ids[BOOL_COL], row))
           return TEST_FAILED;
        if (strcmp(string_data, table_get_string(t, col_ids[STRING_COL], row)) != 0)
           return TEST_FAILED;
    }

    return TEST_PASSED;
}
#endif
int test_c_api(void)
{
#if 0
    if (c_test_1() == TEST_FAILED)
        return TEST_FAILED;

    if (c_test_2() == TEST_FAILED)
        return TEST_FAILED;
#endif
    return TEST_PASSED;
}

