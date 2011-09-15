#include <string.h>

#include "ctightdb.h"

#define TEST_DATA_ROWS 256     // Rows of test data
#define TEST_DATA_COLUMNS 3    // Amount of different column types

#define INT_COL 0
#define BOOL_COL 1
#define STRING_COL 2

#define TEST_PASSED 0
#define TEST_FAILED -1

int main(int argc, const char *argv[]){
	// Test data
	int int_data[TEST_DATA_ROWS];
	bool bool_data[TEST_DATA_ROWS];
	for(size_t i = 0; i < TEST_DATA_ROWS; i++){
		int_data[i] = i - (TEST_DATA_ROWS / 2);
		bool_data[i] = i % 2;
	}
	const char string_data[] = "This is a test. Testing. 1, 2, 3 testing";

	size_t col_ids[TEST_DATA_COLUMNS];
	const char* col_names[] = {"Int_col", "Bool_col", "String_col"};

	Table* t = table_new("test");
	col_ids[INT_COL] = table_register_column(t, COLUMN_TYPE_INT, col_names[INT_COL]);
	col_ids[BOOL_COL] = table_register_column(t, COLUMN_TYPE_BOOL, col_names[BOOL_COL]);
	col_ids[STRING_COL] = table_register_column(t, COLUMN_TYPE_STRING, col_names[STRING_COL]);

	for(size_t i = 0; i < TEST_DATA_COLUMNS; i++) {
		if(col_ids[i] != table_get_column_index(t, col_names[i]))
			return TEST_FAILED;
	}

	for(size_t row = 0; row < TEST_DATA_ROWS; row++){
		table_insert_int(t, col_ids[INT_COL], row, int_data[row]);
		table_insert_bool(t, col_ids[BOOL_COL], row, bool_data[row]);
		table_insert_string(t, col_ids[STRING_COL], row, string_data);
		table_insert_done(t);
	}

	for(size_t row = 0; row < TEST_DATA_ROWS; row++){
		if(int_data[row] != table_get_int(t, col_ids[INT_COL], row))
		   return TEST_FAILED;
		if(bool_data[row] != table_get_bool(t, col_ids[BOOL_COL], row))
		   return TEST_FAILED;
		if(strcmp(string_data, table_get_string(t, col_ids[STRING_COL], row)) != 0)
		   return TEST_FAILED;
	}

	return TEST_PASSED;
}
