#define NOMINMAX

#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>

#include <tightdb/importer.hpp>

using namespace std;
using namespace UnitTest;
using namespace tightdb;


int main(int argc, char* argv[])
{
    Importer importer;
	tightdb::Table table;

	/*
	argc = 2;
	argv[1] = "d:/csv/star0000-1.csv";
	*/

	if(argc == 1) {
		printf("\nError: enter .csv file name as argument 1. For example 500.csv located somewhere in this branch :)\n");
		return 0;
	}

	size_t n = importer.import_csv(argv[1], table, -1, true, 10000, ',');

	if(n == -1) {
		printf("File does not exist\n");
		return 0;
	}

	if(n == -2) {
		return 0;
	}

	// Print column names
	printf("\n");
	for(size_t t = 0; t < table.get_column_count(); t++) {
		printf("%s    ", table.get_column_name(t).data());
	}
	printf("\n\n");

	// Print scheme
	for(size_t c = 0; c < table.get_column_count(); c++) {
		if(table.get_column_type(c) == tightdb::type_Bool)
			printf("Bool    ");
		if(table.get_column_type(c) == tightdb::type_Double)
			printf("Double    ");
		if(table.get_column_type(c) == tightdb::type_Float)
			printf("Float    ");
		if(table.get_column_type(c) == tightdb::type_Int)
			printf("Integer    ");
		if(table.get_column_type(c) == tightdb::type_String)
			printf("String    ");
	}
	printf("\n\n");

	// Print payload
	for(size_t r = 0; r < table.size(); r++) {
		for(size_t c = 0; c < table.get_column_count(); c++) {
			if(table.get_column_type(c) == tightdb::type_Bool)
				printf("%d    ", table.get_bool(c, r));
			if(table.get_column_type(c) == tightdb::type_Double)
				printf("%f    ", table.get_double(c, r));
			if(table.get_column_type(c) == tightdb::type_Float)
				printf("%f    ", table.get_float(c, r));
			if(table.get_column_type(c) == tightdb::type_Int)
				printf("%d    ", table.get_int(c, r));
			if(table.get_column_type(c) == tightdb::type_String)
				printf("%s    ", table.get_string(c, r).data());
		}
		printf("\n");
		
	}


}
