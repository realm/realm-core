#define NOMINMAX

#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/importer.hpp>
#include <stdarg.h>

using namespace std;
using namespace UnitTest;
using namespace tightdb;


FILE* out_file;
FILE* in_file;

size_t type_detection_rows_flag = 10000;
bool null_to_0_flag = true;
size_t import_rows_flag = static_cast<size_t>(-1);
size_t skip_rows_flag = 0;
char separator_flag = ',';

void abort(bool b, const char* fmt, ...)
{
	if(b) {
		va_list argv;
		va_start(argv, fmt);
		fprintf(stderr, "csv: ");
		vfprintf(stderr, fmt, argv);
		va_end(argv);
		fprintf(stderr, "\n");
		exit(1);
	}
}

FILE* open_files(char* in)
{
	if(strcmp(in, "-stdin") == 0)
		return stdin;
	else {
		FILE* f = fopen(in, "rb");
		abort(f == NULL, "Error opening input file '%s' for reading", in);
		return f;
	}
}

int main(int argc, char* argv[])
{
	
#if 1
	char* hey[] = {"self", "d:/csv/managers.csv", "d:/csv/__test.tightdb"};

	argc = sizeof(hey) / sizeof(hey[0]);
	argv = hey;
#endif


	Importer importer;


	if(argc >= 5 && strlen(argv[1]) >= 3 && strncmp(argv[1], "-s", 2) == 0) {
		// Manual specification of scheme

		vector<DataType> scheme;
		vector<string> column_names;
		char* types = &argv[1][2];
		size_t columns = strlen(argv[1]);

		abort(argc < columns + 4, "");

		for(size_t n = 0; n < columns; n++) {
			if(types[n] == 's')
				scheme.push_back(type_String);
			else if(types[n] == 'd')
				scheme.push_back(type_Double);
			else if(types[n] == 'f')
				scheme.push_back(type_Float);
			else if(types[n] == 'i')
				scheme.push_back(type_Int);
			else if(types[n] == 'b')
				scheme.push_back(type_Bool);
			else
				abort();

			abort(argv[n + 2][0] == '-', "Not enough column names");
			column_names.push_back(argv[n + 2]);
		}

		in_file = open_files(argv[argc - 2]);

		Group group(argv[argc - 1]);
		TableRef table2 = group.get_table("table");
		Table &table = *table2;

		importer.import_csv(in_file, table, scheme, column_names, skip_rows_flag, import_rows_flag);
		group.commit();


	}
	else if (argc >= 3) {
		// Auto detection
		for(size_t a = 1; a < argc - 2; a++) {
			if(strncmp(argv[a], "-a=", 3) == 0)
				type_detection_rows_flag = atoi(&argv[a][3]);
			else if(strncmp(argv[a], "-n=", 3) == 0)
				import_rows_flag = atoi(&argv[a][3]);
			else if(strcmp(argv[a], "-e") == 0)
				null_to_0_flag = false;
			else
				abort(true, "");
		}

		in_file = open_files(argv[argc - 2]);
		
		Group group(argv[argc - 1]);
		TableRef table2 = group.get_table("table");
		Table &table = *table2;

		try {
			importer.import_csv(in_file, table, type_detection_rows_flag, null_to_0_flag, import_rows_flag);
		}
		catch (const runtime_error& error) {
			cerr << error.what();
		}
		group.commit();
	}


	return 0;



//size_t Importer::import_csv(const char* file, tightdb::Table& table, size_t type_detection_rows, bool null_to_0, size_t import_rows, char separator)
//size_t Importer::import_csv(const char* file, tightdb::Table& table, vector<tightdb::DataType> scheme, vector<string> column_names, size_t skip_first_rows, size_t import_rows, char separator)

	cerr <<     "Simple auto-import:\n"
		            "  csv <input> <output>\n"
					"\n"
					"Advanced auto-detection of scheme:\n"
					"  csv [-a=N] [-n=N] [-e] [-s=x] <input> <output>\n"
					"\n"
					" -a: Use the first N rows to auto-detect scheme (default -a=10000).\n"
					" -e: Because TightDB does not support NULL values, empty fields in the .csv file\n"
					"     may be converted to 0, 0.0 or false. Set the -e flag to avoid this. In this\n"
					"     case, a column with occurences of empty strings will be imported as string\n"
					"     type column.\n"
					" -n: Import only the first N rows of data payload\n"
					"\n"
					"Manual specification of scheme:\n"
					"  csv -s={s|i|b|f|d}{s|i|b|f|d}... name1 name2 ... [-s] [-n] <input> <output>\n"
					"\n"
					"  column types: s=string, i=integer, b=bool, f=float, d=double\n"
					" -s: skip first N rows (can be used to skip headers)\n"
					" -n: only import first N rows of payload\n"
					"\n"
					"Examples:\n"
					"  csv file.csv file.tightdb\n"
					"  csv -a=20000 file.csv file.tightdb\n"
					"  csv -s=ssdbi Name Email Height Gender Age file.csv file.tightdb\n"

//					"================================================================================" 80 chars of =
					
					;


	cerr << "csv <input .csv file> <output .tightdb file> ";



//	csv hello.csv db.tdb
//	




	/*

    Importer importer;
//	tightdb::Table table;





	argc = 2;
	argv[1] = "c:/d/csv/perf.csv";
	

	if(argc == 1) {
		printf("\nError: enter .csv file name as argument 1. For example 500.csv located somewhere in this branch :)\n");
		return 0;
	}

	size_t n = importer.import_csv(NULL, table, -1, true, 10000, ',');

	group.commit();

	//group.write("c:/d/wow");

	return 0;



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
//	for(size_t r = 0; r < table.size(); r++) {
	for(size_t r = 0; r < 100; r++) {
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
   
	//group.commit();
	//group.write("c:/d/new");


	*/

	

}
