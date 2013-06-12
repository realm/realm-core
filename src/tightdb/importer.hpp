#include <stddef.h>

/*
Main method: import_csv(). Arguments:
---------------------------------------------------------------------------------------------------------------------
empty_as_string_flag:	
	Imports a column that has occurences of empty strings as String type column. Else fields arec onverted to false/0/0.0

type_detection_rows:
	tells how many rows to read before analyzing data types (to see if numeric rows are really
	numeric everywhere, and not strings that happen to just mostly contain numeric characters


This library supports: 
---------------------------------------------------------------------------------------------------------------------
	* Auto detection of float vs. double, depending on number of significant digits
	* Bool types can be case insensitive "true, false, 0, 1, yes, no"
	* Newline inside data fields, plus autodetection of non-conforming non-quoted newlines (as in some IBM sample files)
	* TightDB types String, Integer, Bool, Float and Double
	* Auto detection of header and naming of TightDB columns accordingly
	* double-quoted and non-quoted fields, and these can be mixed arbitrarely
	* double-quotes inside data field
	* *nix + MacOSv9 + Windows line feed
	* Comma and dot as radix point
	* Scientific notation of floats/doubles (+1.23e-10)
	* FAST FAST FAST (200 MB/s). Uses state-machine instead of traditional char-by-char loop with state checks inside


Problems:
---------------------------------------------------------------------------------------------------------------------
	A csv file does not tell its sheme. So we auto-detect it, based on the first N rows. However if a given column 
	contains 'false, false, false, hello' and we detect and create TightDB table scheme using the first 3 rows, we fail
	when we meet 'hello' (this error is handled with a thorough error message)


*/

static const size_t chunk_size = 16*1024; 
static const size_t record_chunks = 100;


#include <vector>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

class Importer 
{
public:
	Importer();
	size_t import_csv_auto(FILE* file, Table& table,     size_t type_detection_rows = 1000, bool empty_as_string_flag = false,               size_t import_rows = static_cast<size_t>(-1));
	size_t import_csv_manual(FILE* file, Table& table,   vector<DataType> scheme, vector<string> column_names, size_t skip_first_rows = 0,   size_t import_rows = static_cast<size_t>(-1));
	bool Quiet;
	char Separator;

private:
	size_t import_csv(FILE* file, Table& table, vector<DataType> *scheme, vector<string> *column_names, size_t type_detection_rows, bool empty_as_string_flag, size_t skip_first_rows, size_t import_rows);
	template <bool can_fail> float parse_float(const char*col, bool* success = NULL);
	template <bool can_fail> double parse_double(const char* col, bool* success = NULL, size_t* significants = NULL);
    template <bool can_fail> int64_t parse_integer(const char* col, bool* success = NULL);
	template <bool can_fail> bool parse_bool(const char*col, bool* success = NULL);
	vector<DataType> types (vector<string> v);
	size_t import(vector<vector<string> > & payload, size_t records);
	vector<DataType> detect_scheme (vector<vector<string> > payload, size_t begin, size_t end);
	vector<DataType> lowest_common (vector<DataType> types1, vector<DataType> types2); 
	char src[2*chunk_size];
	size_t top;
	size_t s;
	size_t d;
    size_t field;
    FILE* f;
	bool m_empty_as_string_flag;
	size_t m_fields;
	char m_bool_true;

};


