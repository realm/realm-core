#include <stddef.h>

/*
Main method: import_csv(). Arguments:
---------------------------------------------------------------------------------------------------------------------
null_to_0:	
	imports value rows as TightDB value types (Integer, Float or Double) even though they contain empty
	strings (null / ""). Else they are converted to String

type_detection_rows:
	tells how many rows to read before analyzing data types (to see if numeric rows are really
	numeric everywhere, and not strings that happen to just mostly contain numeric characters


This library supports: 
---------------------------------------------------------------------------------------------------------------------
	* Auto detection of header and naming of TightDB columns accordingly
	* Newline inside data fields, plus autodetection of non-conforming non-quoted newlines (as in some IBM sample files)
	* double-quoted and non-quoted fields, and these can be mixed arbitrarely
	* double-quotes inside data field
	* *nix + MacOSv9 + Windows line feed
	* TightDB types String, Integer, Bool, Float and Double
	* Bool types can be case insensitive "true, false, 0, 1, yes, no"
	* Auto detection of float vs. double, depending on number of significant digits
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

class Importer 
{
public:
	size_t import_csv(const char* file, tightdb::Table& table, size_t import_rows = size_t(-1), bool null_to_0 = true, size_t type_detection_rows = 1000);

private:
	template <bool can_fail> float parse_float(const char*col, bool* success = NULL);
	template <bool can_fail> double parse_double(const char* col, bool* success = NULL, size_t* significants = NULL);
    template <bool can_fail> int64_t parse_integer(const char* col, bool* success = NULL);
	template <bool can_fail> bool parse_bool(const char*col, bool* success = NULL);
	vector<tightdb::DataType> types (vector<string> v);
	size_t import(const char* csv_file, vector<vector<string> > & payload, size_t records);
	vector<tightdb::DataType> detect_scheme (vector<vector<string> > payload, size_t begin, size_t end);
	vector<tightdb::DataType> lowest_common (vector<tightdb::DataType> types1, vector<tightdb::DataType> types2); 
	char src[2*chunk_size];
	size_t top;
	size_t s;
	size_t d;
    size_t field;
    FILE* f;
	bool m_null_to_0;
	size_t m_fields;

	char m_bool_true;
};


