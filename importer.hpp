#include <stdint.h>


static const size_t chunk_size = 16*1024; 
static const size_t record_chunks = 100;


#include <vector>
#include <tightdb.hpp>

using namespace std;



class Importer 
{

public:
	void import_csv(const char* file, tightdb::Table& table, bool null_to_0 = true, size_t type_detection_rows = 1000);


private:
	template <bool can_fail> float parse_float(const char*col, bool* success = NULL);
	template <bool can_fail> double parse_double(const char* col, bool* success = NULL, size_t* significants = NULL);
    template <bool can_fail> int64_t parse_integer(const char* col, bool* success = NULL);
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

};


