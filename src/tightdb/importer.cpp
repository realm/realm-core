#include "importer.hpp"
#include <stdint.h>
#include <limits>
#include <vector>
#include <assert.h>

using namespace std;




// Set can_fail = true to detect if a string is an integer. In this case, provide a success argument
// Set can_fail = false to do the conversion. In this case, omit success argument. 
template <bool can_fail> int64_t Importer::parse_integer(const char* col, bool* success)
{
	int64_t	x = 0;

	if(can_fail && !*col) {
		*success = false;
		return 0;
	}
	if (*col == '-'){
		++col;
		x = 0;
		while (*col != '\0') {
			if (can_fail && ('0' > *col || *col > '9')){
				*success = false;
				return 0;
			}
			int64_t y = *col - '0';
			if (can_fail && x < (std::numeric_limits<int64_t>::min()+y)/10){
				*success = false;
				return 0;
			}

			x = 10*x-y;
			++col;
		}
		if(can_fail)
			*success = true;
		return x;
	}
	else if (*col == '+')
		++col;

	while (*col != '\0'){
		if(can_fail && ('0' > *col || *col > '9')){
			*success = false;
			return 0;
		}
		int64_t y = *col - '0';
		x = 10*x+y;
		++col;
	}

	if(can_fail)
		*success = true;

	return x;
}


template <bool can_fail> bool Importer::parse_bool(const char*col, bool* success)
{
	// Must be tuples of {true value, false value}
	static const char* a[] = {"True", "False", "true", "false", "TRUE", "FALSE", "1", "0", "Yes", "No", "yes", "no", "YES", "NO"};

	char c = *col;	

	// Perform quick check in order to terminate fast if non-bool. Unfortunatly VC / gcc does NOT optimize a loop 
	// that iterates through a[n][0] to remove redundant letters, even though 'a' is static const. So we need to do 
	// it manually.
	if(can_fail) {
		if (col[0] == 0) {
			*success = true;
			return false;
		}

		char lower = c | 32;
		if(c != '1' && c != '0' && lower != 't' && lower != 'f' && lower != 'y' && lower != 'n') {
			*success = false;
			return false;
		}

		for(size_t t = 0; t < sizeof(a) / sizeof(a[0]); t++)
		{
			if(strcmp(col, a[t]) == 0)
			{
				*success = true;
//				m_bool_true = a[t >> 1 << 1][0];
				return !((t & 0x1) == 0);
			}
		}
		*success = false;
		return false;
	}
	
	char lower = c | 32;
	if(c == '1' || lower == 't' || lower == 'y')
		return true;
	else
		return false;

//  This is a super fast way that only requires 1 compare because we have already detected the true/false values
//  earlier, so we just need to compare with that. However, it's not complately safe, so we avoid it
//	return (c == m_bool_true);

}

// Set can_fail = true to detect if a string is a float. In this case, provide a 'success' argument. It will return false
// if the number of significant decimal digits in the input string is greater than 6, in which case you should use 
// doubles instead.
//
// Set can_fail = false to do the conversion. In this case, omit success argument. 
template <bool can_fail> float Importer::parse_float(const char*col, bool* success)
{
	bool s;
	size_t significants = 0;	
	double d = parse_double<can_fail>(col, &s, &significants);

	// Fail if plain text input has more than 6 significant digits (a float can have ~7.2 correct digits)
	if(can_fail && (!s || significants > 6)) {
		*success = false;
		return 0.0;	
	}

	if(can_fail && success != NULL)
		*success = true;

	return static_cast<float>(d);
}

// Set can_fail = true to detect if a string is a double. In this case, provide 'success' and 'significants' arguments.
// It will return the number of significant digits in the input string in 'significants', that is, 4 for 1.234 and
// 4 for -2.531e9 and 5 for 1.0000 (this is helper functionality for parse_float()).
//
// Set can_fail = false to do the conversion. In this case, omit success argument. 
template <bool can_fail> double Importer::parse_double(const char* col, bool* success, size_t* significants)
{
	const char* orig_col = col;
	double x;
	bool is_neg = false;
	size_t dummy;
	if(can_fail && significants == NULL)
		significants = &dummy;

	if(can_fail && *col == 0) {
		*success = true;
		return 0;
	}

	if (*col == '-') {
		is_neg = true;
		++col;
	}
	else if (*col == '+')
		++col;

	x = 0;
	while('0' <= *col && *col <= '9'){
		int y = *col - '0';
		x *= 10;
		x += y;
		++col;
		++*significants;
	}
			
	if(*col == '.'|| *col == ','){
		++col;
		double pos = 1;
		while ('0' <= *col && *col <= '9'){
			pos /= 10;
			int y = *col - '0';
			++col;
			x += y*pos;
			++*significants;
		}
	}

	if(*col == 'e' || *col == 'E'){
		if(can_fail && col == orig_col) {
			*success = false;
			return 0;
		}

		++col;
		int64_t e;
		e = parse_integer<false>(col);
				
		if (e != 0) {
			double base;	
			if (e < 0) {
				base = 0.1;
				e = -e;
			}
			else {
				base = 10;
			}
	
			while (e != 1) {
				if ((e & 1) == 0) {
					base = base*base;
					e >>= 1;
				}
				else {
					x *= base;
					--e;
				}
			}
			x *= base;
		}
	}
	else if (can_fail && *col != '\0') {
		*success = false;
		return 0;
	}
	if(is_neg)
		x = -x;

	if(can_fail)
		*success = true;

	return x;
}

// Takes a row of payload and returns a vector of TightDB types. Prioritizes Bool > Int > Float > Double > String. If 
// m_null_to_0 == true, then empty strings ('') are converted to Integer 0.
vector<tightdb::DataType> Importer::types (vector<string> v)
{
	vector<tightdb::DataType> res;

	for(size_t t = 0; t < v.size(); t++) {
		bool i;
		bool d;
		bool f;
		bool b;

		parse_integer<true>(v[t].c_str(), &i);
		parse_double<true>(v[t].c_str(), &d);
		parse_float<true>(v[t].c_str(), &f);
		parse_bool<true>(v[t].c_str(), &b);

		if(v[t] == "" && m_null_to_0) {
			// If m_null_to_0 is set, then integer/bool fields containing empty strings may be converted to 0/false 
			i = true;
			d = true;
			f = true;
			b = true;
		}

		res.push_back(b ? tightdb::type_Bool : i ? tightdb::type_Int : f ? tightdb::type_Float : d ? tightdb::type_Double : tightdb::type_String);
	}

	return res;
}

// Takes two vectors of TightDB types, and for each field finds best type that can represent both.
vector<tightdb::DataType> Importer::lowest_common(vector<tightdb::DataType> types1, vector<tightdb::DataType> types2)
{
	vector<tightdb::DataType> res;

	for(size_t t = 0; t < types1.size(); t++) {
		// All choices except for the last must be ||. The last must be &&
		if(types1[t] == tightdb::type_String || types2[t] == tightdb::type_String) 
			res.push_back(tightdb::type_String);
		else if(types1[t] == tightdb::type_Double || types2[t] == tightdb::type_Double)
			res.push_back(tightdb::type_Double);
		else if(types1[t] == tightdb::type_Float || types2[t] == tightdb::type_Float)
			res.push_back(tightdb::type_Float);
		else if(types1[t] == tightdb::type_Int || types2[t] == tightdb::type_Int)
			res.push_back(tightdb::type_Int);
		else if(types1[t] == tightdb::type_Bool && types2[t] == tightdb::type_Bool)
			res.push_back(tightdb::type_Bool);
		else
			assert(false);
	}
	return res;
}

// Takes payload vectors, and for each field finds best type that can represent all rows.
vector<tightdb::DataType> Importer::detect_scheme(vector<vector<string> > payload, size_t begin, size_t end)
{
	vector<tightdb::DataType> res;
	res = types(payload[begin]);

	for(size_t t = begin + 1; t < end && t < payload.size(); t++) {
		vector<tightdb::DataType> t2 = types(payload[t]);	
		res = lowest_common(res, t2);
	}
	return res;
}

size_t Importer::import(const char* csv_file, vector<vector<string> > & payload, size_t records) 
{
	size_t original_size = payload.size();

nextrecord:

    if(payload.size() - original_size >= records)
        goto end;

    if(top - s < chunk_size / 2) {
        memmove(src, src + s, top - s);
        top -= s;
        size_t r = fread(src + top, 1, chunk_size / 2, f);
        top += r;
        s = 0;
        if(r != chunk_size / 2) {
            src[top] = 0;
        }
    }

    if(src[s] == 0)
        goto end;

	payload.push_back(vector<string>());

nextfield:

    if(src[s] == 0)
        goto end;

	payload.back().push_back("");

	while(src[s] == ' ')
		s++;

	if(src[s] == '"') {
		// Field in quotes - can only end with another quote
		s++;
payload:
		while(src[s] != '"') {
			// Payload character
			payload.back().back().push_back(src[s]);
			s++;
		}

		if(src[s + 1] == '"') {
			// Double-quote
			payload.back().back().push_back('"');
			s += 2;
			goto payload;
		}
		else {
			// Done with field
			s += 1;

			// Only whitespace is allowed to occur between end quote and non-comma/non-eof/non-newline
			while(src[s] == ' ')
				s++;
        }

	}
	else {

		// Field not in quotes - cannot contain quotes or commas. So read until quote or comma or eof. Even though it's
		// non-conforming, some CSV files can contain non-quoted line breaks, so we need to test if we can't test for
		// new record by just testing for 0a/0d.
		size_t fields = payload.back().size();
		while(src[s] != ',' && src[s] != 0 && ((src[s] != 0xd && src[s] != 0xa) || (fields < m_fields && m_fields != size_t(-1)) )) {
			payload.back().back().push_back(src[s]);
			s++;
		}
		
	}

	if(src[s] == ',') {
		s++;
		goto nextfield;
	}

	if(src[s] == 0xd || src[s] == 0xa) {
		s++;
		if(src[s] == 0xd || src[s] == 0xa)
			s++;
		goto nextrecord;
	}

	goto nextfield;

end:

	return payload.size() - original_size;
}


size_t Importer::import_csv(const char* file, tightdb::Table& table, size_t import_rows, bool null_to_0, size_t type_detection_rows)
{
	vector<vector<string> > payload;
	bool header_present = false;

	m_null_to_0 = null_to_0;

	top = 0;
	s = 0;
	d = 0;
	m_fields = static_cast<size_t>(-1);

    f = fopen(file, "rb");
	if(f == 0)
		return size_t(-1);

	// 3 scenarios for header: 1) If first line is text-only and second row contains at least 1 non-string field, then
	// header is guaranteed present. 2) If both lines are string-only, we can't tell, and import first line as payload
	// 3) If at least 1 field of the first row is non-string, no header is present.
	import(file, payload, 2);

	// The flight data files contain "" as last field in header. This is the reason why we temporary disable conversion
	// to 0 during hedaer detection 
	bool original_null_to_0 = null_to_0;
	m_null_to_0 = false;
	vector<tightdb::DataType> scheme1 = detect_scheme(payload, 0, 1);

	// First row is best one to detect number of fields since it's less likely to contain embedded line breaks
	m_fields = scheme1.size();

	vector<tightdb::DataType> scheme2 = detect_scheme(payload, 1, 2);
	bool only_strings1 = true;
	bool only_strings2 = true;
	for(size_t t = 0; t < scheme1.size(); t++) {
		if(scheme1[t] != tightdb::type_String)
			only_strings1 = false;
		if(scheme2[t] != tightdb::type_String)
			only_strings2 = false;
	}
	m_null_to_0 = original_null_to_0;

	if(only_strings1 && !only_strings2)
		header_present = true;

	vector<string> header;
	if(header_present) {
		// Use first row of csv for column names
		header = payload[0];
		payload.erase(payload.begin());
		for(size_t t = 0; t < header.size(); t++) {
			// In flight database, header is present but contains null ("") as last field. We replace such occurences by 
			// a string
			if(header[t] == "") {
				char buf[10];
				sprintf(buf, "noname_%d\0", t);
				header[t] = buf;
			}
		}

	}
	else {
		// Use "1", "2", "3", ... for column names
		for(int i = 0; i < scheme1.size(); i++) {
			char buf[10];
			sprintf(buf, "%d\0", i);
			header.push_back(buf);
		}
	}

	// Detect scheme using next N rows. 
	import(file, payload, type_detection_rows);
	vector<tightdb::DataType> scheme = detect_scheme(payload, 0, type_detection_rows);

	// Create sheme in TightDB table
	for(size_t t = 0; t < scheme.size(); t++)
		table.add_column(scheme[t], tightdb::StringData(header[t]));
   

	size_t imported_rows = 0;
	do {
	
		for(size_t row = 0; row < payload.size(); row++) {

			if(imported_rows == import_rows)
				return imported_rows;

			table.add_empty_row();
			for(size_t col = 0; col < scheme.size(); col++) {
				bool success;

				if(scheme[col] == tightdb::type_String)
					table.set_string(col, imported_rows, tightdb::StringData(payload[row][col]));
				else if(scheme[col] == tightdb::type_Int)
					table.set_int(col, imported_rows, parse_integer<true>(payload[row][col].c_str(), &success));
				else if(scheme[col] == tightdb::type_Double)
					table.set_double(col, imported_rows, parse_double<true>(payload[row][col].c_str(), &success));
				else if(scheme[col] == tightdb::type_Float)
					table.set_float(col, imported_rows, parse_float<true>(payload[row][col].c_str(), &success));
				else if(scheme[col] == tightdb::type_Bool)
					table.set_bool(col, imported_rows, parse_bool<true>(payload[row][col].c_str(), &success));
				else
					assert(false);

				if(!success) {
					fprintf(stderr, "Column %d was was auto detected to be of type %s using the first %d rows of CSV"
									"file, but in row %d the field contained '%s' which is of another type. Try "
									"import_csv() with larger type_detection_rows argument.\n",
									(int)col,
									scheme[col] == tightdb::type_Bool ? "Bool" : 
									scheme[col] == tightdb::type_String ? "String" :
									scheme[col] == tightdb::type_Int ? "Integer" :
									scheme[col] == tightdb::type_Float ? "Float" :
									scheme[col] == tightdb::type_Double ? "Double" : "?",
									(int)type_detection_rows,
									(int)col - header_present ? 1 : 0, 
									payload[row][col].c_str());

					// Remove all columns so that user can call csv_import() on it again
					table.clear();
					for(size_t t = 0; t < table.get_column_count(); t++)
						table.remove_column(0);
					
					return 0;																									  
				}
			}
			imported_rows++;
		}
		payload.clear();
		import(file, payload, record_chunks);
	}
	while(payload.size() > 0);

	return imported_rows;

}




