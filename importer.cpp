#include "importer.hpp"
#include <stdint.h>
#include <limits>
#include <vector>
#include <assert.h>

using namespace std;

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

template <bool can_fail> double Importer::parse_double(const char*col, bool* success, size_t* significants)
{
	double x;
	bool is_neg = false;
	size_t dummy;
	if(can_fail && significants == NULL)
		significants = &dummy;

	if(can_fail && *col == 0) {
		*success = false;
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
	else {
		if (can_fail && *col != '\0') {
			*success = false;
			return 0;
		}

	}

	if(is_neg)
		x = -x;

	if(can_fail)
		*success = true;

	return x;
}

// Takes a row of payload and returns a vector of TightDB types. Prioritizes Int > Float > Double > String
vector<tightdb::DataType> Importer::types (vector<string> v)
{
	vector<tightdb::DataType> res;

	for(size_t t = 0; t < v.size(); t++) {
		bool i;
		bool d;
		bool f;

		parse_integer<true>(v[t].c_str(), &i);
		parse_float<true>(v[t].c_str(), &f);
		parse_double<true>(v[t].c_str(), &d);

		if(v[t] == "" && m_null_to_0) {
			i = true;
			d = true;
			f = true;
		}

		res.push_back(i ? tightdb::type_Int : f ? tightdb::type_Float : d ? tightdb::type_Double : tightdb::type_String);
	}

	return res;
}

// Takes two vectors of TightDB types, and for each field finds best type that can represent both.
vector<tightdb::DataType> Importer::lowest_common(vector<tightdb::DataType> types1, vector<tightdb::DataType> types2)
{
	vector<tightdb::DataType> res;

	for(size_t t = 0; t < types1.size(); t++) {
		if(types1[t] == tightdb::type_String || types2[t] == tightdb::type_String) 
			res.push_back(tightdb::type_String);
		else if(types1[t] == tightdb::type_Double || types2[t] == tightdb::type_Double)
			res.push_back(tightdb::type_Double);
		else if(types1[t] == tightdb::type_Float || types2[t] == tightdb::type_Float)
			res.push_back(tightdb::type_Float);
		else if(types1[t] == tightdb::type_Int && types2[t] == tightdb::type_Int)
			res.push_back(tightdb::type_Int);
		else
			assert(false);
	}
	return res;
}


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

		// Field not in quotes - cannot contain quotes, commas or line breaks. So read until comma, eof or linebreak
		while(src[s] != ',' && src[s] != 0xd && src[s] != 0xa && src[s] != 0) {
			payload.back().back().push_back(src[s]);
			s++;
		}
		
	}

	if(src[s] == ',') {
		s++;
		goto nextfield;
	}

	if(src[s] == 0xd)
		s++;

	if(src[s] == 0xa) {
		s++;
		goto nextrecord;	
	}
	else {
		goto nextfield;
	}

end:

	return payload.size() - original_size;
}


void Importer::import_csv(const char* file, tightdb::Table& table, bool null_to_0, size_t type_detection_rows)
{
	vector<vector<string> > payload;
	bool header_present = false;

	m_null_to_0 = null_to_0;

	top = 0;
	s = 0;
	d = 0;

    f = fopen(file, "rb");

	// 3 scenarios for header: 1) If first line is text-only and second row contains at least 1 non-string field, then
	// header is probably present. 2) If both lines are string-only, we can't tell, and import first line as payload 3)
	// If at least 1 field of the first row is non-string, no header is present
	import(file, payload, 2);
	vector<tightdb::DataType> scheme1 = detect_scheme(payload, 0, 1);
	vector<tightdb::DataType> scheme2 = detect_scheme(payload, 1, 2);

	bool only_strings1 = true;
	bool only_strings2 = true;
	for(size_t t = 0; t < scheme1.size(); t++) {
		if(scheme1[t] != tightdb::type_String)
			only_strings1 = false;
		if(scheme2[t] != tightdb::type_String)
			only_strings2 = false;
	}

	if(only_strings1 && !only_strings2)
		header_present = true;

	vector<string> header;
	if(header_present) {
		// Use first row of csv for column names
		header = payload[0];
		payload.erase(payload.begin());
	}
	else {
		// Use "1", "2", "3", ... for column names
		for(int i = 0; i < scheme1.size(); i++) {
			char buf[10];
			sprintf(buf, "%d\0", i);
			header.push_back(buf);
		}
	}

	// Detect scheme using first N rows. 
	import(file, payload, type_detection_rows);
	vector<tightdb::DataType> scheme = detect_scheme(payload, 0, type_detection_rows);

	for(size_t t = 0; t < scheme.size(); t++) {
		table.add_column(scheme[t], tightdb::StringData(header[t]));
    }


	do {
		payload.clear();
		import(file, payload, record_chunks);
	
		for(size_t row = 0; row < payload.size(); row++) {
			table.add_empty_row();
			for(size_t col = 0; col < scheme.size(); col++) {
				if(scheme[col] == tightdb::type_String)
					table.set_string(col, row, tightdb::StringData(payload[row][col]));
				else if(scheme[col] == tightdb::type_Int)
					table.set_int(col, row, parse_integer<false>(payload[row][col].c_str()));
				else if(scheme[col] == tightdb::type_Double)
					table.set_double(col, row, parse_double<false>(payload[row][col].c_str()));
				else if(scheme[col] == tightdb::type_Float)
					table.set_float(col, row, parse_float<false>(payload[row][col].c_str()));
				else
					assert(false);
			}
		}
	}
	while(payload.size() > 0);

}




