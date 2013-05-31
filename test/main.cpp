#define NOMINMAX

#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>


#define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace UnitTest;

namespace {

struct CustomTestReporter: TestReporter {
    void ReportTestStart(TestDetails const& test)
    {
        static_cast<void>(test);
//        cerr << test.filename << ":" << test.lineNumber << ": Begin " << test.testName << "\n";
    }

    void ReportFailure(TestDetails const& test, char const* failure)
    {
        cerr << test.filename << ":" << test.lineNumber << ": error: "
            "Failure in " << test.testName << ": " << failure << "\n";
    }

    void ReportTestFinish(TestDetails const& test, float seconds_elapsed)
    {
        static_cast<void>(test);
        static_cast<void>(seconds_elapsed);
//        cerr << test.filename << ":" << test.lineNumber << ": End\n";
    }

    void ReportSummary(int total_test_count, int failed_test_count, int failure_count, float seconds_elapsed)
    {
        if (0 < failure_count)
            cerr << "FAILURE: " << failed_test_count << " "
                "out of " << total_test_count << " tests failed "
                "(" << failure_count << " failures).\n";
        else
            cerr << "Success: " << total_test_count << " tests passed.\n";

        const streamsize orig_prec = cerr.precision();
        cerr.precision(2);
        cerr << "Test time: " << seconds_elapsed << " seconds.\n";
        cerr.precision(orig_prec);
    }
};

} // anonymous namespace



template <bool can_fail> int64_t parse_integer(const char* col, bool* success = NULL){
	int64_t	x = 0;

	if(*col == '-'){
		++col;
		x = 0;
		while(*col != '\0'){
			if(can_fail && ('0' > *col || *col > '9')){
				*success = false;
				return 0;
			}

			int64_t y = *col - '0';
			if(can_fail && x < (std::numeric_limits<int64_t>::min()+y)/10){
				*success = false;
				return 0;
			}

			x = 10*x-y;
			++col;
		}
		return x;
	}else if(*col == '+')
		++col;

	while(*col != '\0'){
		if(can_fail && ('0' > *col || *col > '9')){
			*success = false;
			return 0;
		}
		int64_t y = *col - '0';
		x = 10*x+y;
		++col;
	}

	return x;
}

template <bool can_fail> double parse_double(const char*col, bool* success = NULL){
			double x;

			bool is_neg = false;
			if(*col == '-'){
				is_neg = true;
				++col;
			}else if(*col == '+')
				++col;

			x = 0;
			while('0' <= *col && *col <= '9'){
				int y = *col - '0';
				x *= 10;
				x += y;
				++col;
			}
			
			if(*col == '.'|| *col == ','){
				++col;
				double pos = 1;
				while('0' <= *col && *col <= '9'){
					pos /= 10;
					int y = *col - '0';
					++col;
					x += y*pos;
				}
			}

			if(*col == 'e' || *col == 'E'){
				++col;
				int64_t e;
				e = parse_integer<false>(col);
				
				if(e != 0){
					double base;	
					if(e < 0){
						base = 0.1;
						e = -e;
					}else{
						base = 10;
					}
	
					while(e != 1){
						if((e & 1) == 0){
							base = base*base;
							e >>= 1;
						}else{
							x *= base;
							--e;
						}
					}
					x *= base;
				}
			}else{
				if(can_fail && *col != '\0') {
					*success = false;
					return 0;
				}

			}

			if(is_neg)
				x = -x;

			return x;
		}




const size_t chunk_size = 16*1024;
char src[2*chunk_size];

size_t top = 0;

vector<vector<string>> v;

int main(int argc, char* argv[])
{
	bool success;
	double d2 = parse_double<true>("3.5", &success);
	int i2 = parse_integer<true>("3432", &success);

	size_t s = 0;
	size_t d = 0;

    size_t field;

    FILE* f = fopen("d:/csv/perf.csv", "rb"); // csv.txt", "rb");

nextrecord:

    if(top - s < chunk_size / 2) {
        memmove(src, src + s, top - s);
        top -= s;
        size_t r = fread(src + top, 1, chunk_size / 2, f);
        top += r;
        s = 0;
        if(r != chunk_size / 2) {
            src[top] = 0;
        }
        printf("");
    }

    if(src[s] == 0)
        goto end;

	v.push_back(vector<string>());

nextfield:

    if(src[s] == 0)
        goto end;

	v.back().push_back("");

	while(src[s] == ' ')
		s++;

	if(src[s] == '"') {
		// Field in quotes - can only end with another quote
		s++;




payload:
		while(src[s] != '"') {
			// Payload character
			v.back().back().push_back(src[s]);
			s++;
		}

		if(src[s + 1] == '"') {
			// Double-quote
			v.back().back().push_back('"');
			s += 2;
			goto payload;
		}
		else {
			// Done with field
			s += 1;

			// Only whitespace is allowed to occur between end quote and non-comma/non-eof/non-newline
			while(src[s] == ' ')
				s++;

			s++;
		}

	}
	else {
		// Field not in quotes - cannot contain quotes, commas or line breaks. So read until comma, eof or linebreak
		while(src[s] != ',' && src[s] != 0xd && src[s] != 0xa && src[s] != 0) {
			v.back().back().push_back(src[s]);
			s++;
		}
			
		if(src[s] == ',') {
			s++;
			goto nextfield;
		}
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

	exit(-1);

	

    bool const no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exitcode") == 0;

#ifdef TIGHTDB_DEBUG
    cerr << "Running Debug unit tests\n";
#else
    cerr << "Running Release unit tests\n";
#endif

    cerr << "TIGHTDB_MAX_LIST_SIZE = " << TIGHTDB_MAX_LIST_SIZE << "\n";

#ifdef TIGHTDB_COMPILER_SSE
    cerr << "Compiler supported SSE (auto detect): Yes\n";
#else
    cerr << "Compiler supported SSE (auto detect): No\n";
#endif

    cerr << "This CPU supports SSE (auto detect):  " << (tightdb::cpuid_sse<42>() ? "4.2" : (tightdb::cpuid_sse<30>() ? "3.0" : "None"));
    cerr << "\n\n";

    CustomTestReporter reporter;
    TestRunner runner(reporter);
    const int res = runner.RunTestsIf(Test::GetTestList(), 0, True(), 0);

#ifdef _MSC_VER
    getchar(); // wait for key
#endif
    return no_error_exit_staus ? 0 : res;
}
