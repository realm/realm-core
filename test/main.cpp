#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>
#include <vector>


//#define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace UnitTest;
using namespace tightdb;

struct 
{
    string name;
    float time;
} typedef result_t;

vector<result_t> results;

namespace {

bool compare (const result_t &lhs, const result_t &rhs){
    return lhs.time > rhs.time;
}

struct CustomTestReporter: TestReporter {
    


    void ReportTestStart(TestDetails const& test)
    {
        static_cast<void>(test);
     //   cerr << test.filename << ":" << test.lineNumber << ": Begin " << test.testName << "\n";
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
        result_t r;
        r.name = test.testName;
        r.time = seconds_elapsed;
        results.push_back(r);

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

        cerr << "\nTop 5 time usage:\n";
        std::sort(results.begin(), results.end(), compare);
        for(size_t t = 0; t < 5; t++) {
            size_t space = 30 - (results[t].name.size() > 30 ? 30 : results[t].name.size());
            cerr << results[t].name << ": " << string(space, ' ').c_str() << results[t].time << " s\n";
        }
    }
};

} // anonymous namespace


int main(int argc, char* argv[])
{


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
