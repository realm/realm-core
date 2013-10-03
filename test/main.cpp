#include <cstring>
#include <algorithm>
#include <vector>
#include <iostream>
#include <iomanip>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>

#include "util/timer.hpp"


//#define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace UnitTest;
using namespace tightdb;


namespace {

class CustomTestReporter: public TestReporter {
public:
    struct Result {
        string m_test_name;
        double m_elapsed_seconds;
        bool operator<(const Result& r) const
        {
            return m_elapsed_seconds > r.m_elapsed_seconds; // Descending order
        }
    };

    vector<Result> m_results;

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

    void ReportTestFinish(TestDetails const& test, float elapsed_seconds)
    {
        static_cast<void>(test);
        static_cast<void>(elapsed_seconds);
        Result r;
        r.m_test_name = test.testName;
        r.m_elapsed_seconds = elapsed_seconds;
        m_results.push_back(r);
//        cerr << test.filename << ":" << test.lineNumber << ": End\n";
    }

    void ReportSummary(int total_test_count, int failed_test_count, int failure_count,
                       float elapsed_seconds)
    {
        if (0 < failure_count)
            cerr << "FAILURE: " << failed_test_count << " "
                "out of " << total_test_count << " tests failed "
                "(" << failure_count << " failures).\n";
        else
            cerr << "Success: " << total_test_count << " tests passed.\n";

        streamsize orig_prec = cerr.precision();
        cerr.precision(2);
        cerr << "Test time: ";
        test_util::Timer::format(elapsed_seconds, cerr);
        cerr << "\n";
        cerr.precision(orig_prec);

        cerr << "\nTop 5 time usage:\n";
        sort(m_results.begin(), m_results.end());
        size_t n = min<size_t>(5, m_results.size());
        for(size_t i = 0; i < n; ++i) {
            const Result& r = m_results[i];
            string text = r.m_test_name + ":";
            cerr << left << setw(32) << text << right;
            test_util::Timer::format(r.m_elapsed_seconds, cerr);
            cerr << "\n";
        }
    }
};

} // anonymous namespace


int main(int argc, char* argv[])
{
#ifndef _WIN32
    string tightdbd_path;
    // When running the unit-tests in Xcode, it runs them
    // in its own temporary directory. So we have to make sure we
    // look for the daemon there
    const char* xcode_env = getenv("__XCODE_BUILT_PRODUCTS_DIR_PATHS");
    if (xcode_env) {
#ifdef TIGHTDB_DEBUG
        tightdbd_path = "tightdbd-dbg-noinst";
#else
        tightdbd_path = "tightdbd-noinst";
#endif
    }
    else {
#ifdef TIGHTDB_DEBUG
        tightdbd_path = "../src/tightdb/tightdbd-dbg-noinst";
#else
        tightdbd_path = "../src/tightdb/tightdbd-noinst";
#endif
    }
    setenv("TIGHTDBD_PATH", tightdbd_path.c_str(), 0);
#endif
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
