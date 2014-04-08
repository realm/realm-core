#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <vector>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <tightdb/util/features.h>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/version.hpp>

#include "util/timer.hpp"

#include "test.hpp"


// #define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif


using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using namespace tightdb::test_util::unit_test;


namespace {


void fix_async_daemon_path()
{
    // `setenv()` is POSIX. _WIN32 has `_putenv_s()` instead.
#ifndef _WIN32
    const char* async_daemon;
    // When running the unit-tests in Xcode, it runs them
    // in its own temporary directory. So we have to make sure we
    // look for the daemon there
    const char* xcode_env = getenv("__XCODE_BUILT_PRODUCTS_DIR_PATHS");
    if (xcode_env) {
#ifdef TIGHTDB_DEBUG
        async_daemon = "tightdbd-dbg-noinst";
#else
        async_daemon = "tightdbd-noinst";
#endif
    }
    else {
#ifdef TIGHTDB_COVER
        async_daemon = "../src/tightdb/tightdbd-cov-noinst";
#else
#  ifdef TIGHTDB_DEBUG
        async_daemon = "../src/tightdb/tightdbd-dbg-noinst";
#  else
        async_daemon = "../src/tightdb/tightdbd-noinst";
#  endif
#endif
    }
    setenv("TIGHTDB_ASYNC_DAEMON", async_daemon, 0);
#endif // _WIN32
}


void display_build_config()
{
    const char* with_debug =
        Version::has_feature(feature_Debug) ? "Enabled" : "Disabled";
    const char* with_replication =
        Version::has_feature(feature_Replication) ? "Enabled" : "Disabled";

#ifdef TIGHTDB_COMPILER_SSE
    const char* compiler_sse = "Yes";
#else
    const char* compiler_sse = "No";
#endif

#ifdef TIGHTDB_COMPILER_AVX
    const char* compiler_avx = "Yes";
#else
    const char* compiler_avx = "No";
#endif

    const char* cpu_sse = tightdb::sseavx<42>() ? "4.2" :
        (tightdb::sseavx<30>() ? "3.0" : "None");

    const char* cpu_avx = tightdb::sseavx<1>() ? "Yes" : "No";

    cout <<
        "\n"
        "TightDB version: "<<Version::get_version()<<"\n"
        "  with Debug "<<with_debug<<"\n"
        "  with Replication "<<with_replication<<"\n"
        "\n"
        "TIGHTDB_MAX_LIST_SIZE = " << TIGHTDB_MAX_LIST_SIZE << "\n"
        "\n"
        // Be aware that ps3/xbox have sizeof(void*) = 4 && sizeof(size_t) == 8
        // We decide to print size_t here
        "sizeof(size_t) * 8 = " << sizeof(size_t)* 8 << "\n"
        "\n"
        "Compiler supported SSE (auto detect):       " << compiler_sse << "\n"
        "This CPU supports SSE (auto detect):        "<<cpu_sse<<"\n"
        "Compiler supported AVX (auto detect):       "<<compiler_avx<<"\n"
        "This CPU supports AVX (AVX1) (auto detect): "<<cpu_avx<<"\n"
        "\n";
}


// Records elapsed time for each test and shows a "Top 5" at the end.
class CustomReporter: public SimpleReporter {
public:
    explicit CustomReporter(bool report_progress):
        SimpleReporter(report_progress)
    {
    }

    ~CustomReporter() TIGHTDB_NOEXCEPT
    {
    }

    void end(const TestDetails& details, double elapsed_seconds) TIGHTDB_OVERRIDE
    {
        result r;
        r.m_test_name = details.test_name;
        r.m_elapsed_seconds = elapsed_seconds;
        m_results.push_back(r);
        SimpleReporter::end(details, elapsed_seconds);
    }

    void summary(const Summary& summary) TIGHTDB_OVERRIDE
    {
        SimpleReporter::summary(summary);

        size_t max_n = 5;
        size_t n = min<size_t>(max_n, m_results.size());
        if (n < 2)
            return;

        partial_sort(m_results.begin(), m_results.begin() + n, m_results.end());
        size_t name_col_width = 0, time_col_width = 0;
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            size_t size = r.m_test_name.size();
            if (size > name_col_width)
                name_col_width = size;
            size = Timer::format(r.m_elapsed_seconds).size();
            if (size > time_col_width)
                time_col_width = size;
        }
        name_col_width += 2;
        size_t full_width = name_col_width + time_col_width;
        cout.fill('-');
        cout << "\nTop "<<n<<" time usage:\n"<<setw(int(full_width)) << "" << "\n";
        cout.fill(' ');
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            cout <<
                left  << setw(int(name_col_width)) << r.m_test_name <<
                right << setw(int(time_col_width)) << Timer::format(r.m_elapsed_seconds) << "\n";
        }
    }

private:
    struct result {
        string m_test_name;
        double m_elapsed_seconds;
        bool operator<(const result& r) const
        {
            return m_elapsed_seconds > r.m_elapsed_seconds; // Descending order
        }
    };

    vector<result> m_results;
};


bool run_tests()
{
    {
        const char* str = getenv("UNITTEST_KEEP_FILES");
        if (str && strlen(str) != 0)
            keep_test_files();
    }

    UniquePtr<Reporter> reporter;
    UniquePtr<Filter> filter;

    // Set up reporter
    ofstream xml_file;
    const char* xml_str = getenv("UNITTEST_XML");
    bool xml = (xml_str && strlen(xml_str) != 0) || getenv("JENKINS_URL");
    if (xml) {
        xml_file.open("unit-test-report.xml");
        reporter.reset(create_xml_reporter(xml_file));
    }
    else {
        const char* str = getenv("UNITTEST_PROGRESS");
        bool report_progress = str && strlen(str) != 0;
        reporter.reset(new CustomReporter(report_progress));
    }

    // Set up filter
    const char* filter_str = getenv("UNITTEST_FILTER");
    const char* test_only = get_test_only();
    if (test_only)
        filter_str = test_only;
    if (filter_str && strlen(filter_str) != 0)
        filter.reset(create_wildcard_filter(filter_str));

    // Run
    TestList& list = get_default_test_list();
    bool success = list.run(reporter.get(), filter.get());

    if (test_only)
        cout << "\n*** BE AWARE THAT MOST TESTS ARE EXCLUDED DUE TO USING 'ONLY' MACRO ***\n";

    if (!xml)
        cout << "\n";

    return success;
}


} // anonymous namespace



int main(int argc, char* argv[])
{
    bool no_error_exit_staus = 2 <= argc && strcmp(argv[1], "--no-error-exitcode") == 0;

    fix_async_daemon_path();
    display_build_config();

    bool success = run_tests();

#ifdef _MSC_VER
    getchar(); // wait for key
#endif

    return success || no_error_exit_staus ? EXIT_SUCCESS : EXIT_FAILURE;
}
