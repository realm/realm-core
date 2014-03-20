#include <cstdlib>
#include <algorithm>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/version.hpp>

#include "../util/timer.hpp"
#include "../util/unit_test.hpp"
#include "../util/test_only.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using namespace tightdb::test_util::unit_test;


namespace {

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

    void end(const Location& loc, double elapsed_seconds) TIGHTDB_OVERRIDE
    {
        result r;
        r.m_test_name = loc.test_name;
        r.m_elapsed_seconds = elapsed_seconds;
        m_results.push_back(r);
        SimpleReporter::end(loc, elapsed_seconds);
    }

    void summary(const Summary& summary) TIGHTDB_OVERRIDE
    {
        SimpleReporter::summary(summary);

        cout << "\nTop 5 time usage:\n";
        sort(m_results.begin(), m_results.end());
        size_t n = min<size_t>(5, m_results.size());
        size_t longest_name = 0;
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            size_t size = r.m_test_name.size();
            if (size > longest_name)
                longest_name = size;
        }
        for(size_t i = 0; i != n; ++i) {
            const result& r = m_results[i];
            cout << left << setw(longest_name+2) << (r.m_test_name + ":") << right <<
                Timer::format(r.m_elapsed_seconds) << "\n";
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

} // anonymous namespace



int main()
{
    cout << "TightDB version: " << Version::get_version();
    cout << "\n  with Debug " << 
        (Version::has_feature(feature_Debug) ? "Enabled" : "Disabled"); 
    cout << "\n  with Replication " << 
        (Version::has_feature(feature_Replication) ? "Enabled" : "Disabled"); 
    cout << endl << endl;

    UniquePtr<Reporter> reporter;
    UniquePtr<Filter> filter;

    ofstream jenkins_file;
    const char* jenkins_url = getenv("JENKINS_URL");
    if (jenkins_url) {
        jenkins_file.open("unit-test-report.xml");
        reporter.reset(create_xml_reporter(jenkins_file));
    }
    else {
        const char* str = getenv("UNITTEST_PROGRESS");
        bool report_progress = str && strlen(str) != 0;
        reporter.reset(new CustomReporter(report_progress));
    }

    const char* filter_str = getenv("UNITTEST_FILTER");
    const char* test_only = get_test_only();
    if (test_only)
        filter_str = test_only;
    if (filter_str && strlen(filter_str) != 0)
        filter.reset(create_wildcard_filter(filter_str));

    bool success = run(reporter.get(), filter.get());

    if (test_only)
        cout << "\n*** BE AWARE THAT MOST TESTS ARE EXCLUDED DUE TO USING 'ONLY' MACRO ***\n\n";

    return success ? EXIT_SUCCESS : EXIT_FAILURE;
}
