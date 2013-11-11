#include <cstring>
#include <algorithm>
#include <vector>
#include <iostream>
#include <iomanip>
#include <limits>       // std::numeric_limits
#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>

#include "util/timer.hpp"


#define USE_VLD
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
// Define OLD, FINN or CURRENT to benchmark different versions
    double score = 0;

    {
        // BYTE sized array
        test_util::Timer t;
        double best;

        tightdb::Array a;
        int64_t val = 0;

        for(int i = 0; i < 40; i++) {
            val += rand() % 5;
            a.add(val);
        }

        best = 9999; //std::numeric_limits<double>::max();    
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 3000000; j++) {
                for(int i = 0; i < val; i += val / 30) {
                    volatile size_t t = a.upper_bound_int(i); // average
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "byte array, average direction:  \t" << best << "\n";
        score += best;

        best = 9999; //std::numeric_limits<double>::max();    
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 3000000; j++) {
                for(int i = 0; i < val; i += val / 30) {
                    volatile size_t t = a.upper_bound_int(0); // always go left
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "byte array, always go left:     \t" << best << "\n";
        score += best;

        best = 9999; //std::numeric_limits<double>::max();         
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 3000000; j++) {
                for(int i = 0; i < val; i += val / 30) {
                    volatile size_t t = a.upper_bound_int(val); // always go right
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "byte array, always go right:    \t" << best << "\n";
        score += best;
    }


    {
        // 32 bit int array
        test_util::Timer t;
        double best;

        tightdb::Array a;
        int64_t val = 0;

        for(int i = 0; i < 1000; i++) {
            val += rand() % 1000;
            a.add(val);
        }

        best = 9999; //std::numeric_limits<double>::max();      
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 30000; j++) {
                for(int i = 0; i < val; i += val / 1000) {
                    volatile size_t t = a.upper_bound_int(i); // average direction
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "32-bit array, average direction:\t" << best << "\n";

        best = 9999; //std::numeric_limits<double>::max();         
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 100000; j++) {
                for(int i = 0; i < val; i += val / 1000) {
                    volatile size_t t = a.upper_bound_int(0); // always go left
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "32-bit array, always go left:   \t" << best << "\n";
        score += best;

        best = 9999; //std::numeric_limits<double>::max();        
        for(int iter = 0; iter < 3; iter++) {
            t.reset();
            for(int j = 0; j < 100000; j++) {
                for(int i = 0; i < val; i += val / 1000) {
                    volatile size_t t = a.upper_bound_int(val);  // always go right
                }
            }
            if(t < best)
                best = t;
        }
        cerr << "32-bit array, always go right:  \t" << best << "\n";
        score += best;
    }




    cerr << "sum: " << score;

    return 0;
}
