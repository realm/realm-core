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


/*
lower = better

current:
---------------------------------------------------
VC2010:
byte array, average direction:          1.766
byte array, always go left:             1.734
byte array, always go right:            1.766
32-bit array, average direction:        1.187
32-bit array, always go left:           1.812
32-bit array, always go right:          2.031
sum: 9.109

gcc47:
byte array, average direction:  	1.46292
byte array, always go left:     	1.2532
byte array, always go right:    	2.07292
32-bit array, average direction:	0.992568
32-bit array, always go left:   	1.1024
32-bit array, always go right:  	1.85952
sum: 7.75095


old:
---------------------------------------------------
VC2010:
byte array, average direction:          1.843
byte array, always go left:             1.75
byte array, always go right:            2.015
32-bit array, average direction:        1.094
32-bit array, always go left:           1.781
32-bit array, always go right:          1.546
sum: 8.935

gcc47:
byte array, average direction:  	1.95876
byte array, always go left:     	1.47498
byte array, always go right:    	1.87065
32-bit array, average direction:	0.901498
32-bit array, always go left:   	2.62416
32-bit array, always go right:  	2.96418
sum: 10.8927

finns:
---------------------------------------------------
VC2010:
byte array, average direction:          1.594
byte array, always go left:             1.875
byte array, always go right:            1.343
32-bit array, average direction:        1.454
32-bit array, always go left:           2.203
32-bit array, always go right:          2.109
sum: 9.124

gcc47:
byte array, average direction:  	1.63972
byte array, always go left:     	1.91009
byte array, always go right:    	1.50329
32-bit array, average direction:	0.941885
32-bit array, always go left:   	3.01376
32-bit array, always go right:  	2.58277
sum: 10.6496
*/
    
// Define OLD, FINN or CURRENT inside the upper_count_int method in Array to benchmark different versions
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
