#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/query_engine.hpp>

//#include <tightdb/query_expression.h>

#define USE_VLD
#if defined(_MSC_VER) && defined(_DEBUG) && defined(USE_VLD)
    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace UnitTest;
using namespace tightdb;

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


// *******************************************************************************************************************
//
// 
//
// *******************************************************************************************************************






int main(int argc, char* argv[])
{

    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Float, "second1");


    for (int i = 0; i < 10100; i++) {
        table.add_empty_row();
        table.set_int(0, i,  rand() % 10 );
        table.set_float(1, i, float(rand() % 10));
    }

    table.set_int(0, 10000, 20);
    table.set_float(1, 10000, 20.0);

        // 633
#if 1
        // Slow dynamic, 250 ms
        //== col 

//        fcol(1) + (icol(0) + 20) > 50.0

        Subexpr* col = new Columns<int64_t>(0);
        Subexpr* colf = new Columns<float>(1);

        Subexpr* cc1 = new Value<int64_t>(20);
        Subexpr* cc2 = new Value<float>(50.0);  

        Subexpr* ck0 = new Operator<int64_t, Plus<int64_t> >(col, cc1);  
        Subexpr* ck = new Operator<float, Plus<float> >(colf, ck0);  



#else
        // Fast static, 190
//        Columns* col = new Columns(&a);
        Columns* col = new Columns(0);
        Constant* cc1 = new Constant(20);
        Constant* cc2 = new Constant(50);  
        Subexpr* ck = new Operator<Plus, Columns, Constant>(col, cc1);  

#endif

        Expression *e = new Compare<Greater, float>(ck, cc2);


    tightdb::TableView t1;

    size_t match = 0;


        {
            volatile size_t m;
            unsigned int best = -1;
            for(int y = 0; y < 20; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 10000; i++) {
                    match = table.where().expression(e).find_next();
                //    m = e->compare(0, 10000);

                }
                
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << ", match = " <<  match << "\n";
        }



        /*


        {
            volatile size_t m;
            unsigned int best = -1;
            for(int y = 0; y < 1; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 100000; i++) {
                    for(size_t t = 0; t < 1010; t += 8) {
                        if(a.get(t) + 20 == 50 || a.get(t + 1) + 20 == 50 || a.get(t + 2) + 20 == 50 || a.get(t + 3) + 20 == 50 ||
                           a.get(t + 4) + 20 == 50 || a.get(t + 5) + 20 == 50 || a.get(t + 6) + 20 == 50 || a.get(t + 7) + 20 == 50) {
                            m = t;
                            break;
                        }
                    }
                }
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << "  " << m << "\n";
        }
        */

         CustomTestReporter reporter;
    TestRunner runner(reporter);
    const int res = runner.RunTestsIf(Test::GetTestList(), 0, True(), 0);


        return 0;
}
 

// 638

// 545   568