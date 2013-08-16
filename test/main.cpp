#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/query_engine.hpp>

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

    table.set_int(0, 10003, 20);
    table.set_float(1, 10003, 20.0);


// new-generation syntax:
        cerr << "555555555555\n";

        Columns<int64_t> first(0);
        Columns<float> second(1);

        Expression* eee = first > first;



        Expression* eee1 = first + second > 8.9 + first;
        Expression* eee2 = first + second > first + 8;
        Expression* eee3 = first + second > 10;
        Expression* eee4 = second > 8;       
        Expression* eee5 = second > 8.1;
        Expression* eee6 = 8.1 > second;
        Expression* eee7 = first + second > first + int64_t(8);
        Expression* eee8 = second + 3 > first + int64_t(8);
       
        size_t match2;
        
        match2 = table.where().expression(eee2).find_next();

        match2 = table.where().expression(eee3).find_next();

        match2 = table.where().expression(eee4).find_next();




// query expressions:

//        float_column(1) + (int_column(0) + 20) > 50.0
#if 1
    // Slow
    Subexpr* col = new Columns<int64_t>(0);
    Subexpr* colf = new Columns<float>(1);

    Subexpr* cc1 = new Value<int64_t>(20);
    Subexpr* cc2 = new Value<float>(50.0);  

    Subexpr* ck0 = new Operator<Plus<int64_t> >(col, cc1);  
    Subexpr* ck = new Operator<Plus<float> >(colf, ck0);  

    Expression *e = new Compare<Greater, float>(ck, cc2);
#else
    // Fast
    Columns<int64_t>* col = new Columns<int64_t>(0);
    Columns<float>* colf = new Columns<float>(1);

    Value<int64_t>* cc1 = new Value<int64_t>(20);
    Value<float>* cc2 = new Value<float>(50.0);  

    Operator<Plus<int64_t>, Columns<int64_t>, Value<int64_t>>* ck0 = new Operator<Plus<int64_t>, Columns<int64_t>, Value<int64_t>>(col, cc1);  
    Subexpr* ck = new Operator<Plus<float>, Columns<float>, Operator<Plus<int64_t>, Columns<int64_t>, Value<int64_t>>>(colf, ck0);  

    Expression *e = new Compare<Greater, float, Subexpr, Value<float>>(ck, cc2);
#endif

  
    tightdb::TableView t1;

    size_t match = 0;


        {
            int best = -1;
            for(int y = 0; y < 20; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 10; i++) {
                    match = table.where().expression(e).find_next();

                }
                
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << ", match = " <<  match << "\n";
        }



        /*


        {
        // Static implementation, to compare speed of our dynamic methods above
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