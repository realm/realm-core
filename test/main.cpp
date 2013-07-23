#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/column.hpp>
#include <tightdb/utilities.hpp>


#include <tightdb/sequential_getter.h>

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

struct plus2 { 
    int64_t operator()(int64_t v1, int64_t v2) const {return v1 + v2;} 
};




// *******************************************************************************************************************
//
// 
//
// *******************************************************************************************************************






int main(int argc, char* argv[])
{

    Table table;
    table.add_column(type_Int, "first1");
    table.add_column(type_Int, "second1");


    for (int i = 0; i < 10100; i++) {
        table.add_empty_row();
        table.set_int(0, i,  rand() % 10 );
        table.set_int(1, i, 0);
    }





        Column a;

        for(size_t t = 0; t < 10100; t++)
            a.add(rand() % 10);

        a.set(9998, 30);

        CColumn* col = new CColumn(&a);
        CDynConstant* cc1 = new CDynConstant(20);
        CExpression* cc2 = new CDynConstant(50);

//        CExpression* ck = new COperator<plus2>(cc1, col);
//        CExpression* ck = new COperator<plus2, CDynConstant, CColumn>(cc1, col);          //
        CExpression* ck = new COperator<plus2, CColumn, CDynConstant>(col, cc1);           // slow
//        CExpression* ck = new COperator<plus2>(col, cc1);           // slow
        CCompareBase *e = new CCompare<Equal>(col, cc2);







    tightdb::TableView t1;













        {
            volatile size_t m;
            unsigned int best = -1;
            for(int y = 0; y < 80; y++)
            {
                UnitTest::Timer timer;
                timer.Start();
                for(int i = 0; i < 10000; i++) {
                    t1 = table.where().expression(e).find_all();
                //    m = e->Compare(0, 10000);

                }
                
                int t = timer.GetTimeInMs() ;
                if (t < best)
                    best = t;

            }
            cerr << best << "  " << m << "\n";
        }






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


        
        return 0;
}
 
